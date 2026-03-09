from __future__ import annotations

import argparse
import json
import platform
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from src.batch.main import (
    PICKUP_TIMESTAMP_CANDIDATES,
    compute_borough_summary,
    compute_hourly_pickup_counts,
    compute_payment_tip_summary,
    compute_summary_metrics,
    compute_zone_summary,
    create_spark_session,
    enrich_with_zone_lookup,
    filter_trip_dataframe_for_analysis,
    get_payment_type_label,
    load_trip_dataframe,
    load_zone_lookup_dataframe,
    prepare_trip_dataframe,
    resolve_first_available_column,
)
from src.common.settings import get_settings

SETTINGS = get_settings()
DEFAULT_INPUT_PATH = str(SETTINGS.raw_taxi_data_dir / "yellow" / "2023")
DEFAULT_ZONE_LOOKUP_PATH = str(SETTINGS.taxi_zone_lookup_path)
DEFAULT_OUTPUT_PATH = Path("data/benchmarks/pandas_vs_spark_jan2023")
DEFAULT_ROW_LIMITS = ("100000", "500000", "full")
DEFAULT_ENGINES = ("pandas", "pyspark")
NUMERIC_COLUMNS = ("passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark pandas vs PySpark on NYC Taxi aggregations.")
    parser.add_argument("--input-path", default=DEFAULT_INPUT_PATH, help="Parquet input file or directory")
    parser.add_argument("--zone-lookup-path", default=DEFAULT_ZONE_LOOKUP_PATH, help="Taxi zone lookup CSV path")
    parser.add_argument("--pickup-year", type=int, default=SETTINGS.taxi_pickup_year, help="Optional pickup year filter")
    parser.add_argument(
        "--row-limits",
        nargs="*",
        default=list(DEFAULT_ROW_LIMITS),
        help="Benchmark row limits. Supports integers and 'full'. Example: 100000 500000 full",
    )
    parser.add_argument(
        "--engines",
        nargs="*",
        choices=DEFAULT_ENGINES,
        default=list(DEFAULT_ENGINES),
        help="Engines to benchmark",
    )
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH), help="Output path prefix without extension")
    parser.add_argument("--preview-rows", type=int, default=5, help="Preview rows to keep in the report")
    return parser


def parse_row_limits(values: Sequence[str]) -> list[int | None]:
    parsed: list[int | None] = []
    for value in values:
        token = value.strip().lower()
        if token == "full":
            parsed.append(None)
            continue

        row_limit = int(token)
        if row_limit <= 0:
            raise ValueError(f"Row limit must be positive, got: {value}")
        parsed.append(row_limit)

    return parsed


def format_row_limit(row_limit: int | None) -> str:
    return "full" if row_limit is None else f"{row_limit:,}"


def _safe_number(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


def _records_preview(dataframe: pd.DataFrame, rows: int) -> list[dict[str, Any]]:
    if dataframe.empty:
        return []
    preview = dataframe.head(rows).copy()
    return [{key: _safe_number(value) for key, value in record.items()} for record in preview.to_dict(orient="records")]


def _serializable_dict(data: dict[str, Any]) -> dict[str, Any]:
    return {key: _safe_number(value) for key, value in data.items()}


def load_pandas_trip_dataframe(input_path: str, row_limit: int | None = None) -> pd.DataFrame:
    dataset = ds.dataset(input_path, format="parquet")
    frames: list[pd.DataFrame] = []
    loaded_rows = 0

    for batch in dataset.to_batches(batch_size=65536):
        frame = batch.to_pandas()
        if row_limit is not None:
            remaining = row_limit - loaded_rows
            if remaining <= 0:
                break
            frame = frame.iloc[:remaining]

        frames.append(frame)
        loaded_rows += len(frame)
        if row_limit is not None and loaded_rows >= row_limit:
            break

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_pandas_zone_lookup(zone_lookup_path: str) -> pd.DataFrame | None:
    path = Path(zone_lookup_path)
    if not path.exists():
        return None
    dataframe = pd.read_csv(path, keep_default_na=False)
    return dataframe[["LocationID", "Borough", "Zone", "service_zone"]].drop_duplicates(subset=["LocationID"])


def prepare_pandas_trip_dataframe(dataframe: pd.DataFrame, pickup_year: int | None) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    pickup_column = resolve_first_available_column(list(dataframe.columns), PICKUP_TIMESTAMP_CANDIDATES)
    if pickup_column is None:
        available = ", ".join(map(str, dataframe.columns))
        raise ValueError(
            "No supported pickup timestamp column found. "
            f"Expected one of {PICKUP_TIMESTAMP_CANDIDATES}, got: {available}"
        )
    prepared = dataframe.copy()
    prepared["pickup_ts"] = pd.to_datetime(prepared[pickup_column], errors="coerce")
    prepared = prepared.dropna(subset=["pickup_ts"])

    if pickup_year is not None:
        prepared = prepared[prepared["pickup_ts"].dt.year == pickup_year]

    prepared["pickup_date"] = prepared["pickup_ts"].dt.date
    prepared["pickup_hour"] = prepared["pickup_ts"].dt.hour

    for column_name in NUMERIC_COLUMNS:
        if column_name in prepared.columns:
            prepared[column_name] = pd.to_numeric(prepared[column_name], errors="coerce")

    if "payment_type" in prepared.columns:
        prepared["payment_type_label"] = prepared["payment_type"].apply(get_payment_type_label)

    if {"fare_amount", "tip_amount"}.issubset(prepared.columns):
        prepared["tip_pct"] = np.where(
            prepared["fare_amount"] > 0,
            (prepared["tip_amount"] / prepared["fare_amount"]) * 100.0,
            np.nan,
        )

    return prepared


def enrich_pandas_with_zone_lookup(dataframe: pd.DataFrame, zone_lookup_df: pd.DataFrame | None) -> pd.DataFrame:
    if zone_lookup_df is None or dataframe.empty:
        return dataframe.copy()

    enriched = dataframe.copy()

    if "PULocationID" in enriched.columns:
        pickup_lookup = zone_lookup_df.rename(
            columns={
                "LocationID": "PULocationID",
                "Borough": "pickup_borough",
                "Zone": "pickup_zone",
                "service_zone": "pickup_service_zone",
            }
        )
        enriched = enriched.merge(pickup_lookup, on="PULocationID", how="left")

    if "DOLocationID" in enriched.columns:
        dropoff_lookup = zone_lookup_df.rename(
            columns={
                "LocationID": "DOLocationID",
                "Borough": "dropoff_borough",
                "Zone": "dropoff_zone",
                "service_zone": "dropoff_service_zone",
            }
        )
        enriched = enriched.merge(dropoff_lookup, on="DOLocationID", how="left")

    fill_columns = [
        column_name
        for column_name in (
            "pickup_borough",
            "pickup_zone",
            "pickup_service_zone",
            "dropoff_borough",
            "dropoff_zone",
            "dropoff_service_zone",
        )
        if column_name in enriched.columns
    ]
    if fill_columns:
        enriched[fill_columns] = enriched[fill_columns].fillna("Unknown")

    return enriched


def compute_pandas_outputs(dataframe: pd.DataFrame, preview_rows: int) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "row_count": int(len(dataframe)),
        "active_days": int(dataframe["pickup_date"].nunique()) if "pickup_date" in dataframe.columns else None,
    }

    if "passenger_count" in dataframe.columns:
        summary["avg_passenger_count"] = dataframe["passenger_count"].mean()
    if "trip_distance" in dataframe.columns:
        summary["avg_trip_distance"] = dataframe["trip_distance"].mean()
    if "pickup_borough" in dataframe.columns:
        summary["active_pickup_boroughs"] = int(dataframe["pickup_borough"].nunique())
    if "total_amount" in dataframe.columns:
        summary["avg_total_amount"] = dataframe["total_amount"].mean()
        summary["sum_total_amount"] = dataframe["total_amount"].sum()
    if "tip_amount" in dataframe.columns:
        summary["sum_tip_amount"] = dataframe["tip_amount"].sum()
    if "tip_pct" in dataframe.columns:
        summary["avg_tip_pct"] = dataframe["tip_pct"].mean()

    outputs: dict[str, Any] = {"summary": _serializable_dict(summary)}

    if {"pickup_date", "pickup_hour"}.issubset(dataframe.columns):
        hourly_df = (
            dataframe.groupby(["pickup_date", "pickup_hour"], dropna=False)
            .agg(
                trip_count=("pickup_hour", "size"),
                sum_total_amount=("total_amount", "sum") if "total_amount" in dataframe.columns else ("pickup_hour", "size"),
            )
            .reset_index()
            .sort_values(["pickup_date", "pickup_hour"])
        )
        outputs["hourly_preview"] = _records_preview(hourly_df, preview_rows)

    if "pickup_borough" in dataframe.columns:
        borough_agg: dict[str, tuple[str, str]] = {"trip_count": ("pickup_borough", "size")}
        if "total_amount" in dataframe.columns:
            borough_agg["sum_total_amount"] = ("total_amount", "sum")
            borough_agg["avg_total_amount"] = ("total_amount", "mean")
        if "trip_distance" in dataframe.columns:
            borough_agg["avg_trip_distance"] = ("trip_distance", "mean")
        if "tip_pct" in dataframe.columns:
            borough_agg["avg_tip_pct"] = ("tip_pct", "mean")
        borough_df = (
            dataframe.groupby("pickup_borough", dropna=False)
            .agg(**borough_agg)
            .reset_index()
            .sort_values(["sum_total_amount", "trip_count"] if "sum_total_amount" in borough_agg else ["trip_count"], ascending=False)
        )
        outputs["borough_preview"] = _records_preview(borough_df, preview_rows)

    if {"pickup_borough", "pickup_zone"}.issubset(dataframe.columns):
        zone_group_columns = ["pickup_borough", "pickup_zone"]
        if "pickup_service_zone" in dataframe.columns:
            zone_group_columns.append("pickup_service_zone")
        zone_agg: dict[str, tuple[str, str]] = {"trip_count": ("pickup_zone", "size")}
        if "total_amount" in dataframe.columns:
            zone_agg["sum_total_amount"] = ("total_amount", "sum")
            zone_agg["avg_total_amount"] = ("total_amount", "mean")
        if "trip_distance" in dataframe.columns:
            zone_agg["avg_trip_distance"] = ("trip_distance", "mean")
        if "tip_pct" in dataframe.columns:
            zone_agg["avg_tip_pct"] = ("tip_pct", "mean")
        zone_df = (
            dataframe.groupby(zone_group_columns, dropna=False)
            .agg(**zone_agg)
            .reset_index()
            .sort_values(["sum_total_amount", "trip_count"] if "sum_total_amount" in zone_agg else ["trip_count"], ascending=False)
        )
        outputs["zone_preview"] = _records_preview(zone_df, preview_rows)

    if "payment_type_label" in dataframe.columns:
        payment_group_columns = ["payment_type_label"]
        if "payment_type" in dataframe.columns:
            payment_group_columns.insert(0, "payment_type")
        payment_agg: dict[str, tuple[str, str]] = {"trip_count": ("payment_type_label", "size")}
        if "fare_amount" in dataframe.columns:
            payment_agg["avg_fare_amount"] = ("fare_amount", "mean")
            payment_agg["sum_fare_amount"] = ("fare_amount", "sum")
        if "tip_amount" in dataframe.columns:
            payment_agg["avg_tip_amount"] = ("tip_amount", "mean")
            payment_agg["sum_tip_amount"] = ("tip_amount", "sum")
        if "tip_pct" in dataframe.columns:
            payment_agg["avg_tip_pct"] = ("tip_pct", "mean")
        if "total_amount" in dataframe.columns:
            payment_agg["sum_total_amount"] = ("total_amount", "sum")
        payment_df = dataframe.groupby(payment_group_columns, dropna=False).agg(**payment_agg).reset_index()
        if {"sum_fare_amount", "sum_tip_amount"}.issubset(payment_df.columns):
            payment_df["tip_to_fare_pct"] = np.where(
                payment_df["sum_fare_amount"] > 0,
                (payment_df["sum_tip_amount"] / payment_df["sum_fare_amount"]) * 100.0,
                np.nan,
            )
        payment_df = payment_df.sort_values("payment_type" if "payment_type" in payment_df.columns else "payment_type_label")
        outputs["payment_tip_preview"] = _records_preview(payment_df, preview_rows)

    return outputs


def benchmark_pandas(
    *,
    input_path: str,
    zone_lookup_path: str,
    pickup_year: int | None,
    row_limit: int | None,
    preview_rows: int,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    dataframe = load_pandas_trip_dataframe(input_path, row_limit=row_limit)
    zone_lookup_df = load_pandas_zone_lookup(zone_lookup_path)
    prepared_df = prepare_pandas_trip_dataframe(dataframe, pickup_year=pickup_year)
    enriched_df = enrich_pandas_with_zone_lookup(prepared_df, zone_lookup_df)
    outputs = compute_pandas_outputs(enriched_df, preview_rows=preview_rows)
    duration_seconds = time.perf_counter() - started_at

    return {
        "engine": "pandas",
        "row_limit": row_limit,
        "row_limit_label": format_row_limit(row_limit),
        "duration_seconds": round(duration_seconds, 4),
        "outputs": outputs,
    }


def _spark_preview(dataframe: Any, preview_rows: int) -> list[dict[str, Any]]:
    if dataframe is None:
        return []
    return [_serializable_dict(row.asDict(recursive=True)) for row in dataframe.limit(preview_rows).collect()]


def benchmark_pyspark(
    spark: Any,
    *,
    input_path: str,
    zone_lookup_path: str,
    pickup_year: int | None,
    row_limit: int | None,
    preview_rows: int,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    trip_df = load_trip_dataframe(spark, input_path)
    if row_limit is not None:
        trip_df = trip_df.limit(row_limit)

    zone_lookup_df = load_zone_lookup_dataframe(spark, zone_lookup_path)
    prepared_df = prepare_trip_dataframe(trip_df)
    enriched_df = enrich_with_zone_lookup(prepared_df, zone_lookup_df)
    analysis_df = filter_trip_dataframe_for_analysis(enriched_df, pickup_year=pickup_year)

    outputs = {
        "summary": _serializable_dict(compute_summary_metrics(analysis_df).first().asDict(recursive=True)),
        "hourly_preview": _spark_preview(compute_hourly_pickup_counts(analysis_df), preview_rows),
        "borough_preview": _spark_preview(compute_borough_summary(analysis_df), preview_rows),
        "zone_preview": _spark_preview(compute_zone_summary(analysis_df), preview_rows),
        "payment_tip_preview": _spark_preview(compute_payment_tip_summary(analysis_df), preview_rows),
    }
    duration_seconds = time.perf_counter() - started_at

    return {
        "engine": "pyspark",
        "row_limit": row_limit,
        "row_limit_label": format_row_limit(row_limit),
        "duration_seconds": round(duration_seconds, 4),
        "outputs": outputs,
    }


def warmup_pyspark_session(spark: Any, input_path: str) -> None:
    trip_df = load_trip_dataframe(spark, input_path).limit(1000)
    prepared_df = prepare_trip_dataframe(trip_df)
    prepared_df.count()


def build_comparison_rows(results: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, dict[str, Any]]] = {}
    for result in results:
        buckets.setdefault(result["row_limit_label"], {})[result["engine"]] = result

    comparison_rows: list[dict[str, Any]] = []
    for row_limit_label, engines in buckets.items():
        row: dict[str, Any] = {
            "row_limit": row_limit_label,
            "pandas_seconds": engines.get("pandas", {}).get("duration_seconds"),
            "pyspark_seconds": engines.get("pyspark", {}).get("duration_seconds"),
        }
        pandas_seconds = row["pandas_seconds"]
        pyspark_seconds = row["pyspark_seconds"]
        if isinstance(pandas_seconds, (int, float)) and isinstance(pyspark_seconds, (int, float)) and pandas_seconds > 0:
            row["spark_vs_pandas_ratio"] = round(pyspark_seconds / pandas_seconds, 3)
        comparison_rows.append(row)

    def _sort_key(item: dict[str, Any]) -> tuple[int, int]:
        if item["row_limit"] == "full":
            return (1, 10**12)
        return (0, int(str(item["row_limit"]).replace(",", "")))

    return sorted(comparison_rows, key=_sort_key)


def build_markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# pandas vs PySpark Benchmark",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Input path: `{report['input_path']}`",
        f"- Zone lookup path: `{report['zone_lookup_path']}`",
        f"- Pickup year filter: `{report['pickup_year']}`",
        f"- Spark startup seconds: `{report.get('spark_startup_seconds')}`",
        "",
        "## Runtime comparison",
        "",
        "| Row limit | pandas (s) | PySpark (s) | Spark / pandas |",
        "|---|---:|---:|---:|",
    ]

    for row in report["comparison_rows"]:
        lines.append(
            f"| {row['row_limit']} | {row.get('pandas_seconds')} | {row.get('pyspark_seconds')} | {row.get('spark_vs_pandas_ratio')} |"
        )

    lines.extend(
        [
            "",
            "## Result previews",
            "",
        ]
    )

    for result in report["results"]:
        lines.extend(
            [
                f"### {result['engine']} / {result['row_limit_label']}",
                "",
                f"- duration_seconds: `{result['duration_seconds']}`",
                f"- summary: `{json.dumps(result['outputs']['summary'], ensure_ascii=False)}`",
                "",
            ]
        )

    return "\n".join(lines) + "\n"


def write_report_files(report: dict[str, Any], output_path: Path) -> tuple[Path, Path]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    json_path = output_path.with_suffix(".json")
    markdown_path = output_path.with_suffix(".md")
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    markdown_path.write_text(build_markdown_report(report), encoding="utf-8")
    return json_path, markdown_path


def main() -> None:
    args = build_parser().parse_args()
    row_limits = parse_row_limits(args.row_limits)
    results: list[dict[str, Any]] = []
    spark_startup_seconds: float | None = None
    spark = None

    try:
        if "pandas" in args.engines:
            for row_limit in row_limits:
                result = benchmark_pandas(
                    input_path=args.input_path,
                    zone_lookup_path=args.zone_lookup_path,
                    pickup_year=args.pickup_year,
                    row_limit=row_limit,
                    preview_rows=args.preview_rows,
                )
                print(f"[benchmark] pandas row_limit={result['row_limit_label']} duration={result['duration_seconds']}s")
                results.append(result)

        if "pyspark" in args.engines:
            spark_started = time.perf_counter()
            spark = create_spark_session(app_name="streampipe-benchmark", with_delta=False)
            spark_startup_seconds = round(time.perf_counter() - spark_started, 4)
            print(f"[benchmark] pyspark session startup={spark_startup_seconds}s")
            warmup_pyspark_session(spark, args.input_path)
            print("[benchmark] pyspark warmup complete")

            for row_limit in row_limits:
                result = benchmark_pyspark(
                    spark,
                    input_path=args.input_path,
                    zone_lookup_path=args.zone_lookup_path,
                    pickup_year=args.pickup_year,
                    row_limit=row_limit,
                    preview_rows=args.preview_rows,
                )
                print(f"[benchmark] pyspark row_limit={result['row_limit_label']} duration={result['duration_seconds']}s")
                results.append(result)
    finally:
        if spark is not None:
            spark.stop()

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": args.input_path,
        "zone_lookup_path": args.zone_lookup_path,
        "pickup_year": args.pickup_year,
        "python_version": platform.python_version(),
        "row_limits": [format_row_limit(limit) for limit in row_limits],
        "engines": args.engines,
        "spark_startup_seconds": spark_startup_seconds,
        "comparison_rows": build_comparison_rows(results),
        "results": results,
    }

    json_path, markdown_path = write_report_files(report, Path(args.output_path))
    print(f"[benchmark] json report: {json_path}")
    print(f"[benchmark] markdown    : {markdown_path}")


if __name__ == "__main__":
    main()
