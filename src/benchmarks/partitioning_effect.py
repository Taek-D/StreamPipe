from __future__ import annotations

import argparse
import json
import shutil
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

from src.batch.main import create_spark_session

DEFAULT_OUTPUT_ROOT = Path("data/benchmarks/partitioning_effect_demo_data")
DEFAULT_REPORT_PATH = Path("data/benchmarks/partitioning_effect_demo")
DEFAULT_BOROUGHS = ("Manhattan", "Brooklyn", "Queens", "Bronx")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark partition pruning impact with a deterministic Spark dataset.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Directory used for benchmark tables")
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH), help="Output report path prefix without extension")
    parser.add_argument("--days", type=int, default=21, help="Number of pickup dates to generate")
    parser.add_argument("--rows-per-day", type=int, default=800, help="Synthetic rows per day")
    parser.add_argument("--repeats", type=int, default=3, help="Number of query timing repeats per layout")
    parser.add_argument("--master", default=None, help="Optional Spark master URL")
    parser.add_argument("--app-name", default="streampipe-partitioning-benchmark", help="Spark application name")
    return parser


def clear_directory(path: str | Path) -> None:
    target = Path(path)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 4)
    return value


def build_partitioning_demo_rows(
    *,
    days: int,
    rows_per_day: int,
    boroughs: Sequence[str] = DEFAULT_BOROUGHS,
) -> list[dict[str, Any]]:
    if days < 2:
        raise ValueError("days must be >= 2")
    if rows_per_day < len(boroughs):
        raise ValueError("rows_per_day must be >= number of boroughs")

    start_date = date(2026, 1, 1)
    rows: list[dict[str, Any]] = []

    for day_index in range(days):
        pickup_date = start_date + timedelta(days=day_index)
        for row_index in range(rows_per_day):
            borough = boroughs[row_index % len(boroughs)]
            zone_id = (row_index % 18) + 1
            rows.append(
                {
                    "record_id": f"{pickup_date.isoformat()}-{row_index:05d}",
                    "pickup_date": pickup_date.isoformat(),
                    "pickup_borough": borough,
                    "pickup_zone": f"{borough[:3].upper()}-{zone_id:02d}",
                    "trip_distance": round(1.5 + (row_index % 9) * 0.6 + day_index * 0.02, 3),
                    "total_amount": round(12.0 + (row_index % 11) * 1.7 + day_index * 0.15, 2),
                }
            )

    return rows


def benchmark_layout(
    spark,
    *,
    path: str,
    pickup_date: str,
    pickup_borough: str,
    repeats: int,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    durations: list[float] = []
    preview_rows: list[dict[str, Any]] = []

    for _ in range(repeats):
        spark.catalog.clearCache()
        started = time.perf_counter()
        query_df = (
            spark.read.parquet(path)
            .filter((F.col("pickup_date") == F.lit(pickup_date)) & (F.col("pickup_borough") == F.lit(pickup_borough)))
            .groupBy("pickup_date", "pickup_borough")
            .agg(
                F.count("*").alias("matched_rows"),
                F.sum("total_amount").alias("sum_total_amount"),
            )
        )
        preview_rows = [
            {key: serialize_value(value) for key, value in row.asDict(recursive=True).items()}
            for row in query_df.collect()
        ]
        durations.append(time.perf_counter() - started)

    avg_seconds = round(sum(durations) / len(durations), 4)
    return {
        "avg_seconds": avg_seconds,
        "min_seconds": round(min(durations), 4),
        "max_seconds": round(max(durations), 4),
        "matched_rows": preview_rows[0]["matched_rows"] if preview_rows else 0,
        "result_preview": preview_rows,
    }


def build_improvement_summary(layouts: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    baseline = next((layout for layout in layouts if layout.get("layout") == "unpartitioned"), None)
    baseline_seconds = baseline.get("avg_seconds") if baseline else None
    summary: list[dict[str, Any]] = []

    for layout in layouts:
        improvement_pct = None
        if baseline_seconds and layout.get("layout") != "unpartitioned":
            improvement_pct = round(((baseline_seconds - layout["avg_seconds"]) / baseline_seconds) * 100.0, 2)
        summary.append(
            {
                "layout": layout["layout"],
                "partition_columns": layout["partition_columns"],
                "avg_seconds": layout["avg_seconds"],
                "matched_rows": layout["matched_rows"],
                "improvement_vs_unpartitioned_pct": improvement_pct,
            }
        )

    return summary


def build_report(
    *,
    output_root: str,
    dataset_row_count: int,
    query_filter: dict[str, str],
    layouts: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "output_root": output_root,
        "dataset_row_count": dataset_row_count,
        "query_filter": query_filter,
        "layouts": list(layouts),
        "comparison_rows": build_improvement_summary(layouts),
    }


def build_markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Partitioning Effect Benchmark",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Output root: `{report['output_root']}`",
        f"- Dataset rows: `{report['dataset_row_count']}`",
        f"- Query filter: `{json.dumps(report['query_filter'], ensure_ascii=False)}`",
        "",
        "## Layout comparison",
        "",
        "| Layout | Partition columns | Avg seconds | Improvement vs unpartitioned (%) | Matched rows |",
        "|---|---|---:|---:|---:|",
    ]

    for row in report["comparison_rows"]:
        lines.append(
            f"| {row['layout']} | {', '.join(row['partition_columns']) or 'none'} | {row['avg_seconds']} | "
            f"{row.get('improvement_vs_unpartitioned_pct')} | {row['matched_rows']} |"
        )

    return "\n".join(lines) + "\n"


def write_report_files(report: dict[str, Any], output_path: str | Path) -> tuple[Path, Path]:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    json_path = target.with_suffix(".json")
    markdown_path = target.with_suffix(".md")
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(build_markdown_report(report), encoding="utf-8")
    return json_path, markdown_path


def main() -> None:
    args = build_parser().parse_args()
    output_root = Path(args.output_root)
    clear_directory(output_root)
    spark = create_spark_session(app_name=args.app_name, master=args.master, with_delta=False)

    try:
        rows = build_partitioning_demo_rows(days=args.days, rows_per_day=args.rows_per_day)
        dataframe = spark.createDataFrame(rows)

        layout_specs = [
            ("unpartitioned", []),
            ("partitioned_by_date", ["pickup_date"]),
            ("partitioned_by_date_borough", ["pickup_date", "pickup_borough"]),
        ]
        layout_rows: list[dict[str, Any]] = []

        for layout_name, partition_columns in layout_specs:
            layout_path = output_root / layout_name
            writer = dataframe.write.mode("overwrite")
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            writer.parquet(str(layout_path))
            benchmark_result = benchmark_layout(
                spark,
                path=str(layout_path),
                pickup_date=rows[-1]["pickup_date"],
                pickup_borough="Queens",
                repeats=args.repeats,
            )
            layout_rows.append(
                {
                    "layout": layout_name,
                    "path": str(layout_path),
                    "partition_columns": partition_columns,
                    **benchmark_result,
                }
            )

        report = build_report(
            output_root=str(output_root),
            dataset_row_count=len(rows),
            query_filter={"pickup_date": rows[-1]["pickup_date"], "pickup_borough": "Queens"},
            layouts=layout_rows,
        )
        json_path, markdown_path = write_report_files(report, args.report_path)
        print(f"[partitioning] json report: {json_path}")
        print(f"[partitioning] markdown   : {markdown_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
