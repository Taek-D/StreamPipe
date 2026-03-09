from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING

from src.common.spark import build_spark_session
from src.common.settings import get_settings
from src.quality.checks import append_taxi_quality_flags, compute_taxi_quality_summary, select_taxi_quality_issue_rows

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

SETTINGS = get_settings()
DEFAULT_INPUT_PATH = str(SETTINGS.raw_taxi_data_dir / "yellow" / "2023")
DEFAULT_OUTPUT_PATH = "data/processed/batch_taxi_metrics"
DEFAULT_ZONE_LOOKUP_PATH = str(SETTINGS.taxi_zone_lookup_path)
DEFAULT_PICKUP_YEAR = SETTINGS.taxi_pickup_year
PICKUP_TIMESTAMP_CANDIDATES = (
    "tpep_pickup_datetime",
    "pickup_datetime",
    "lpep_pickup_datetime",
)
PICKUP_LOCATION_ID_CANDIDATES = ("PULocationID", "PUlocationID")
DROPOFF_LOCATION_ID_CANDIDATES = ("DOLocationID", "DOlocationID")
PAYMENT_TYPE_LABELS = {
    1: "credit_card",
    2: "cash",
    3: "no_charge",
    4: "dispute",
    5: "unknown",
    6: "voided_trip",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the StreamPipe NYC Taxi batch analytics job.")
    parser.add_argument("--input-path", default=DEFAULT_INPUT_PATH, help="Parquet input path")
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT_PATH, help="Directory to write aggregates")
    parser.add_argument(
        "--output-format",
        default="parquet",
        choices=["parquet", "delta", "json"],
        help="Output storage format",
    )
    parser.add_argument(
        "--zone-lookup-path",
        default=DEFAULT_ZONE_LOOKUP_PATH,
        help="Taxi zone lookup CSV path for borough/zone joins",
    )
    parser.add_argument(
        "--pickup-year",
        type=int,
        default=DEFAULT_PICKUP_YEAR,
        help="Optional pickup year filter (useful when source files contain outlier timestamps)",
    )
    parser.add_argument("--master", default=None, help="Optional Spark master URL")
    parser.add_argument("--app-name", default="streampipe-batch", help="Spark application name")
    parser.add_argument("--show-rows", type=int, default=20, help="Number of rows to print for debug output")
    parser.add_argument(
        "--quality-sample-output-path",
        default=None,
        help="Optional override path for storing quality issue rows separately",
    )
    parser.add_argument("--skip-write", action="store_true", help="Skip writing results to disk")
    return parser


def create_spark_session(app_name: str, master: str | None = None, *, with_delta: bool = False) -> "SparkSession":
    return build_spark_session(
        app_name=app_name,
        master_url=master,
        with_delta=with_delta,
        extra_configs={
            "spark.sql.shuffle.partitions": "8",
            "spark.default.parallelism": "4",
        },
    )


def load_trip_dataframe(spark: "SparkSession", input_path: str) -> "DataFrame":
    local_path = Path(input_path)
    if not local_path.exists():
        raise SystemExit(f"Input path not found: {input_path}")
    reader = spark.read
    if local_path.is_dir():
        reader = reader.option("recursiveFileLookup", "true")
    return reader.parquet(input_path)


def load_zone_lookup_dataframe(spark: "SparkSession", zone_lookup_path: str) -> "DataFrame | None":
    local_path = Path(zone_lookup_path)
    if not local_path.exists():
        return None

    from pyspark.sql import functions as F

    return (
        spark.read.option("header", "true").option("inferSchema", "true").csv(zone_lookup_path)
        .select(
            F.col("LocationID").cast("long").alias("LocationID"),
            F.col("Borough").alias("Borough"),
            F.col("Zone").alias("Zone"),
            F.col("service_zone").alias("service_zone"),
        )
        .dropDuplicates(["LocationID"])
    )


def resolve_first_available_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def resolve_pickup_timestamp_column(dataframe: "DataFrame") -> str:
    column = resolve_first_available_column(dataframe.columns, PICKUP_TIMESTAMP_CANDIDATES)
    if column:
        return column

    available = ", ".join(dataframe.columns)
    raise ValueError(
        "No supported pickup timestamp column found. "
        f"Expected one of {PICKUP_TIMESTAMP_CANDIDATES}, got: {available}"
    )


def build_order_columns(dataframe: "DataFrame", *preferred: str) -> list["Column | str"]:
    from pyspark.sql import functions as F

    order_columns: list["Column | str"] = []
    for name in preferred:
        if name in dataframe.columns:
            if name.startswith("sum_") or name.startswith("avg_") or name.endswith("_count"):
                order_columns.append(F.desc(name))
            else:
                order_columns.append(name)
    return order_columns


def get_payment_type_label(value: int | float | str | None) -> str:
    try:
        code = int(value) if value is not None else None
    except (TypeError, ValueError):
        code = None
    return PAYMENT_TYPE_LABELS.get(code, "other")


def build_payment_type_label_expression(column_name: str) -> "Column":
    from pyspark.sql import functions as F

    mapping_items: list[Column] = []
    for code, label in PAYMENT_TYPE_LABELS.items():
        mapping_items.extend([F.lit(code), F.lit(label)])

    label_map = F.create_map(*mapping_items)
    return F.coalesce(label_map[F.col(column_name).cast("int")], F.lit("other"))


def prepare_trip_dataframe(dataframe: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    pickup_column = resolve_pickup_timestamp_column(dataframe)

    prepared = (
        dataframe.withColumn("pickup_ts", F.to_timestamp(F.col(pickup_column)))
        .filter(F.col("pickup_ts").isNotNull())
        .withColumn("pickup_date", F.to_date(F.col("pickup_ts")))
        .withColumn("pickup_hour", F.hour(F.col("pickup_ts")))
    )

    numeric_columns = ("passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount")
    for column_name in numeric_columns:
        if column_name in prepared.columns:
            prepared = prepared.withColumn(column_name, F.col(column_name).cast("double"))

    if "payment_type" in prepared.columns:
        prepared = prepared.withColumn("payment_type_label", build_payment_type_label_expression("payment_type"))

    if {"fare_amount", "tip_amount"}.issubset(prepared.columns):
        prepared = (
            prepared.withColumn(
                "tip_ratio",
                F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount")),
            )
            .withColumn(
                "tip_pct",
                F.when(F.col("fare_amount") > 0, (F.col("tip_amount") / F.col("fare_amount")) * F.lit(100.0)),
            )
        )

    return prepared


def filter_trip_dataframe_for_analysis(dataframe: "DataFrame", pickup_year: int | None = None) -> "DataFrame":
    from pyspark.sql import functions as F

    if pickup_year is None:
        return dataframe
    return dataframe.filter(F.year(F.col("pickup_ts")) == F.lit(pickup_year))


def enrich_with_zone_lookup(dataframe: "DataFrame", zone_lookup_df: "DataFrame | None") -> "DataFrame":
    from pyspark.sql import functions as F

    if zone_lookup_df is None:
        return dataframe

    enriched = dataframe
    pickup_location_column = resolve_first_available_column(dataframe.columns, PICKUP_LOCATION_ID_CANDIDATES)
    dropoff_location_column = resolve_first_available_column(dataframe.columns, DROPOFF_LOCATION_ID_CANDIDATES)

    if pickup_location_column:
        pickup_lookup = zone_lookup_df.select(
            F.col("LocationID").alias(pickup_location_column),
            F.col("Borough").alias("pickup_borough"),
            F.col("Zone").alias("pickup_zone"),
            F.col("service_zone").alias("pickup_service_zone"),
        )
        enriched = enriched.join(pickup_lookup, on=pickup_location_column, how="left")

    if dropoff_location_column:
        dropoff_lookup = zone_lookup_df.select(
            F.col("LocationID").alias(dropoff_location_column),
            F.col("Borough").alias("dropoff_borough"),
            F.col("Zone").alias("dropoff_zone"),
            F.col("service_zone").alias("dropoff_service_zone"),
        )
        enriched = enriched.join(dropoff_lookup, on=dropoff_location_column, how="left")

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
        enriched = enriched.fillna("Unknown", subset=fill_columns)

    return enriched


def compute_summary_metrics(dataframe: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    aggregations = [
        F.count("*").alias("row_count"),
        F.countDistinct("pickup_date").alias("active_days"),
        F.min("pickup_ts").alias("min_pickup_ts"),
        F.max("pickup_ts").alias("max_pickup_ts"),
    ]

    if "passenger_count" in dataframe.columns:
        aggregations.append(F.avg("passenger_count").alias("avg_passenger_count"))

    if "trip_distance" in dataframe.columns:
        aggregations.append(F.avg("trip_distance").alias("avg_trip_distance"))

    if "pickup_borough" in dataframe.columns:
        aggregations.append(F.countDistinct("pickup_borough").alias("active_pickup_boroughs"))

    if "total_amount" in dataframe.columns:
        aggregations.extend(
            [
                F.avg("total_amount").alias("avg_total_amount"),
                F.sum("total_amount").alias("sum_total_amount"),
            ]
        )

    if "tip_amount" in dataframe.columns:
        aggregations.append(F.sum("tip_amount").alias("sum_tip_amount"))

    if "tip_pct" in dataframe.columns:
        aggregations.append(F.avg("tip_pct").alias("avg_tip_pct"))

    return dataframe.agg(*aggregations)


def compute_hourly_pickup_counts(dataframe: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    aggregations: list[Column] = [F.count("*").alias("trip_count")]
    if "total_amount" in dataframe.columns:
        aggregations.append(F.sum("total_amount").alias("sum_total_amount"))

    return dataframe.groupBy("pickup_date", "pickup_hour").agg(*aggregations).orderBy("pickup_date", "pickup_hour")


def compute_borough_summary(dataframe: "DataFrame") -> "DataFrame | None":
    from pyspark.sql import functions as F

    if "pickup_borough" not in dataframe.columns:
        return None

    aggregations: list[Column] = [F.count("*").alias("trip_count")]
    if "total_amount" in dataframe.columns:
        aggregations.extend(
            [
                F.sum("total_amount").alias("sum_total_amount"),
                F.avg("total_amount").alias("avg_total_amount"),
            ]
        )
    if "trip_distance" in dataframe.columns:
        aggregations.append(F.avg("trip_distance").alias("avg_trip_distance"))
    if "tip_pct" in dataframe.columns:
        aggregations.append(F.avg("tip_pct").alias("avg_tip_pct"))

    summary_df = dataframe.groupBy("pickup_borough").agg(*aggregations)
    order_columns = build_order_columns(summary_df, "sum_total_amount", "trip_count")
    return summary_df.orderBy(*order_columns) if order_columns else summary_df


def compute_zone_summary(dataframe: "DataFrame") -> "DataFrame | None":
    from pyspark.sql import functions as F

    required_columns = {"pickup_borough", "pickup_zone"}
    if not required_columns.issubset(dataframe.columns):
        return None

    aggregations: list[Column] = [F.count("*").alias("trip_count")]
    if "total_amount" in dataframe.columns:
        aggregations.extend(
            [
                F.sum("total_amount").alias("sum_total_amount"),
                F.avg("total_amount").alias("avg_total_amount"),
            ]
        )
    if "trip_distance" in dataframe.columns:
        aggregations.append(F.avg("trip_distance").alias("avg_trip_distance"))
    if "tip_pct" in dataframe.columns:
        aggregations.append(F.avg("tip_pct").alias("avg_tip_pct"))

    grouping_columns = ["pickup_borough", "pickup_zone"]
    if "pickup_service_zone" in dataframe.columns:
        grouping_columns.append("pickup_service_zone")

    summary_df = dataframe.groupBy(*grouping_columns).agg(*aggregations)
    order_columns = build_order_columns(summary_df, "sum_total_amount", "trip_count")
    return summary_df.orderBy(*order_columns) if order_columns else summary_df


def compute_payment_tip_summary(dataframe: "DataFrame") -> "DataFrame | None":
    from pyspark.sql import functions as F

    if "payment_type_label" not in dataframe.columns:
        return None

    aggregations: list[Column] = [F.count("*").alias("trip_count")]
    if "fare_amount" in dataframe.columns:
        aggregations.extend(
            [
                F.avg("fare_amount").alias("avg_fare_amount"),
                F.sum("fare_amount").alias("sum_fare_amount"),
            ]
        )
    if "tip_amount" in dataframe.columns:
        aggregations.extend(
            [
                F.avg("tip_amount").alias("avg_tip_amount"),
                F.sum("tip_amount").alias("sum_tip_amount"),
            ]
        )
    if "tip_pct" in dataframe.columns:
        aggregations.append(F.avg("tip_pct").alias("avg_tip_pct"))
    if "total_amount" in dataframe.columns:
        aggregations.append(F.sum("total_amount").alias("sum_total_amount"))

    grouping_columns = ["payment_type_label"]
    if "payment_type" in dataframe.columns:
        grouping_columns.insert(0, "payment_type")

    summary_df = dataframe.groupBy(*grouping_columns).agg(*aggregations)
    if {"sum_fare_amount", "sum_tip_amount"}.issubset(summary_df.columns):
        summary_df = summary_df.withColumn(
            "tip_to_fare_pct",
            F.when(F.col("sum_fare_amount") > 0, (F.col("sum_tip_amount") / F.col("sum_fare_amount")) * F.lit(100.0)),
        )
    order_columns = build_order_columns(summary_df, "payment_type", "trip_count")
    return summary_df.orderBy(*order_columns) if order_columns else summary_df


def write_batch_outputs(outputs: dict[str, "DataFrame"], output_path: str, output_format: str) -> None:
    base_path = Path(output_path)
    for name, dataframe in outputs.items():
        target_path = str(base_path / name)
        dataframe.write.mode("overwrite").format(output_format).save(target_path)


def write_quality_issue_rows(
    dataframe: "DataFrame",
    *,
    output_path: str,
    output_format: str,
    override_path: str | None = None,
) -> None:
    target_path = override_path or str(Path(output_path) / "quality_issue_rows")
    dataframe.write.mode("overwrite").format(output_format).save(target_path)


def show_output(name: str, dataframe: "DataFrame | None", rows: int) -> None:
    if dataframe is None:
        return

    print(f"=== {name} ===")
    dataframe.show(rows, truncate=False)


def main() -> None:
    args = build_parser().parse_args()
    spark = create_spark_session(
        app_name=args.app_name,
        master=args.master,
        with_delta=args.output_format == "delta",
    )

    try:
        trip_df = load_trip_dataframe(spark, args.input_path)
        zone_lookup_df = load_zone_lookup_dataframe(spark, args.zone_lookup_path)
        prepared_df = prepare_trip_dataframe(trip_df)
        enriched_df = enrich_with_zone_lookup(prepared_df, zone_lookup_df)
        quality_flagged_df = append_taxi_quality_flags(enriched_df, pickup_year=args.pickup_year)
        analysis_df = filter_trip_dataframe_for_analysis(quality_flagged_df, pickup_year=args.pickup_year)

        outputs: dict[str, DataFrame] = {
            "summary": compute_summary_metrics(analysis_df),
            "hourly": compute_hourly_pickup_counts(analysis_df),
            "quality_summary": compute_taxi_quality_summary(quality_flagged_df, pickup_year=args.pickup_year),
        }

        borough_df = compute_borough_summary(analysis_df)
        if borough_df is not None:
            outputs["borough_summary"] = borough_df

        zone_df = compute_zone_summary(analysis_df)
        if zone_df is not None:
            outputs["zone_summary"] = zone_df

        payment_tip_df = compute_payment_tip_summary(analysis_df)
        if payment_tip_df is not None:
            outputs["payment_tip_summary"] = payment_tip_df

        quality_issue_rows_df = select_taxi_quality_issue_rows(quality_flagged_df)

        print("=== StreamPipe Batch Analytics ===")
        print(f"input_path        : {args.input_path}")
        print(f"zone_lookup_path  : {args.zone_lookup_path}")
        print(f"pickup_year       : {args.pickup_year}")
        print(f"output_path       : {args.output_path}")
        print(f"output_format     : {args.output_format}")
        print(f"quality_issue_out : {args.quality_sample_output_path or str(Path(args.output_path) / 'quality_issue_rows')}")

        for name, dataframe in outputs.items():
            show_output(name, dataframe, args.show_rows)
        show_output("quality_issue_rows", quality_issue_rows_df, args.show_rows)

        if not args.skip_write:
            write_batch_outputs(outputs, args.output_path, args.output_format)
            write_quality_issue_rows(
                quality_issue_rows_df,
                output_path=args.output_path,
                output_format=args.output_format,
                override_path=args.quality_sample_output_path,
            )
            print(f"Saved batch outputs to: {args.output_path}")
        else:
            print("Skip write enabled; no files were saved.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
