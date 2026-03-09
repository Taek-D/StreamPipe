from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from src.common.schemas import CLICKSTREAM_EVENT_SCHEMA
from src.common.spark import build_spark_session

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "clickstream.events"
DEFAULT_CHECKPOINT = "data/checkpoints/clickstream"
DEFAULT_OUTPUT_PATH = "data/processed/streaming_metrics"


@dataclass(frozen=True, slots=True)
class AlertThresholds:
    spike_threshold: int = 20
    drop_threshold: int = 2
    min_baseline_sessions: int = 5
    min_purchase_from_page_rate: float = 3.0
    min_purchase_from_cart_rate: float = 20.0
    traffic_zscore_threshold: float = 2.0
    traffic_baseline_std_floor: float = 0.1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the StreamPipe streaming analytics job.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to subscribe")
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["latest", "earliest"],
        help="Kafka startingOffsets setting",
    )
    parser.add_argument("--checkpoint-location", default=DEFAULT_CHECKPOINT, help="Base checkpoint directory")
    parser.add_argument(
        "--sink",
        "--output-mode",
        dest="sink",
        default="console",
        choices=["console", "memory", "parquet"],
        help="Streaming sink type",
    )
    parser.add_argument("--query-name", default="clickstream_metrics", help="Base query name")
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT_PATH, help="Base output path for parquet sink")
    parser.add_argument("--window-duration", default="5 minutes", help="Window duration for aggregation")
    parser.add_argument("--slide-duration", default="5 minutes", help="Slide duration for aggregation")
    parser.add_argument("--watermark", default="10 minutes", help="Watermark threshold")
    parser.add_argument("--master", default=None, help="Optional Spark master URL")
    parser.add_argument("--app-name", default="streampipe-streaming", help="Spark application name")
    parser.add_argument("--trigger-seconds", type=int, default=10, help="Micro-batch trigger interval")
    parser.add_argument(
        "--kafka-package",
        default="",
        help="Optional spark-sql-kafka package coordinate for local execution",
    )
    parser.add_argument(
        "--spark-jars",
        nargs="*",
        default=[],
        help="Optional local Spark jar paths to use instead of resolving packages online",
    )
    parser.add_argument("--anomaly-spike-threshold", type=int, default=20, help="Alert when total sessions exceed this threshold")
    parser.add_argument("--anomaly-drop-threshold", type=int, default=2, help="Alert when total sessions fall to or below this threshold")
    parser.add_argument(
        "--anomaly-min-baseline-sessions",
        type=int,
        default=5,
        help="Minimum sessions required before conversion alerts are emitted",
    )
    parser.add_argument(
        "--min-purchase-from-page-rate",
        type=float,
        default=3.0,
        help="Minimum purchase/page conversion rate percentage before alerting",
    )
    parser.add_argument(
        "--min-purchase-from-cart-rate",
        type=float,
        default=20.0,
        help="Minimum purchase/cart conversion rate percentage before alerting",
    )
    parser.add_argument("--await-termination", action="store_true", help="Block and await termination")
    return parser


def build_query_name(base_name: str, suffix: str) -> str:
    return f"{base_name}_{suffix}"


def calculate_rate(numerator: int | float, denominator: int | float) -> float | None:
    if denominator <= 0:
        return None
    return round((numerator / denominator) * 100.0, 4)


def calculate_z_score(
    value: int | float,
    mean: int | float | None,
    stddev: int | float | None,
    *,
    stddev_floor: float = 1.0,
) -> float | None:
    if mean is None or stddev is None:
        return None

    safe_stddev = max(float(stddev), stddev_floor)
    if safe_stddev <= 0:
        return None
    return round((float(value) - float(mean)) / safe_stddev, 4)


def evaluate_anomaly_flags(
    *,
    total_sessions: int,
    page_view_sessions: int,
    add_to_cart_sessions: int,
    purchase_sessions: int,
    thresholds: AlertThresholds,
    baseline_mean: float | None = None,
    baseline_stddev: float | None = None,
) -> list[str]:
    alerts: list[str] = []

    purchase_from_page_rate = calculate_rate(purchase_sessions, page_view_sessions) or 0.0
    purchase_from_cart_rate = calculate_rate(purchase_sessions, add_to_cart_sessions) or 0.0
    traffic_z_score = calculate_z_score(
        total_sessions,
        baseline_mean,
        baseline_stddev,
        stddev_floor=thresholds.traffic_baseline_std_floor,
    )

    if traffic_z_score is not None:
        if traffic_z_score >= thresholds.traffic_zscore_threshold:
            alerts.append("traffic_spike")
        if traffic_z_score <= -thresholds.traffic_zscore_threshold:
            alerts.append("traffic_drop")
    else:
        if total_sessions >= thresholds.spike_threshold:
            alerts.append("traffic_spike")
        if 0 < total_sessions <= thresholds.drop_threshold:
            alerts.append("traffic_drop")
    if page_view_sessions >= thresholds.min_baseline_sessions and purchase_from_page_rate < thresholds.min_purchase_from_page_rate:
        alerts.append("low_purchase_conversion")
    if add_to_cart_sessions >= thresholds.min_baseline_sessions and purchase_from_cart_rate < thresholds.min_purchase_from_cart_rate:
        alerts.append("cart_abandonment_risk")

    return alerts


def create_spark_session(
    app_name: str,
    master: str | None = None,
    kafka_package: str = "",
    spark_jars: list[str] | None = None,
) -> "SparkSession":
    packages = [kafka_package] if kafka_package else None
    return build_spark_session(
        app_name=app_name,
        master_url=master,
        extra_packages=packages,
        extra_jars=spark_jars,
        extra_configs={
            "spark.sql.shuffle.partitions": "8",
            "spark.default.parallelism": "4",
        },
    )


def load_kafka_stream(
    spark: "SparkSession",
    bootstrap_servers: str,
    topic: str,
    *,
    starting_offsets: str = "latest",
) -> "DataFrame":
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_clickstream_events(kafka_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    parsed = kafka_df.select(
        F.from_json(F.col("value").cast("string"), CLICKSTREAM_EVENT_SCHEMA).alias("event"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    return (
        parsed.select("event.*", "kafka_timestamp")
        .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp")))
        .drop("timestamp")
        .filter(F.col("event_timestamp").isNotNull())
    )


def build_windowed_event_metrics(
    event_df: "DataFrame",
    watermark: str,
    window_duration: str,
    slide_duration: str,
) -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        event_df.withWatermark("event_timestamp", watermark)
        .groupBy(
            F.window(F.col("event_timestamp"), window_duration, slide_duration),
            F.col("event_type"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("user_id").alias("approx_unique_users"),
            F.approx_count_distinct("session_id").alias("approx_unique_sessions"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("event_type"),
            F.col("event_count"),
            F.col("approx_unique_users"),
            F.col("approx_unique_sessions"),
        )
    )


def build_funnel_metrics(
    event_df: "DataFrame",
    watermark: str,
    window_duration: str,
    slide_duration: str,
) -> "DataFrame":
    from pyspark.sql import functions as F

    aggregated = (
        event_df.withWatermark("event_timestamp", watermark)
        .groupBy(F.window(F.col("event_timestamp"), window_duration, slide_duration))
        .agg(
            F.approx_count_distinct("session_id").alias("total_sessions"),
            F.approx_count_distinct("user_id").alias("total_users"),
            F.approx_count_distinct(F.when(F.col("event_type") == "page_view", F.col("session_id"))).alias("page_view_sessions"),
            F.approx_count_distinct(F.when(F.col("event_type") == "product_view", F.col("session_id"))).alias("product_view_sessions"),
            F.approx_count_distinct(F.when(F.col("event_type") == "add_to_cart", F.col("session_id"))).alias("add_to_cart_sessions"),
            F.approx_count_distinct(F.when(F.col("event_type") == "purchase", F.col("session_id"))).alias("purchase_sessions"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_sessions",
            "total_users",
            "page_view_sessions",
            "product_view_sessions",
            "add_to_cart_sessions",
            "purchase_sessions",
        )
    )

    return (
        aggregated.withColumn(
            "page_to_product_rate",
            F.when(F.col("page_view_sessions") > 0, (F.col("product_view_sessions") / F.col("page_view_sessions")) * F.lit(100.0)),
        )
        .withColumn(
            "product_to_cart_rate",
            F.when(F.col("product_view_sessions") > 0, (F.col("add_to_cart_sessions") / F.col("product_view_sessions")) * F.lit(100.0)),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.when(F.col("add_to_cart_sessions") > 0, (F.col("purchase_sessions") / F.col("add_to_cart_sessions")) * F.lit(100.0)),
        )
        .withColumn(
            "page_to_purchase_rate",
            F.when(F.col("page_view_sessions") > 0, (F.col("purchase_sessions") / F.col("page_view_sessions")) * F.lit(100.0)),
        )
    )


def build_anomaly_alerts(funnel_df: "DataFrame", thresholds: AlertThresholds) -> "DataFrame":
    from pyspark.sql import functions as F

    alert_candidates = F.array(
        F.when(F.col("total_sessions") >= F.lit(thresholds.spike_threshold), F.lit("traffic_spike")),
        F.when(
            (F.col("total_sessions") > 0) & (F.col("total_sessions") <= F.lit(thresholds.drop_threshold)),
            F.lit("traffic_drop"),
        ),
        F.when(
            (F.col("page_view_sessions") >= F.lit(thresholds.min_baseline_sessions))
            & (F.coalesce(F.col("page_to_purchase_rate"), F.lit(0.0)) < F.lit(thresholds.min_purchase_from_page_rate)),
            F.lit("low_purchase_conversion"),
        ),
        F.when(
            (F.col("add_to_cart_sessions") >= F.lit(thresholds.min_baseline_sessions))
            & (F.coalesce(F.col("cart_to_purchase_rate"), F.lit(0.0)) < F.lit(thresholds.min_purchase_from_cart_rate)),
            F.lit("cart_abandonment_risk"),
        ),
    )

    alerts_df = (
        funnel_df.withColumn("_alert_candidates", alert_candidates)
        .withColumn("alert_types", F.expr("filter(_alert_candidates, x -> x is not null)"))
        .drop("_alert_candidates")
        .withColumn("alert_count", F.size(F.col("alert_types")))
        .filter(F.col("alert_count") > 0)
        .withColumn(
            "severity",
            F.when(
                F.array_contains(F.col("alert_types"), "traffic_spike")
                | F.array_contains(F.col("alert_types"), "low_purchase_conversion"),
                F.lit("high"),
            )
            .when(F.array_contains(F.col("alert_types"), "cart_abandonment_risk"), F.lit("medium"))
            .otherwise(F.lit("low")),
        )
    )

    return alerts_df.select(
        "window_start",
        "window_end",
        "total_sessions",
        "page_view_sessions",
        "add_to_cart_sessions",
        "purchase_sessions",
        "page_to_purchase_rate",
        "cart_to_purchase_rate",
        "alert_types",
        "alert_count",
        "severity",
    )


def start_stream_query(
    metrics_df: "DataFrame",
    *,
    sink: str,
    checkpoint_base: str,
    query_name: str,
    trigger_seconds: int,
    output_base_path: str,
    output_mode: str = "update",
) -> "StreamingQuery":
    checkpoint_path = str(Path(checkpoint_base) / query_name)
    writer = (
        metrics_df.writeStream.outputMode(output_mode)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=f"{trigger_seconds} seconds")
    )

    if sink == "memory":
        return writer.format("memory").queryName(query_name).start()

    if sink == "parquet":
        output_path = Path(output_base_path) / query_name
        output_path.mkdir(parents=True, exist_ok=True)
        return writer.outputMode("append").format("parquet").option("path", str(output_path)).start()

    return writer.format("console").option("truncate", "false").option("numRows", 50).start()


def main() -> None:
    args = build_parser().parse_args()
    thresholds = AlertThresholds(
        spike_threshold=args.anomaly_spike_threshold,
        drop_threshold=args.anomaly_drop_threshold,
        min_baseline_sessions=args.anomaly_min_baseline_sessions,
        min_purchase_from_page_rate=args.min_purchase_from_page_rate,
        min_purchase_from_cart_rate=args.min_purchase_from_cart_rate,
    )

    spark = create_spark_session(
        app_name=args.app_name,
        master=args.master,
        kafka_package=args.kafka_package,
        spark_jars=args.spark_jars,
    )

    queries: list[StreamingQuery] = []

    try:
        kafka_df = load_kafka_stream(
            spark,
            args.bootstrap_servers,
            args.topic,
            starting_offsets=args.starting_offsets,
        )
        event_df = parse_clickstream_events(kafka_df)
        event_metrics_df = build_windowed_event_metrics(
            event_df,
            watermark=args.watermark,
            window_duration=args.window_duration,
            slide_duration=args.slide_duration,
        )
        funnel_metrics_df = build_funnel_metrics(
            event_df,
            watermark=args.watermark,
            window_duration=args.window_duration,
            slide_duration=args.slide_duration,
        )
        anomaly_alerts_df = build_anomaly_alerts(funnel_metrics_df, thresholds)

        queries.append(
            start_stream_query(
                event_metrics_df,
                sink=args.sink,
                checkpoint_base=args.checkpoint_location,
                query_name=build_query_name(args.query_name, "events"),
                trigger_seconds=args.trigger_seconds,
                output_base_path=args.output_path,
            )
        )
        queries.append(
            start_stream_query(
                funnel_metrics_df,
                sink=args.sink,
                checkpoint_base=args.checkpoint_location,
                query_name=build_query_name(args.query_name, "funnel"),
                trigger_seconds=args.trigger_seconds,
                output_base_path=args.output_path,
            )
        )
        queries.append(
            start_stream_query(
                anomaly_alerts_df,
                sink=args.sink,
                checkpoint_base=args.checkpoint_location,
                query_name=build_query_name(args.query_name, "anomalies"),
                trigger_seconds=args.trigger_seconds,
                output_base_path=args.output_path,
            )
        )

        print("=== StreamPipe Streaming Analytics ===")
        print(f"bootstrap_servers      : {args.bootstrap_servers}")
        print(f"topic                  : {args.topic}")
        print(f"starting_offsets       : {args.starting_offsets}")
        print(f"sink                   : {args.sink}")
        print(f"checkpoint_base        : {args.checkpoint_location}")
        print(f"query_name             : {args.query_name}")
        print(f"output_path            : {args.output_path}")
        print(f"window_duration        : {args.window_duration}")
        print(f"slide_duration         : {args.slide_duration}")
        print(f"watermark              : {args.watermark}")
        print(f"anomaly_spike_threshold: {args.anomaly_spike_threshold}")
        print(f"anomaly_drop_threshold : {args.anomaly_drop_threshold}")
        print("queries started:")
        for query in queries:
            print(f"  - {query.name}")

        if args.await_termination:
            spark.streams.awaitAnyTermination()
        else:
            print("Use --await-termination to keep the queries attached to this terminal.")
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
