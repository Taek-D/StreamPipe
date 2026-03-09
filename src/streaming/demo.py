from __future__ import annotations

import argparse
import json
import math
import shutil
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

from src.simulator.clickstream_generator import build_kafka_producer, send_event
from src.streaming.main import (
    AlertThresholds,
    build_anomaly_alerts,
    build_funnel_metrics,
    build_query_name,
    build_windowed_event_metrics,
    calculate_z_score,
    create_spark_session,
    evaluate_anomaly_flags,
    load_kafka_stream,
    parse_clickstream_events,
    start_stream_query,
)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "clickstream.events"
DEFAULT_CHECKPOINT = "data/checkpoints/clickstream_demo"
DEFAULT_OUTPUT_PATH = "data/processed/streaming_demo"
DEFAULT_REPORT_DIR = Path("docs/reports")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a deterministic in-memory streaming funnel/anomaly demo.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to publish and consume")
    parser.add_argument("--checkpoint-location", default=DEFAULT_CHECKPOINT, help="Checkpoint base directory")
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT_PATH, help="Unused base output path required by shared query starter")
    parser.add_argument("--query-name", default="clickstream_demo", help="Base query name")
    parser.add_argument("--window-duration", default="2 minutes", help="Window duration")
    parser.add_argument("--slide-duration", default="2 minutes", help="Slide duration")
    parser.add_argument("--watermark", default="2 minutes", help="Watermark threshold")
    parser.add_argument("--trigger-seconds", type=int, default=2, help="Micro-batch trigger interval")
    parser.add_argument("--master", default=None, help="Optional Spark master URL")
    parser.add_argument("--app-name", default="streampipe-stream-demo", help="Spark application name")
    parser.add_argument(
        "--report-path",
        default=None,
        help="Optional output report prefix without extension. Defaults to docs/reports/streaming_demo_result_YYYYMMDD",
    )
    parser.add_argument("--kafka-package", default="", help="Optional spark-sql-kafka package coordinate")
    parser.add_argument(
        "--spark-jars",
        nargs="*",
        default=[],
        help="Optional local Spark jar paths to use instead of resolving packages online",
    )
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["latest", "earliest"],
        help="Kafka startingOffsets setting",
    )
    parser.add_argument("--page-view-sessions", type=int, default=10, help="Distinct sessions with page_view events")
    parser.add_argument("--add-to-cart-sessions", type=int, default=6, help="Distinct sessions with add_to_cart events")
    parser.add_argument("--purchase-sessions", type=int, default=1, help="Distinct sessions with purchase events")
    parser.add_argument("--anomaly-spike-threshold", type=int, default=5, help="Alert threshold for traffic spikes")
    parser.add_argument("--anomaly-drop-threshold", type=int, default=1, help="Alert threshold for traffic drops")
    parser.add_argument(
        "--anomaly-min-baseline-sessions",
        type=int,
        default=5,
        help="Minimum page/cart baseline sessions before conversion alerts are emitted",
    )
    parser.add_argument(
        "--min-purchase-from-page-rate",
        type=float,
        default=40.0,
        help="Minimum purchase/page conversion rate percentage before alerting",
    )
    parser.add_argument(
        "--min-purchase-from-cart-rate",
        type=float,
        default=30.0,
        help="Minimum purchase/cart conversion rate percentage before alerting",
    )
    parser.add_argument(
        "--baseline-total-sessions",
        type=int,
        default=4,
        help="Synthetic recent-1-hour baseline session level used for Z-score traffic alerts in the demo report",
    )
    parser.add_argument(
        "--baseline-lookback-windows",
        type=int,
        default=12,
        help="Number of historical windows representing the recent 1-hour baseline",
    )
    return parser


def build_demo_event_payload(
    *,
    session_number: int,
    event_type: str,
    timestamp: str,
    scenario_prefix: str = "demo",
) -> dict[str, Any]:
    product_id = None if event_type == "page_view" else f"SKU-{session_number:04d}"
    page = {
        "page_view": "home",
        "product_view": "product",
        "add_to_cart": "cart",
        "purchase": "checkout",
    }[event_type]

    return {
        "event_id": f"{scenario_prefix}-event-{session_number:02d}-{event_type}",
        "user_id": f"{scenario_prefix}-user-{session_number:02d}",
        "session_id": f"{scenario_prefix}-session-{session_number:02d}",
        "event_type": event_type,
        "page": page,
        "product_id": product_id,
        "timestamp": timestamp,
    }


def build_demo_scenario_events(
    *,
    page_view_sessions: int,
    add_to_cart_sessions: int,
    purchase_sessions: int,
    timestamp: str | None = None,
    scenario_prefix: str = "demo",
) -> list[dict[str, Any]]:
    if page_view_sessions < 1:
        raise ValueError("page_view_sessions must be >= 1")
    if add_to_cart_sessions < 0 or purchase_sessions < 0:
        raise ValueError("add_to_cart_sessions and purchase_sessions must be >= 0")
    if add_to_cart_sessions > page_view_sessions:
        raise ValueError("add_to_cart_sessions cannot exceed page_view_sessions")
    if purchase_sessions > add_to_cart_sessions:
        raise ValueError("purchase_sessions cannot exceed add_to_cart_sessions")

    event_timestamp = timestamp or datetime.now(timezone.utc).isoformat()
    events: list[dict[str, Any]] = []

    for session_number in range(1, page_view_sessions + 1):
        events.append(
            build_demo_event_payload(
                session_number=session_number,
                event_type="page_view",
                timestamp=event_timestamp,
                scenario_prefix=scenario_prefix,
            )
        )
        events.append(
            build_demo_event_payload(
                session_number=session_number,
                event_type="product_view",
                timestamp=event_timestamp,
                scenario_prefix=scenario_prefix,
            )
        )

        if session_number <= add_to_cart_sessions:
            events.append(
                build_demo_event_payload(
                    session_number=session_number,
                    event_type="add_to_cart",
                    timestamp=event_timestamp,
                    scenario_prefix=scenario_prefix,
                )
            )

        if session_number <= purchase_sessions:
            events.append(
                build_demo_event_payload(
                    session_number=session_number,
                    event_type="purchase",
                    timestamp=event_timestamp,
                    scenario_prefix=scenario_prefix,
                )
            )

    return events


def summarize_demo_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    event_type_counts = dict(sorted(Counter(event["event_type"] for event in events).items()))
    session_ids = {event["session_id"] for event in events}
    user_ids = {event["user_id"] for event in events}

    return {
        "generated_event_count": len(events),
        "generated_session_count": len(session_ids),
        "generated_user_count": len(user_ids),
        "event_type_counts": event_type_counts,
    }


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 4)
    return value


def build_timing_summary(
    *,
    event_count: int,
    session_count: int,
    publish_seconds: float | None,
    processing_seconds: float | None,
) -> dict[str, float | None]:
    publish_duration = round(publish_seconds, 4) if publish_seconds is not None else None
    processing_duration = round(processing_seconds, 4) if processing_seconds is not None else None

    if publish_seconds is None or processing_seconds is None:
        end_to_end_seconds = None
    else:
        end_to_end_seconds = round(publish_seconds + processing_seconds, 4)

    events_per_second = None
    sessions_per_second = None
    if end_to_end_seconds and end_to_end_seconds > 0:
        events_per_second = round(event_count / end_to_end_seconds, 4)
        sessions_per_second = round(session_count / end_to_end_seconds, 4)

    return {
        "publish_seconds": publish_duration,
        "processing_seconds": processing_duration,
        "end_to_end_seconds": end_to_end_seconds,
        "events_per_second": events_per_second,
        "sessions_per_second": sessions_per_second,
    }


def resolve_report_path(report_path: str | None) -> Path:
    if report_path:
        return Path(report_path)
    stamp = datetime.now().strftime("%Y%m%d")
    return DEFAULT_REPORT_DIR / f"streaming_demo_result_{stamp}"


def build_baseline_session_series(
    *,
    baseline_total_sessions: int,
    lookback_windows: int,
) -> list[int]:
    if baseline_total_sessions < 1:
        raise ValueError("baseline_total_sessions must be >= 1")
    if lookback_windows < 2:
        raise ValueError("baseline_lookback_windows must be >= 2")

    offsets = (-1, 0, 1, 0)
    values: list[int] = []
    for index in range(lookback_windows):
        candidate = baseline_total_sessions + offsets[index % len(offsets)]
        values.append(max(candidate, 1))
    return values


def summarize_baseline_session_series(baseline_sessions: list[int]) -> dict[str, Any]:
    baseline_mean = round(mean(baseline_sessions), 4)
    baseline_stddev = round(pstdev(baseline_sessions), 4) if len(baseline_sessions) > 1 else 0.0
    return {
        "lookback_window_count": len(baseline_sessions),
        "recent_hour_total_sessions": baseline_sessions,
        "mean_total_sessions": baseline_mean,
        "stddev_total_sessions": baseline_stddev,
    }


def determine_alert_severity(alert_types: list[str]) -> str:
    if "traffic_spike" in alert_types or "low_purchase_conversion" in alert_types:
        return "high"
    if "cart_abandonment_risk" in alert_types:
        return "medium"
    return "low"


def build_demo_anomaly_rows(
    funnel_rows: list[dict[str, Any]],
    *,
    thresholds: AlertThresholds,
    baseline_sessions: list[int],
) -> list[dict[str, Any]]:
    if not funnel_rows:
        return []

    latest_row = sorted(funnel_rows, key=lambda row: str(row.get("window_end") or row.get("window_start") or ""))[-1]
    baseline_summary = summarize_baseline_session_series(baseline_sessions)
    total_sessions = int(latest_row.get("total_sessions") or 0)
    page_view_sessions = int(latest_row.get("page_view_sessions") or 0)
    add_to_cart_sessions = int(latest_row.get("add_to_cart_sessions") or 0)
    purchase_sessions = int(latest_row.get("purchase_sessions") or 0)

    alert_types = evaluate_anomaly_flags(
        total_sessions=total_sessions,
        page_view_sessions=page_view_sessions,
        add_to_cart_sessions=add_to_cart_sessions,
        purchase_sessions=purchase_sessions,
        thresholds=thresholds,
        baseline_mean=baseline_summary["mean_total_sessions"],
        baseline_stddev=baseline_summary["stddev_total_sessions"],
    )
    if not alert_types:
        return []

    return [
        {
            "window_start": serialize_value(latest_row.get("window_start")),
            "window_end": serialize_value(latest_row.get("window_end")),
            "total_sessions": total_sessions,
            "page_view_sessions": page_view_sessions,
            "add_to_cart_sessions": add_to_cart_sessions,
            "purchase_sessions": purchase_sessions,
            "page_to_purchase_rate": serialize_value(latest_row.get("page_to_purchase_rate")),
            "cart_to_purchase_rate": serialize_value(latest_row.get("cart_to_purchase_rate")),
            "baseline_window_count": baseline_summary["lookback_window_count"],
            "baseline_session_mean": baseline_summary["mean_total_sessions"],
            "baseline_session_stddev": baseline_summary["stddev_total_sessions"],
            "traffic_z_score": calculate_z_score(
                total_sessions,
                baseline_summary["mean_total_sessions"],
                baseline_summary["stddev_total_sessions"],
                stddev_floor=thresholds.traffic_baseline_std_floor,
            ),
            "alert_types": alert_types,
            "alert_count": len(alert_types),
            "severity": determine_alert_severity(alert_types),
            "anomaly_model": "zscore_vs_recent_hour",
        }
    ]


def _safe_progress_number(value: Any) -> float | int | None:
    if value is None:
        return None

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None

    if numeric.is_integer():
        return int(numeric)
    return round(numeric, 4)


def collect_query_progress_snapshots(queries: list[Any]) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []

    for query in queries:
        progress = query.lastProgress or {}
        duration_ms = progress.get("durationMs") if isinstance(progress, dict) else {}
        if not isinstance(duration_ms, dict):
            duration_ms = {}

        snapshots.append(
            {
                "query_name": query.name,
                "batch_id": progress.get("batchId") if isinstance(progress, dict) else None,
                "num_input_rows": _safe_progress_number(progress.get("numInputRows")) if isinstance(progress, dict) else None,
                "input_rows_per_second": _safe_progress_number(progress.get("inputRowsPerSecond")) if isinstance(progress, dict) else None,
                "processed_rows_per_second": _safe_progress_number(progress.get("processedRowsPerSecond")) if isinstance(progress, dict) else None,
                "trigger_execution_ms": _safe_progress_number(duration_ms.get("triggerExecution")),
            }
        )

    return snapshots


def collect_memory_rows(spark, query_name: str, order_by: str | None = None) -> list[dict[str, Any]]:
    order_clause = f" order by {order_by}" if order_by else ""
    return [row.asDict(recursive=True) for row in spark.sql(f"select * from {query_name}{order_clause}").collect()]


def clear_directory(path: str | Path) -> None:
    directory = Path(path)
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True, exist_ok=True)


def build_markdown_report(report: dict[str, Any]) -> str:
    scenario = report.get("scenario_summary", {})
    timing = report.get("timing", {})
    baseline = report.get("traffic_baseline", {})
    alert = (report.get("anomaly_alerts") or [{}])[0]

    lines = [
        "# Streaming Funnel And Anomaly Demo",
        "",
        f"- Generated at (UTC): {report.get('generated_at_utc')}",
        f"- Topic: `{(report.get('config') or {}).get('topic')}`",
        f"- Query name: `{(report.get('config') or {}).get('query_name')}`",
        "",
        "## Scenario summary",
        "",
        f"- Generated events: `{scenario.get('generated_event_count')}`",
        f"- Generated sessions: `{scenario.get('generated_session_count')}`",
        f"- Event mix: `{json.dumps(scenario.get('event_type_counts', {}), ensure_ascii=False)}`",
        "",
        "## Timing",
        "",
        f"- Publish seconds: `{timing.get('publish_seconds')}`",
        f"- Processing seconds: `{timing.get('processing_seconds')}`",
        f"- End-to-end seconds: `{timing.get('end_to_end_seconds')}`",
        f"- Events / sec: `{timing.get('events_per_second')}`",
        "",
        "## Z-score baseline",
        "",
        f"- Lookback windows: `{baseline.get('lookback_window_count')}`",
        f"- Mean total sessions: `{baseline.get('mean_total_sessions')}`",
        f"- Stddev total sessions: `{baseline.get('stddev_total_sessions')}`",
        f"- Recent hour session series: `{baseline.get('recent_hour_total_sessions')}`",
        "",
        "## Alerts",
        "",
        f"- Severity: `{alert.get('severity')}`",
        f"- Alert types: `{alert.get('alert_types')}`",
        f"- Traffic z-score: `{alert.get('traffic_z_score')}`",
        "",
        "## Report files",
        "",
        f"- JSON: `{report.get('report_json_path')}`",
        f"- Markdown: `{report.get('report_markdown_path')}`",
    ]
    return "\n".join(lines) + "\n"


def write_report_files(report: dict[str, Any], report_path: str | Path) -> tuple[Path, Path]:
    target = Path(report_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    json_path = target.with_suffix(".json")
    markdown_path = target.with_suffix(".md")
    json_path.write_text(json.dumps(report, ensure_ascii=False, default=serialize_value, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(build_markdown_report(report), encoding="utf-8")
    return json_path, markdown_path


def main() -> None:
    args = build_parser().parse_args()
    checkpoint_base = Path(args.checkpoint_location)
    output_base = Path(args.output_path)
    report_path = resolve_report_path(args.report_path)

    clear_directory(checkpoint_base)
    clear_directory(output_base)

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

    queries = []
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

        for suffix, metrics_df in (
            ("events", event_metrics_df),
            ("funnel", funnel_metrics_df),
            ("anomalies", anomaly_alerts_df),
        ):
            queries.append(
                start_stream_query(
                    metrics_df,
                    sink="memory",
                    checkpoint_base=str(checkpoint_base),
                    query_name=build_query_name(args.query_name, suffix),
                    trigger_seconds=args.trigger_seconds,
                    output_base_path=str(output_base),
                )
            )

        for query in queries:
            query.processAllAvailable()

        demo_events = build_demo_scenario_events(
            page_view_sessions=args.page_view_sessions,
            add_to_cart_sessions=args.add_to_cart_sessions,
            purchase_sessions=args.purchase_sessions,
            scenario_prefix=args.query_name,
        )
        scenario_summary = summarize_demo_events(demo_events)
        baseline_sessions = build_baseline_session_series(
            baseline_total_sessions=args.baseline_total_sessions,
            lookback_windows=args.baseline_lookback_windows,
        )

        producer = build_kafka_producer(args.bootstrap_servers)
        publish_started = time.perf_counter()
        try:
            for payload in demo_events:
                send_event(producer, args.topic, payload)
        finally:
            producer.flush()
            producer.close()
        publish_seconds = time.perf_counter() - publish_started

        processing_started = time.perf_counter()
        for query in queries:
            query.processAllAvailable()
        processing_seconds = time.perf_counter() - processing_started

        timing = build_timing_summary(
            event_count=scenario_summary["generated_event_count"],
            session_count=scenario_summary["generated_session_count"],
            publish_seconds=publish_seconds,
            processing_seconds=processing_seconds,
        )
        query_progress = collect_query_progress_snapshots(queries)
        event_metrics = collect_memory_rows(
            spark,
            build_query_name(args.query_name, "events"),
            order_by="window_start, event_type",
        )
        funnel_metrics = collect_memory_rows(
            spark,
            build_query_name(args.query_name, "funnel"),
            order_by="window_start",
        )
        anomaly_alerts = build_demo_anomaly_rows(
            funnel_metrics,
            thresholds=thresholds,
            baseline_sessions=baseline_sessions,
        )

        result = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "config": {
                "topic": args.topic,
                "query_name": args.query_name,
                "page_view_sessions": args.page_view_sessions,
                "add_to_cart_sessions": args.add_to_cart_sessions,
                "purchase_sessions": args.purchase_sessions,
            },
            "scenario_summary": scenario_summary,
            "timing": timing,
            "traffic_baseline": summarize_baseline_session_series(baseline_sessions),
            "query_progress": query_progress,
            "event_metrics": event_metrics,
            "funnel_metrics": funnel_metrics,
            "anomaly_alerts": anomaly_alerts,
        }
        result["report_json_path"] = str(Path(report_path).with_suffix(".json"))
        result["report_markdown_path"] = str(Path(report_path).with_suffix(".md"))
        json_path, markdown_path = write_report_files(result, report_path)
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
        print(f"[stream-demo] json report: {json_path}")
        print(f"[stream-demo] markdown   : {markdown_path}")
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
