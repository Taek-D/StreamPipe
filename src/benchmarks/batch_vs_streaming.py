from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

DEFAULT_BENCHMARK_REPORT_PATH = Path("data/benchmarks/pandas_vs_spark_jan2023.json")
DEFAULT_OUTPUT_PATH = Path("data/benchmarks/batch_vs_streaming_jan2023")
DEFAULT_STREAMING_REPORT_GLOB = "streaming_demo_result_*.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a normalized batch-vs-streaming comparison report.")
    parser.add_argument(
        "--benchmark-report",
        default=str(DEFAULT_BENCHMARK_REPORT_PATH),
        help="Path to the pandas vs PySpark benchmark JSON report",
    )
    parser.add_argument(
        "--streaming-report",
        default=None,
        help="Path to the streaming demo JSON report. Defaults to the latest docs/reports/streaming_demo_result_*.json file.",
    )
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH), help="Output path prefix without extension")
    return parser


def load_json_report(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def resolve_latest_streaming_report(path: str | None = None) -> Path:
    if path:
        resolved = Path(path)
        if not resolved.exists():
            raise SystemExit(f"Streaming report not found: {resolved}")
        return resolved

    candidates = sorted(Path("docs/reports").glob(DEFAULT_STREAMING_REPORT_GLOB))
    if not candidates:
        raise SystemExit("No streaming demo report found under docs/reports/")
    return candidates[-1]


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _row_limit_sort_key(label: str) -> tuple[int, int]:
    if label == "full":
        return (1, 10**12)
    return (0, int(label.replace(",", "")))


def build_batch_profiles(benchmark_report: dict[str, Any]) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    spark_startup_seconds = _safe_float(benchmark_report.get("spark_startup_seconds"))

    for result in benchmark_report.get("results", []):
        summary = result.get("outputs", {}).get("summary", {})
        row_count = _safe_int(summary.get("row_count"))
        duration_seconds = _safe_float(result.get("duration_seconds"))
        startup_seconds = spark_startup_seconds if result.get("engine") == "pyspark" else None
        time_to_first_result_seconds = None
        if duration_seconds is not None:
            time_to_first_result_seconds = duration_seconds + (startup_seconds or 0.0)

        rows_per_second = None
        if row_count is not None and duration_seconds and duration_seconds > 0:
            rows_per_second = row_count / duration_seconds

        profiles.append(
            {
                "engine": result.get("engine"),
                "row_limit_label": result.get("row_limit_label"),
                "row_count": row_count,
                "duration_seconds": _round(duration_seconds),
                "startup_seconds": _round(startup_seconds),
                "time_to_first_result_seconds": _round(time_to_first_result_seconds),
                "rows_per_second": _round(rows_per_second),
            }
        )

    return sorted(profiles, key=lambda item: (str(item["engine"]), _row_limit_sort_key(str(item["row_limit_label"]))))


def select_primary_batch_profiles(batch_profiles: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}

    for profile in batch_profiles:
        engine = str(profile.get("engine"))
        current = selected.get(engine)
        if current is None:
            selected[engine] = profile
            continue

        current_label = str(current.get("row_limit_label"))
        candidate_label = str(profile.get("row_limit_label"))
        if candidate_label == "full":
            selected[engine] = profile
            continue
        if current_label != "full" and (profile.get("row_count") or 0) > (current.get("row_count") or 0):
            selected[engine] = profile

    return [selected[key] for key in sorted(selected)]


def build_streaming_profile(streaming_report: dict[str, Any]) -> dict[str, Any]:
    config = streaming_report.get("config", {})
    scenario_summary = streaming_report.get("scenario_summary", {})
    timing = streaming_report.get("timing", {})
    query_progress = streaming_report.get("query_progress", [])
    anomaly_alerts = streaming_report.get("anomaly_alerts", [])

    generated_event_count = _safe_int(scenario_summary.get("generated_event_count"))
    if generated_event_count is None:
        generated_event_count = sum(_safe_int(row.get("event_count")) or 0 for row in streaming_report.get("event_metrics", []))

    generated_session_count = _safe_int(scenario_summary.get("generated_session_count"))
    if generated_session_count is None:
        generated_session_count = _safe_int(config.get("page_view_sessions"))

    max_processed_rows_per_second = None
    progress_rates = [_safe_float(item.get("processed_rows_per_second")) for item in query_progress]
    progress_rates = [rate for rate in progress_rates if rate is not None]
    if progress_rates:
        max_processed_rows_per_second = max(progress_rates)

    alert_count = sum(_safe_int(row.get("alert_count")) or 0 for row in anomaly_alerts)
    severity = anomaly_alerts[0].get("severity") if anomaly_alerts else None

    return {
        "query_name": config.get("query_name"),
        "generated_event_count": generated_event_count,
        "generated_session_count": generated_session_count,
        "publish_seconds": _round(_safe_float(timing.get("publish_seconds"))),
        "processing_seconds": _round(_safe_float(timing.get("processing_seconds"))),
        "end_to_end_seconds": _round(_safe_float(timing.get("end_to_end_seconds"))),
        "events_per_second": _round(_safe_float(timing.get("events_per_second"))),
        "sessions_per_second": _round(_safe_float(timing.get("sessions_per_second"))),
        "observed_processed_rows_per_second": _round(max_processed_rows_per_second),
        "alert_count": alert_count,
        "severity": severity,
    }


def build_metric_definitions() -> list[dict[str, str]]:
    return [
        {
            "metric": "batch_latency_seconds",
            "pipeline": "batch",
            "definition": "A bounded NYC Taxi workload finishes and writes its final aggregate tables.",
        },
        {
            "metric": "streaming_latency_seconds",
            "pipeline": "streaming",
            "definition": "A clickstream demo event burst is published and materialized into streaming sink results.",
        },
        {
            "metric": "batch_throughput_rows_per_second",
            "pipeline": "batch",
            "definition": "Summary row_count divided by batch runtime seconds.",
        },
        {
            "metric": "streaming_throughput_events_per_second",
            "pipeline": "streaming",
            "definition": "Generated event count divided by end-to-end streaming seconds.",
        },
        {
            "metric": "comparison_rule",
            "pipeline": "shared",
            "definition": "Compare freshness and workload shape together because rows/s and events/s are different units.",
        },
    ]


def build_comparison_rows(
    batch_profiles: Sequence[dict[str, Any]],
    streaming_profile: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for profile in select_primary_batch_profiles(batch_profiles):
        rows.append(
            {
                "pipeline": "batch",
                "variant": f"{profile['engine']} / {profile['row_limit_label']}",
                "workload": "bounded NYC Taxi aggregation",
                "latency_seconds": profile.get("time_to_first_result_seconds"),
                "throughput": profile.get("rows_per_second"),
                "throughput_unit": "rows/s",
                "record_count": profile.get("row_count"),
            }
        )

    rows.append(
        {
            "pipeline": "streaming",
            "variant": str(streaming_profile.get("query_name") or "streaming_demo"),
            "workload": "clickstream event burst",
            "latency_seconds": streaming_profile.get("end_to_end_seconds"),
            "throughput": streaming_profile.get("events_per_second"),
            "throughput_unit": "events/s",
            "record_count": streaming_profile.get("generated_event_count"),
        }
    )

    return rows


def build_report(
    benchmark_report: dict[str, Any],
    streaming_report: dict[str, Any],
    *,
    benchmark_report_path: str,
    streaming_report_path: str,
) -> dict[str, Any]:
    batch_profiles = build_batch_profiles(benchmark_report)
    streaming_profile = build_streaming_profile(streaming_report)

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_report_path": benchmark_report_path,
        "streaming_report_path": streaming_report_path,
        "metric_definitions": build_metric_definitions(),
        "batch_profiles": batch_profiles,
        "streaming_profile": streaming_profile,
        "comparison_rows": build_comparison_rows(batch_profiles, streaming_profile),
        "notes": [
            "Batch latency is measured as time-to-final-output for a bounded dataset.",
            "Streaming latency is measured as publish-to-materialized-result for one deterministic demo burst.",
            "Throughput values are normalized within each pipeline type, not across incompatible units.",
        ],
    }


def build_markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Batch vs Streaming Benchmark",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Batch benchmark source: `{report['benchmark_report_path']}`",
        f"- Streaming benchmark source: `{report['streaming_report_path']}`",
        "",
        "## Metric definitions",
        "",
        "| Metric | Pipeline | Definition |",
        "|---|---|---|",
    ]

    for item in report["metric_definitions"]:
        lines.append(f"| {item['metric']} | {item['pipeline']} | {item['definition']} |")

    lines.extend(
        [
            "",
            "## Comparison snapshot",
            "",
            "| Pipeline | Variant | Workload | Latency (s) | Throughput | Unit | Record count |",
            "|---|---|---|---:|---:|---|---:|",
        ]
    )

    for row in report["comparison_rows"]:
        lines.append(
            f"| {row['pipeline']} | {row['variant']} | {row['workload']} | {row.get('latency_seconds')} | "
            f"{row.get('throughput')} | {row.get('throughput_unit')} | {row.get('record_count')} |"
        )

    lines.extend(
        [
            "",
            "## Batch profiles",
            "",
            "| Engine | Row limit | Row count | Runtime (s) | Startup (s) | Time to first result (s) | Rows/s |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )

    for row in report["batch_profiles"]:
        lines.append(
            f"| {row['engine']} | {row['row_limit_label']} | {row.get('row_count')} | {row.get('duration_seconds')} | "
            f"{row.get('startup_seconds')} | {row.get('time_to_first_result_seconds')} | {row.get('rows_per_second')} |"
        )

    streaming = report["streaming_profile"]
    lines.extend(
        [
            "",
            "## Streaming profile",
            "",
            "| Query | Events | Sessions | Publish (s) | Processing (s) | End-to-end (s) | Events/s | Sessions/s | Alerts | Severity |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
            (
                f"| {streaming.get('query_name')} | {streaming.get('generated_event_count')} | "
                f"{streaming.get('generated_session_count')} | {streaming.get('publish_seconds')} | "
                f"{streaming.get('processing_seconds')} | {streaming.get('end_to_end_seconds')} | "
                f"{streaming.get('events_per_second')} | {streaming.get('sessions_per_second')} | "
                f"{streaming.get('alert_count')} | {streaming.get('severity')} |"
            ),
            "",
            "## Notes",
            "",
        ]
    )

    for note in report["notes"]:
        lines.append(f"- {note}")

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
    benchmark_report_path = Path(args.benchmark_report)
    streaming_report_path = resolve_latest_streaming_report(args.streaming_report)

    if not benchmark_report_path.exists():
        raise SystemExit(f"Benchmark report not found: {benchmark_report_path}")

    benchmark_report = load_json_report(benchmark_report_path)
    streaming_report = load_json_report(streaming_report_path)
    report = build_report(
        benchmark_report,
        streaming_report,
        benchmark_report_path=str(benchmark_report_path),
        streaming_report_path=str(streaming_report_path),
    )

    json_path, markdown_path = write_report_files(report, Path(args.output_path))
    print(f"[benchmark] json report: {json_path}")
    print(f"[benchmark] markdown    : {markdown_path}")


if __name__ == "__main__":
    main()
