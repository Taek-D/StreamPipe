from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import pyarrow.parquet as pq

from src.benchmarks.batch_vs_streaming import build_report as build_batch_vs_streaming_report

BATCH_DATASET_ROOTS = (
    Path("data/processed/delta/batch_taxi_metrics_jan_delta"),
    Path("data/processed/delta/batch_taxi_metrics"),
    Path("data/processed/batch_taxi_metrics_jan"),
    Path("data/processed/batch_taxi_metrics_yellow_jan"),
    Path("data/processed/batch_taxi_metrics"),
)
PANDAS_SPARK_REPORT_CANDIDATES = (
    Path("data/benchmarks/pandas_vs_spark_jan2023.json"),
    Path("data/benchmarks/pandas_vs_spark.json"),
)
BATCH_STREAMING_REPORT_CANDIDATES = (
    Path("data/benchmarks/batch_vs_streaming_jan2023.json"),
    Path("data/benchmarks/batch_vs_streaming.json"),
)
PARTITIONING_REPORT_CANDIDATES = (
    Path("data/benchmarks/partitioning_effect_demo.json"),
    Path("data/benchmarks/partitioning_effect.json"),
)
STREAMING_REPORT_GLOB = "streaming_demo_result_*.json"


def first_existing_path(candidates: Sequence[Path]) -> Path | None:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def find_latest_path(directory: str | Path, pattern: str) -> Path | None:
    paths = sorted(Path(directory).glob(pattern))
    if not paths:
        return None
    return paths[-1]


def load_json_file(path: str | Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    resolved = Path(path)
    if not resolved.exists():
        return None
    return json.loads(resolved.read_text(encoding="utf-8"))


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


def _latest_window_row(rows: Sequence[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return sorted(rows, key=lambda row: str(row.get("window_end") or row.get("window_start") or ""))[-1]


def normalize_streaming_report(report: dict[str, Any] | None) -> dict[str, Any] | None:
    if report is None:
        return None

    normalized = dict(report)
    event_metrics = list(normalized.get("event_metrics") or [])
    funnel_metrics = list(normalized.get("funnel_metrics") or [])
    query_progress = list(normalized.get("query_progress") or [])
    config = dict(normalized.get("config") or {})

    scenario_summary = dict(normalized.get("scenario_summary") or {})
    if not scenario_summary.get("generated_event_count"):
        scenario_summary["generated_event_count"] = sum(_safe_int(row.get("event_count")) or 0 for row in event_metrics)
    if not scenario_summary.get("generated_session_count"):
        latest_funnel = _latest_window_row(funnel_metrics) or {}
        scenario_summary["generated_session_count"] = (
            _safe_int(latest_funnel.get("total_sessions")) or _safe_int(config.get("page_view_sessions"))
        )
    if not scenario_summary.get("generated_user_count"):
        latest_funnel = _latest_window_row(funnel_metrics) or {}
        scenario_summary["generated_user_count"] = _safe_int(latest_funnel.get("total_users"))
    if not scenario_summary.get("event_type_counts"):
        scenario_summary["event_type_counts"] = {
            str(row.get("event_type")): sum(
                _safe_int(candidate.get("event_count")) or 0
                for candidate in event_metrics
                if candidate.get("event_type") == row.get("event_type")
            )
            for row in event_metrics
            if row.get("event_type") is not None
        }
    normalized["scenario_summary"] = scenario_summary

    timing = dict(normalized.get("timing") or {})
    if not timing:
        latest_trigger_ms = max((_safe_float(item.get("trigger_execution_ms")) or 0.0) for item in query_progress) if query_progress else 0.0
        processing_seconds = round(latest_trigger_ms / 1000.0, 4) if latest_trigger_ms > 0 else None
        event_count = _safe_int(scenario_summary.get("generated_event_count"))
        session_count = _safe_int(scenario_summary.get("generated_session_count"))
        end_to_end_seconds = processing_seconds
        timing = {
            "publish_seconds": None,
            "processing_seconds": processing_seconds,
            "end_to_end_seconds": end_to_end_seconds,
            "events_per_second": round(event_count / end_to_end_seconds, 4)
            if event_count is not None and end_to_end_seconds
            else None,
            "sessions_per_second": round(session_count / end_to_end_seconds, 4)
            if session_count is not None and end_to_end_seconds
            else None,
        }
    normalized["timing"] = timing

    return normalized


def load_parquet_frame(
    path: str | Path | None,
    *,
    columns: Sequence[str] | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()

    resolved = Path(path)
    if not resolved.exists():
        return pd.DataFrame()

    if resolved.is_dir():
        parquet_files = sorted(resolved.rglob("*.parquet"))
        if not parquet_files:
            return pd.DataFrame()
        frames: list[pd.DataFrame] = []
        remaining = limit
        for file_path in parquet_files:
            frame = pq.read_table(str(file_path), columns=list(columns) if columns else None).to_pandas()
            if remaining is not None:
                frame = frame.head(remaining)
                remaining -= len(frame)
            frames.append(frame)
            if remaining is not None and remaining <= 0:
                break
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    else:
        frame = pq.read_table(str(resolved), columns=list(columns) if columns else None).to_pandas()
        if limit is not None:
            return frame.head(limit)
        return frame


def resolve_batch_dataset_path(dataset_name: str) -> Path | None:
    for root in BATCH_DATASET_ROOTS:
        candidate = root / dataset_name
        if candidate.exists():
            return candidate
    return None


def select_batch_preview_result(report: dict[str, Any] | None) -> dict[str, Any] | None:
    if not report:
        return None

    results = report.get("results", [])
    for engine in ("pyspark", "pandas"):
        for result in results:
            if result.get("engine") == engine and result.get("row_limit_label") == "full":
                return result

    return results[-1] if results else None


def build_batch_preview_tables(report: dict[str, Any] | None) -> dict[str, Any]:
    selected = select_batch_preview_result(report)
    outputs = (selected or {}).get("outputs", {})

    summary = outputs.get("summary", {})
    summary_df = pd.DataFrame([summary]) if summary else pd.DataFrame()

    return {
        "paths": {
            "summary": "benchmark preview",
            "hourly": "benchmark preview",
            "borough_summary": "benchmark preview",
            "zone_summary": "benchmark preview",
            "payment_tip_summary": "benchmark preview",
        },
        "summary": summary_df,
        "hourly": pd.DataFrame(outputs.get("hourly_preview", [])),
        "borough_summary": pd.DataFrame(outputs.get("borough_preview", [])),
        "zone_summary": pd.DataFrame(outputs.get("zone_preview", [])),
        "payment_tip_summary": pd.DataFrame(outputs.get("payment_tip_preview", [])),
    }


def load_quality_tables() -> dict[str, Any]:
    quality_summary_path = resolve_batch_dataset_path("quality_summary")
    quality_issue_path = resolve_batch_dataset_path("quality_issue_rows")
    return {
        "paths": {
            "quality_summary": str(quality_summary_path) if quality_summary_path else None,
            "quality_issue_rows": str(quality_issue_path) if quality_issue_path else None,
        },
        "quality_summary": load_parquet_frame(quality_summary_path),
        "quality_issue_rows": load_parquet_frame(quality_issue_path, limit=20),
    }


def load_pandas_vs_spark_report() -> tuple[dict[str, Any] | None, Path | None]:
    report_path = first_existing_path(PANDAS_SPARK_REPORT_CANDIDATES)
    return load_json_file(report_path), report_path


def load_streaming_demo_report() -> tuple[dict[str, Any] | None, Path | None]:
    report_path = find_latest_path("docs/reports", STREAMING_REPORT_GLOB)
    return normalize_streaming_report(load_json_file(report_path)), report_path


def load_partitioning_effect_report() -> tuple[dict[str, Any] | None, Path | None]:
    report_path = first_existing_path(PARTITIONING_REPORT_CANDIDATES)
    return load_json_file(report_path), report_path


def load_batch_vs_streaming_report(
    pandas_vs_spark_report: dict[str, Any] | None,
    streaming_demo_report: dict[str, Any] | None,
    *,
    pandas_vs_spark_path: Path | None,
    streaming_demo_path: Path | None,
) -> tuple[dict[str, Any] | None, Path | None]:
    report_path = first_existing_path(BATCH_STREAMING_REPORT_CANDIDATES)
    report = load_json_file(report_path)
    if report is not None:
        return report, report_path

    if pandas_vs_spark_report is None or streaming_demo_report is None:
        return None, None

    generated = build_batch_vs_streaming_report(
        pandas_vs_spark_report,
        streaming_demo_report,
        benchmark_report_path=str(pandas_vs_spark_path or "data/benchmarks/pandas_vs_spark_jan2023.json"),
        streaming_report_path=str(streaming_demo_path or "docs/reports/streaming_demo_result_*.json"),
    )
    return generated, None


def load_dashboard_snapshot() -> dict[str, Any]:
    pandas_vs_spark_report, pandas_vs_spark_path = load_pandas_vs_spark_report()
    streaming_demo_report, streaming_demo_path = load_streaming_demo_report()
    partitioning_report, partitioning_report_path = load_partitioning_effect_report()
    batch_tables = build_batch_preview_tables(pandas_vs_spark_report)
    quality_tables = load_quality_tables()
    batch_tables["paths"].update(quality_tables["paths"])
    batch_tables["quality_summary"] = quality_tables["quality_summary"]
    batch_tables["quality_issue_rows"] = quality_tables["quality_issue_rows"]
    batch_vs_streaming_report, batch_vs_streaming_path = load_batch_vs_streaming_report(
        pandas_vs_spark_report,
        streaming_demo_report,
        pandas_vs_spark_path=pandas_vs_spark_path,
        streaming_demo_path=streaming_demo_path,
    )

    return {
        "batch_tables": batch_tables,
        "pandas_vs_spark_report": pandas_vs_spark_report,
        "pandas_vs_spark_path": str(pandas_vs_spark_path) if pandas_vs_spark_path else None,
        "streaming_demo_report": streaming_demo_report,
        "streaming_demo_path": str(streaming_demo_path) if streaming_demo_path else None,
        "batch_vs_streaming_report": batch_vs_streaming_report,
        "batch_vs_streaming_path": str(batch_vs_streaming_path) if batch_vs_streaming_path else None,
        "partitioning_report": partitioning_report,
        "partitioning_report_path": str(partitioning_report_path) if partitioning_report_path else None,
    }
