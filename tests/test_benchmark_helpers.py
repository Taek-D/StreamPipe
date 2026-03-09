from src.benchmarks.batch_vs_streaming import (
    build_batch_profiles,
    build_comparison_rows as build_batch_vs_streaming_rows,
    build_markdown_report as build_batch_vs_streaming_markdown,
    build_report as build_batch_vs_streaming_report,
    build_streaming_profile,
)
from src.benchmarks.pandas_vs_spark import build_comparison_rows, build_markdown_report, parse_row_limits


def test_parse_row_limits_supports_integers_and_full() -> None:
    assert parse_row_limits(["100000", "500000", "full"]) == [100000, 500000, None]


def test_build_comparison_rows_pairs_pandas_and_pyspark() -> None:
    rows = build_comparison_rows(
        [
            {"engine": "pandas", "row_limit_label": "100,000", "duration_seconds": 1.5},
            {"engine": "pyspark", "row_limit_label": "100,000", "duration_seconds": 4.5},
            {"engine": "pandas", "row_limit_label": "full", "duration_seconds": 10.0},
        ]
    )

    assert rows == [
        {
            "row_limit": "100,000",
            "pandas_seconds": 1.5,
            "pyspark_seconds": 4.5,
            "spark_vs_pandas_ratio": 3.0,
        },
        {
            "row_limit": "full",
            "pandas_seconds": 10.0,
            "pyspark_seconds": None,
        },
    ]


def test_build_markdown_report_contains_comparison_table() -> None:
    report = {
        "generated_at_utc": "2026-03-08T00:00:00+00:00",
        "input_path": "data/raw/nyc_taxi/yellow/2023",
        "zone_lookup_path": "data/raw/nyc_taxi/reference/taxi_zone_lookup.csv",
        "pickup_year": 2023,
        "spark_startup_seconds": 2.5,
        "comparison_rows": [
            {
                "row_limit": "100,000",
                "pandas_seconds": 1.5,
                "pyspark_seconds": 4.5,
                "spark_vs_pandas_ratio": 3.0,
            }
        ],
        "results": [
            {
                "engine": "pandas",
                "row_limit_label": "100,000",
                "duration_seconds": 1.5,
                "outputs": {"summary": {"row_count": 100000}},
            }
        ],
    }

    markdown = build_markdown_report(report)

    assert "# pandas vs PySpark Benchmark" in markdown
    assert "| Row limit | pandas (s) | PySpark (s) | Spark / pandas |" in markdown
    assert "### pandas / 100,000" in markdown


def test_build_batch_profiles_adds_rows_per_second_and_cold_start() -> None:
    report = {
        "spark_startup_seconds": 9.127,
        "results": [
            {
                "engine": "pandas",
                "row_limit_label": "full",
                "duration_seconds": 5.5,
                "outputs": {"summary": {"row_count": 1100}},
            },
            {
                "engine": "pyspark",
                "row_limit_label": "full",
                "duration_seconds": 6.0,
                "outputs": {"summary": {"row_count": 1200}},
            },
        ],
    }

    profiles = build_batch_profiles(report)

    assert profiles == [
        {
            "engine": "pandas",
            "row_limit_label": "full",
            "row_count": 1100,
            "duration_seconds": 5.5,
            "startup_seconds": None,
            "time_to_first_result_seconds": 5.5,
            "rows_per_second": 200.0,
        },
        {
            "engine": "pyspark",
            "row_limit_label": "full",
            "row_count": 1200,
            "duration_seconds": 6.0,
            "startup_seconds": 9.127,
            "time_to_first_result_seconds": 15.127,
            "rows_per_second": 200.0,
        },
    ]


def test_build_streaming_profile_prefers_timing_summary_and_progress_rates() -> None:
    streaming_report = {
        "config": {"query_name": "clickstream_demo", "page_view_sessions": 10},
        "scenario_summary": {"generated_event_count": 27, "generated_session_count": 10},
        "timing": {
            "publish_seconds": 0.2,
            "processing_seconds": 1.8,
            "end_to_end_seconds": 2.0,
            "events_per_second": 13.5,
            "sessions_per_second": 5.0,
        },
        "query_progress": [
            {"processed_rows_per_second": 5.0},
            {"processed_rows_per_second": 12.5},
        ],
        "anomaly_alerts": [{"alert_count": 3, "severity": "high"}],
    }

    profile = build_streaming_profile(streaming_report)

    assert profile == {
        "query_name": "clickstream_demo",
        "generated_event_count": 27,
        "generated_session_count": 10,
        "publish_seconds": 0.2,
        "processing_seconds": 1.8,
        "end_to_end_seconds": 2.0,
        "events_per_second": 13.5,
        "sessions_per_second": 5.0,
        "observed_processed_rows_per_second": 12.5,
        "alert_count": 3,
        "severity": "high",
    }


def test_build_batch_vs_streaming_report_contains_snapshot_table() -> None:
    benchmark_report = {
        "spark_startup_seconds": 2.5,
        "results": [
            {
                "engine": "pandas",
                "row_limit_label": "full",
                "duration_seconds": 4.0,
                "outputs": {"summary": {"row_count": 1000}},
            }
        ],
    }
    streaming_report = {
        "config": {"query_name": "clickstream_demo", "page_view_sessions": 10},
        "scenario_summary": {"generated_event_count": 25, "generated_session_count": 10},
        "timing": {"end_to_end_seconds": 2.5, "events_per_second": 10.0, "sessions_per_second": 4.0},
        "query_progress": [],
        "anomaly_alerts": [{"alert_count": 2, "severity": "medium"}],
    }

    report = build_batch_vs_streaming_report(
        benchmark_report,
        streaming_report,
        benchmark_report_path="data/benchmarks/pandas_vs_spark_jan2023.json",
        streaming_report_path="docs/reports/streaming_demo_result_20260308.json",
    )

    markdown = build_batch_vs_streaming_markdown(report)
    comparison_rows = build_batch_vs_streaming_rows(report["batch_profiles"], report["streaming_profile"])

    assert comparison_rows == [
        {
            "pipeline": "batch",
            "variant": "pandas / full",
            "workload": "bounded NYC Taxi aggregation",
            "latency_seconds": 4.0,
            "throughput": 250.0,
            "throughput_unit": "rows/s",
            "record_count": 1000,
        },
        {
            "pipeline": "streaming",
            "variant": "clickstream_demo",
            "workload": "clickstream event burst",
            "latency_seconds": 2.5,
            "throughput": 10.0,
            "throughput_unit": "events/s",
            "record_count": 25,
        },
    ]
    assert "# Batch vs Streaming Benchmark" in markdown
    assert "| Pipeline | Variant | Workload | Latency (s) | Throughput | Unit | Record count |" in markdown
