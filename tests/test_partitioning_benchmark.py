from src.benchmarks.partitioning_effect import (
    build_improvement_summary,
    build_markdown_report,
    build_partitioning_demo_rows,
    build_report,
)


def test_build_partitioning_demo_rows_generates_expected_volume() -> None:
    rows = build_partitioning_demo_rows(days=3, rows_per_day=8)

    assert len(rows) == 24
    assert rows[0]["pickup_date"] == "2026-01-01"
    assert rows[-1]["pickup_date"] == "2026-01-03"
    assert {"record_id", "pickup_date", "pickup_borough", "pickup_zone", "trip_distance", "total_amount"} == set(rows[0])


def test_build_improvement_summary_compares_against_unpartitioned() -> None:
    summary = build_improvement_summary(
        [
            {
                "layout": "unpartitioned",
                "partition_columns": [],
                "avg_seconds": 1.0,
                "matched_rows": 200,
            },
            {
                "layout": "partitioned_by_date",
                "partition_columns": ["pickup_date"],
                "avg_seconds": 0.7,
                "matched_rows": 200,
            },
        ]
    )

    assert summary == [
        {
            "layout": "unpartitioned",
            "partition_columns": [],
            "avg_seconds": 1.0,
            "matched_rows": 200,
            "improvement_vs_unpartitioned_pct": None,
        },
        {
            "layout": "partitioned_by_date",
            "partition_columns": ["pickup_date"],
            "avg_seconds": 0.7,
            "matched_rows": 200,
            "improvement_vs_unpartitioned_pct": 30.0,
        },
    ]


def test_build_report_and_markdown_include_layout_rows() -> None:
    report = build_report(
        output_root="data/benchmarks/partitioning_effect_demo_data",
        dataset_row_count=16800,
        query_filter={"pickup_date": "2026-01-21", "pickup_borough": "Queens"},
        layouts=[
            {
                "layout": "unpartitioned",
                "path": "data/benchmarks/partitioning_effect_demo_data/unpartitioned",
                "partition_columns": [],
                "avg_seconds": 1.0,
                "min_seconds": 0.9,
                "max_seconds": 1.1,
                "matched_rows": 200,
                "result_preview": [{"pickup_date": "2026-01-21", "pickup_borough": "Queens", "matched_rows": 200}],
            }
        ],
    )

    markdown = build_markdown_report(report)

    assert report["comparison_rows"][0]["layout"] == "unpartitioned"
    assert "# Partitioning Effect Benchmark" in markdown
    assert "| Layout | Partition columns | Avg seconds | Improvement vs unpartitioned (%) | Matched rows |" in markdown
