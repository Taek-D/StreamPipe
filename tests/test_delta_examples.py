from src.batch.delta_examples import (
    build_delta_demo_evolved_rows,
    build_delta_demo_report,
    build_delta_demo_seed_rows,
    build_markdown_report,
    build_migration_report,
    summarize_schema_evolution,
)


def test_build_delta_demo_seed_and_evolved_rows_have_expected_shape() -> None:
    seed_rows = build_delta_demo_seed_rows()
    evolved_rows = build_delta_demo_evolved_rows()

    assert len(seed_rows) == 3
    assert len(evolved_rows) == 2
    assert "channel" not in seed_rows[0]
    assert evolved_rows[0]["channel"] == "ads"


def test_summarize_schema_evolution_detects_added_columns() -> None:
    before_schema = [
        {"name": "order_id", "type": "string", "nullable": True},
        {"name": "amount", "type": "double", "nullable": True},
    ]
    after_schema = [
        {"name": "order_id", "type": "string", "nullable": True},
        {"name": "amount", "type": "double", "nullable": True},
        {"name": "channel", "type": "string", "nullable": True},
    ]

    assert summarize_schema_evolution(before_schema, after_schema) == {
        "added_columns": ["channel"],
        "removed_columns": [],
    }


def test_build_migration_report_contains_core_metadata() -> None:
    report = build_migration_report(
        source_path="data/processed/batch_taxi_metrics_jan/summary",
        target_path="data/processed/delta/day4/batch_summary_delta_example",
        input_rows=1,
        output_rows=1,
        latest_version=0,
        partition_by=[],
        schema_rows=[{"name": "row_count", "type": "bigint", "nullable": True}],
        preview_rows=[{"row_count": 10}],
        history_rows=[{"version": 0, "operation": "WRITE", "timestamp": "2026-03-09T00:00:00+00:00"}],
    )

    markdown = build_markdown_report(report)

    assert report["report_type"] == "delta_migration"
    assert report["latest_version"] == 0
    assert "# Delta Migration Example" in markdown
    assert "| Version | Operation | Timestamp |" in markdown


def test_build_delta_demo_report_contains_time_travel_summary() -> None:
    version_zero = {
        "version": 0,
        "row_count": 3,
        "schema": [{"name": "order_id", "type": "string", "nullable": True}],
        "preview_rows": [{"order_id": "ord-001"}],
    }
    latest = {
        "version": 1,
        "row_count": 5,
        "schema": [
            {"name": "order_id", "type": "string", "nullable": True},
            {"name": "channel", "type": "string", "nullable": True},
        ],
        "preview_rows": [{"order_id": "ord-005", "channel": "email"}],
    }
    report = build_delta_demo_report(
        target_path="data/processed/delta/day4/time_travel_orders_demo",
        version_zero_summary=version_zero,
        latest_summary=latest,
        history_rows=[
            {"version": 0, "operation": "WRITE", "timestamp": "2026-03-09T00:00:00+00:00"},
            {"version": 1, "operation": "WRITE", "timestamp": "2026-03-09T00:01:00+00:00"},
        ],
        schema_changes={"added_columns": ["channel"], "removed_columns": []},
        optimization_summary={
            "supported": True,
            "executed": True,
            "skipped": False,
            "zorder_columns": ["event_date", "channel"],
            "compaction_metrics": [{"numFilesAdded": 1}],
            "zorder_metrics": [{"numFilesAdded": 1}],
        },
    )

    markdown = build_markdown_report(report)

    assert report["report_type"] == "delta_demo"
    assert report["time_travel_example"] == {
        "version_as_of": 0,
        "latest_version": 1,
        "version_zero_row_count": 3,
        "latest_row_count": 5,
    }
    assert "# Delta Time Travel And Schema Evolution Demo" in markdown
    assert "Added columns" in markdown
    assert "## OPTIMIZE and ZORDER" in markdown
