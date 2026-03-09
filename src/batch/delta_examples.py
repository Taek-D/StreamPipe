from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from src.common.spark import build_spark_session

DEFAULT_PARQUET_SOURCE_PATH = Path("data/processed/batch_taxi_metrics_jan/summary")
DEFAULT_MIGRATION_TARGET_PATH = Path("data/processed/delta/day4/batch_summary_delta_example")
DEFAULT_DEMO_TARGET_PATH = Path("data/processed/delta/day4/time_travel_orders_demo")
DEFAULT_REPORT_DIR = Path("docs/reports")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Delta Lake migration and demo utilities for StreamPipe Day 4.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    migrate_parser = subparsers.add_parser("migrate", help="Migrate a Parquet dataset to Delta format")
    migrate_parser.add_argument("--source-path", default=str(DEFAULT_PARQUET_SOURCE_PATH), help="Source Parquet path")
    migrate_parser.add_argument("--target-path", default=str(DEFAULT_MIGRATION_TARGET_PATH), help="Target Delta path")
    migrate_parser.add_argument("--partition-by", nargs="*", default=[], help="Optional partition columns")
    migrate_parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Write mode")
    migrate_parser.add_argument(
        "--report-path",
        default=str(DEFAULT_REPORT_DIR / "delta_migration_example"),
        help="Output report path prefix without extension",
    )
    migrate_parser.add_argument("--master", default=None, help="Optional Spark master URL")
    migrate_parser.add_argument("--app-name", default="streampipe-delta-migrate", help="Spark application name")

    demo_parser = subparsers.add_parser("demo", help="Run a deterministic Delta time-travel and schema-evolution demo")
    demo_parser.add_argument("--target-path", default=str(DEFAULT_DEMO_TARGET_PATH), help="Target Delta path for demo table")
    demo_parser.add_argument(
        "--report-path",
        default=str(DEFAULT_REPORT_DIR / "delta_time_travel_demo"),
        help="Output report path prefix without extension",
    )
    demo_parser.add_argument(
        "--zorder-columns",
        nargs="*",
        default=["event_date", "channel"],
        help="Columns to use for the OPTIMIZE ZORDER BY demo step",
    )
    demo_parser.add_argument("--skip-optimize", action="store_true", help="Skip OPTIMIZE / ZORDER demo steps")
    demo_parser.add_argument("--master", default=None, help="Optional Spark master URL")
    demo_parser.add_argument("--app-name", default="streampipe-delta-demo", help="Spark application name")

    return parser


def create_delta_spark_session(app_name: str, master: str | None = None):
    return build_spark_session(
        app_name=app_name,
        master_url=master,
        with_delta=True,
        extra_configs={
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "2",
        },
    )


def clear_directory(path: str | Path) -> None:
    resolved = Path(path)
    if resolved.exists():
        shutil.rmtree(resolved)
    resolved.parent.mkdir(parents=True, exist_ok=True)


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def schema_to_rows(schema: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field in schema.fields:
        rows.append(
            {
                "name": field.name,
                "type": field.dataType.simpleString(),
                "nullable": field.nullable,
            }
        )
    return rows


def dataframe_preview(dataframe: Any, rows: int = 10) -> list[dict[str, Any]]:
    return [{key: serialize_value(value) for key, value in row.asDict(recursive=True).items()} for row in dataframe.limit(rows).collect()]


def collect_history_rows(delta_table: Any, limit: int = 20) -> list[dict[str, Any]]:
    history_df = delta_table.history(limit).orderBy("version")
    return [{key: serialize_value(value) for key, value in row.asDict(recursive=True).items()} for row in history_df.collect()]


def summarize_schema_evolution(
    before_schema: Sequence[dict[str, Any]],
    after_schema: Sequence[dict[str, Any]],
) -> dict[str, list[str]]:
    before_names = [field["name"] for field in before_schema]
    after_names = [field["name"] for field in after_schema]
    before_set = set(before_names)
    after_set = set(after_names)

    return {
        "added_columns": [name for name in after_names if name not in before_set],
        "removed_columns": [name for name in before_names if name not in after_set],
    }


def build_migration_report(
    *,
    source_path: str,
    target_path: str,
    input_rows: int,
    output_rows: int,
    latest_version: int | None,
    partition_by: Sequence[str],
    schema_rows: Sequence[dict[str, Any]],
    preview_rows: Sequence[dict[str, Any]],
    history_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "report_type": "delta_migration",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_path": source_path,
        "target_path": target_path,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "latest_version": latest_version,
        "partition_by": list(partition_by),
        "schema": list(schema_rows),
        "preview_rows": list(preview_rows),
        "history": list(history_rows),
    }


def build_delta_demo_seed_rows() -> list[dict[str, Any]]:
    return [
        {"order_id": "ord-001", "user_id": "user-01", "amount": 24.5, "event_date": "2026-03-09"},
        {"order_id": "ord-002", "user_id": "user-02", "amount": 42.0, "event_date": "2026-03-09"},
        {"order_id": "ord-003", "user_id": "user-03", "amount": 15.0, "event_date": "2026-03-09"},
    ]


def build_delta_demo_evolved_rows() -> list[dict[str, Any]]:
    return [
        {"order_id": "ord-004", "user_id": "user-04", "amount": 31.0, "event_date": "2026-03-10", "channel": "ads"},
        {"order_id": "ord-005", "user_id": "user-05", "amount": 27.5, "event_date": "2026-03-10", "channel": "email"},
    ]


def build_delta_demo_report(
    *,
    target_path: str,
    version_zero_summary: dict[str, Any],
    latest_summary: dict[str, Any],
    history_rows: Sequence[dict[str, Any]],
    schema_changes: dict[str, list[str]],
    optimization_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "report_type": "delta_demo",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "target_path": target_path,
        "version_zero": version_zero_summary,
        "latest": latest_summary,
        "history": list(history_rows),
        "schema_evolution": schema_changes,
        "optimization": optimization_summary,
        "time_travel_example": {
            "version_as_of": version_zero_summary["version"],
            "latest_version": latest_summary["version"],
            "version_zero_row_count": version_zero_summary["row_count"],
            "latest_row_count": latest_summary["row_count"],
        },
    }


def run_delta_optimization(
    delta_table: Any,
    *,
    zorder_columns: Sequence[str],
    enabled: bool,
) -> dict[str, Any]:
    if not enabled:
        return {
            "supported": True,
            "executed": False,
            "skipped": True,
            "zorder_columns": list(zorder_columns),
        }

    if not hasattr(delta_table, "optimize"):
        return {
            "supported": False,
            "executed": False,
            "skipped": False,
            "zorder_columns": list(zorder_columns),
            "error": "delta-spark optimize() API is unavailable in this environment",
        }

    try:
        optimizer = delta_table.optimize()
        compaction_metrics = dataframe_preview(optimizer.executeCompaction())
        zorder_metrics = []
        if zorder_columns:
            zorder_metrics = dataframe_preview(delta_table.optimize().executeZOrderBy(list(zorder_columns)))
        return {
            "supported": True,
            "executed": True,
            "skipped": False,
            "zorder_columns": list(zorder_columns),
            "compaction_metrics": compaction_metrics,
            "zorder_metrics": zorder_metrics,
        }
    except Exception as exc:  # pragma: no cover - depends on local Delta runtime support
        return {
            "supported": True,
            "executed": False,
            "skipped": False,
            "zorder_columns": list(zorder_columns),
            "error": str(exc),
        }


def build_markdown_report(report: dict[str, Any]) -> str:
    if report["report_type"] == "delta_migration":
        lines = [
            "# Delta Migration Example",
            "",
            f"- Generated at (UTC): {report['generated_at_utc']}",
            f"- Source path: `{report['source_path']}`",
            f"- Target path: `{report['target_path']}`",
            f"- Input rows: `{report['input_rows']}`",
            f"- Output rows: `{report['output_rows']}`",
            f"- Latest version: `{report['latest_version']}`",
            f"- Partition by: `{', '.join(report['partition_by']) or 'none'}`",
            "",
            "## History",
            "",
            "| Version | Operation | Timestamp |",
            "|---|---|---|",
        ]

        for row in report["history"]:
            lines.append(f"| {row.get('version')} | {row.get('operation')} | {row.get('timestamp')} |")

        lines.extend(["", "## Schema", "", "| Name | Type | Nullable |", "|---|---|---|"])
        for row in report["schema"]:
            lines.append(f"| {row['name']} | {row['type']} | {row['nullable']} |")

        return "\n".join(lines) + "\n"

    lines = [
        "# Delta Time Travel And Schema Evolution Demo",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Target path: `{report['target_path']}`",
        "",
        "## Version comparison",
        "",
        "| Version | Rows | Columns |",
        "|---|---:|---|",
        f"| {report['version_zero']['version']} | {report['version_zero']['row_count']} | {', '.join(field['name'] for field in report['version_zero']['schema'])} |",
        f"| {report['latest']['version']} | {report['latest']['row_count']} | {', '.join(field['name'] for field in report['latest']['schema'])} |",
        "",
        "## Schema evolution",
        "",
        f"- Added columns: `{', '.join(report['schema_evolution']['added_columns']) or 'none'}`",
        f"- Removed columns: `{', '.join(report['schema_evolution']['removed_columns']) or 'none'}`",
        "",
        "## History",
        "",
        "| Version | Operation | Timestamp |",
        "|---|---|---|",
    ]

    for row in report["history"]:
        lines.append(f"| {row.get('version')} | {row.get('operation')} | {row.get('timestamp')} |")

    optimization = report.get("optimization", {})
    lines.extend(
        [
            "",
            "## OPTIMIZE and ZORDER",
            "",
            f"- Supported: `{optimization.get('supported')}`",
            f"- Executed: `{optimization.get('executed')}`",
            f"- Skipped: `{optimization.get('skipped')}`",
            f"- ZORDER columns: `{', '.join(optimization.get('zorder_columns', [])) or 'none'}`",
        ]
    )
    if optimization.get("error"):
        lines.append(f"- Error: `{optimization['error']}`")

    return "\n".join(lines) + "\n"


def write_report_files(report: dict[str, Any], output_path: str | Path) -> tuple[Path, Path]:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    json_path = target.with_suffix(".json")
    markdown_path = target.with_suffix(".md")
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(build_markdown_report(report), encoding="utf-8")
    return json_path, markdown_path


def migrate_parquet_to_delta(
    *,
    source_path: str,
    target_path: str,
    report_path: str,
    partition_by: Sequence[str],
    mode: str,
    master: str | None,
    app_name: str,
) -> tuple[dict[str, Any], tuple[Path, Path]]:
    from delta.tables import DeltaTable

    spark = create_delta_spark_session(app_name=app_name, master=master)

    try:
        source_df = spark.read.parquet(source_path)
        input_rows = source_df.count()

        if mode == "overwrite":
            clear_directory(target_path)

        writer = source_df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)

        delta_df = spark.read.format("delta").load(target_path)
        delta_table = DeltaTable.forPath(spark, target_path)
        history_rows = collect_history_rows(delta_table)
        latest_version = history_rows[-1]["version"] if history_rows else None

        report = build_migration_report(
            source_path=source_path,
            target_path=target_path,
            input_rows=input_rows,
            output_rows=delta_df.count(),
            latest_version=latest_version,
            partition_by=partition_by,
            schema_rows=schema_to_rows(delta_df.schema),
            preview_rows=dataframe_preview(delta_df),
            history_rows=history_rows,
        )
        paths = write_report_files(report, report_path)
        return report, paths
    finally:
        spark.stop()


def run_delta_demo(
    *,
    target_path: str,
    report_path: str,
    zorder_columns: Sequence[str],
    skip_optimize: bool,
    master: str | None,
    app_name: str,
) -> tuple[dict[str, Any], tuple[Path, Path]]:
    from delta.tables import DeltaTable

    spark = create_delta_spark_session(app_name=app_name, master=master)

    try:
        clear_directory(target_path)

        seed_df = spark.createDataFrame(build_delta_demo_seed_rows())
        seed_df.write.format("delta").mode("overwrite").save(target_path)

        version_zero_df = spark.read.format("delta").option("versionAsOf", 0).load(target_path)
        version_zero_summary = {
            "version": 0,
            "row_count": version_zero_df.count(),
            "schema": schema_to_rows(version_zero_df.schema),
            "preview_rows": dataframe_preview(version_zero_df),
        }

        evolved_df = spark.createDataFrame(build_delta_demo_evolved_rows())
        evolved_df.write.format("delta").mode("append").option("mergeSchema", "true").save(target_path)

        delta_table = DeltaTable.forPath(spark, target_path)
        optimization_summary = run_delta_optimization(
            delta_table,
            zorder_columns=zorder_columns,
            enabled=not skip_optimize,
        )
        history_rows = collect_history_rows(delta_table)
        latest_version = history_rows[-1]["version"] if history_rows else 0
        latest_df = spark.read.format("delta").load(target_path)
        latest_summary = {
            "version": latest_version,
            "row_count": latest_df.count(),
            "schema": schema_to_rows(latest_df.schema),
            "preview_rows": dataframe_preview(latest_df),
        }

        report = build_delta_demo_report(
            target_path=target_path,
            version_zero_summary=version_zero_summary,
            latest_summary=latest_summary,
            history_rows=history_rows,
            schema_changes=summarize_schema_evolution(version_zero_summary["schema"], latest_summary["schema"]),
            optimization_summary=optimization_summary,
        )
        paths = write_report_files(report, report_path)
        return report, paths
    finally:
        spark.stop()


def main() -> None:
    args = build_parser().parse_args()

    if args.command == "migrate":
        _, (json_path, markdown_path) = migrate_parquet_to_delta(
            source_path=args.source_path,
            target_path=args.target_path,
            report_path=args.report_path,
            partition_by=args.partition_by,
            mode=args.mode,
            master=args.master,
            app_name=args.app_name,
        )
    else:
        _, (json_path, markdown_path) = run_delta_demo(
            target_path=args.target_path,
            report_path=args.report_path,
            zorder_columns=args.zorder_columns,
            skip_optimize=args.skip_optimize,
            master=args.master,
            app_name=args.app_name,
        )

    print(f"[delta] json report: {json_path}")
    print(f"[delta] markdown   : {markdown_path}")


if __name__ == "__main__":
    main()
