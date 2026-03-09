from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence
from urllib.request import Request, urlopen

TRIPDATA_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEFAULT_DATASET = "yellow"
DEFAULT_YEAR = 2023
DEFAULT_OUTPUT_DIR = Path("data/raw/nyc_taxi")
DEFAULT_REPORT_DIR = Path("docs/reports/nyc_taxi")
DATASET_CHOICES = ("yellow", "green", "fhv", "fhvhv")
PICKUP_TIMESTAMP_CANDIDATES = (
    "tpep_pickup_datetime",
    "lpep_pickup_datetime",
    "pickup_datetime",
)


@dataclass(frozen=True, slots=True)
class ColumnRecommendation:
    role: str
    column: str
    reason: str


def build_tripdata_filename(dataset: str, year: int, month: int) -> str:
    validate_dataset(dataset)
    validate_month(month)
    return f"{dataset}_tripdata_{year}-{month:02d}.parquet"


def build_tripdata_url(dataset: str, year: int, month: int) -> str:
    return f"{TRIPDATA_BASE_URL}/{build_tripdata_filename(dataset, year, month)}"


def build_output_file_path(base_dir: Path, dataset: str, year: int, month: int) -> Path:
    filename = build_tripdata_filename(dataset, year, month)
    return base_dir / dataset / str(year) / filename


def build_zone_lookup_path(base_dir: Path) -> Path:
    return base_dir / "reference" / "taxi_zone_lookup.csv"


def validate_dataset(dataset: str) -> None:
    if dataset not in DATASET_CHOICES:
        supported = ", ".join(DATASET_CHOICES)
        raise ValueError(f"Unsupported dataset: {dataset}. Expected one of: {supported}")


def validate_month(month: int) -> None:
    if not 1 <= month <= 12:
        raise ValueError(f"Month must be between 1 and 12, got: {month}")


def parse_months(values: Sequence[str] | None) -> list[int]:
    if not values:
        return list(range(1, 13))

    months: set[int] = set()
    for raw in values:
        for token in raw.split(","):
            token = token.strip()
            if not token:
                continue

            if "-" in token:
                start_str, end_str = token.split("-", maxsplit=1)
                start = int(start_str)
                end = int(end_str)
                if start > end:
                    raise ValueError(f"Month range must be ascending, got: {token}")
                for month in range(start, end + 1):
                    validate_month(month)
                    months.add(month)
            else:
                month = int(token)
                validate_month(month)
                months.add(month)

    return sorted(months)


def download_file(
    url: str,
    destination: Path,
    *,
    overwrite: bool = False,
    chunk_size: int = 1024 * 1024,
) -> Path:
    if destination.exists() and not overwrite:
        return destination

    destination.parent.mkdir(parents=True, exist_ok=True)
    request = Request(url, headers={"User-Agent": "StreamPipe/1.0"})

    with urlopen(request) as response, destination.open("wb") as file:
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            file.write(chunk)

    return destination


def download_tripdata_files(
    dataset: str,
    year: int,
    months: Sequence[int],
    output_dir: Path,
    *,
    overwrite: bool = False,
) -> list[Path]:
    validate_dataset(dataset)
    downloaded_files: list[Path] = []

    for month in months:
        validate_month(month)
        destination = build_output_file_path(output_dir, dataset, year, month)
        url = build_tripdata_url(dataset, year, month)
        downloaded_files.append(download_file(url, destination, overwrite=overwrite))

    return downloaded_files


def detect_dataset_variant(columns: Sequence[str]) -> str:
    column_set = set(columns)
    if "tpep_pickup_datetime" in column_set:
        return "yellow"
    if "lpep_pickup_datetime" in column_set:
        return "green"
    if "hvfhs_license_num" in column_set:
        return "fhvhv"
    if "dispatching_base_num" in column_set:
        return "fhv"
    return "unknown"


def resolve_pickup_timestamp_column(columns: Sequence[str]) -> str | None:
    for candidate in PICKUP_TIMESTAMP_CANDIDATES:
        if candidate in columns:
            return candidate
    return None


def recommend_analysis_columns(columns: Sequence[str]) -> list[ColumnRecommendation]:
    available = set(columns)
    pickup_column = resolve_pickup_timestamp_column(columns)
    candidates = [
        ColumnRecommendation("pickup_timestamp", pickup_column or "", "시간대별 승차 패턴 분석 기준 시각"),
        ColumnRecommendation(
            "dropoff_timestamp",
            next((column for column in ("tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropOff_datetime") if column in available), ""),
            "이동 시간 및 체류 시간 계산",
        ),
        ColumnRecommendation(
            "pickup_zone",
            next((column for column in ("PULocationID", "PUlocationID") if column in available), ""),
            "지역별 수요/매출 집계",
        ),
        ColumnRecommendation(
            "dropoff_zone",
            next((column for column in ("DOLocationID", "DOlocationID") if column in available), ""),
            "도착 지역 분석 및 이동 패턴 파악",
        ),
        ColumnRecommendation(
            "passenger_count",
            "passenger_count" if "passenger_count" in available else "",
            "수요 강도와 탑승 인원 분석",
        ),
        ColumnRecommendation(
            "trip_distance",
            "trip_distance" if "trip_distance" in available else "",
            "거리별 수익/수요 패턴 분석",
        ),
        ColumnRecommendation(
            "payment_type",
            "payment_type" if "payment_type" in available else "",
            "결제 방식별 팁/매출 비교",
        ),
        ColumnRecommendation(
            "fare_amount",
            "fare_amount" if "fare_amount" in available else "",
            "기본 운임 분석",
        ),
        ColumnRecommendation(
            "tip_amount",
            "tip_amount" if "tip_amount" in available else "",
            "팁 비율 분석",
        ),
        ColumnRecommendation(
            "total_amount",
            "total_amount" if "total_amount" in available else "",
            "총 매출 집계",
        ),
    ]
    return [candidate for candidate in candidates if candidate.column]


def iter_parquet_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        if input_path.suffix != ".parquet":
            raise ValueError(f"Expected a parquet file, got: {input_path}")
        return [input_path]

    files = sorted(path for path in input_path.rglob("*.parquet") if path.is_file())
    if files:
        return files
    raise FileNotFoundError(f"No parquet files found under: {input_path}")


def inspect_parquet_dataset(input_path: Path) -> dict:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required to inspect parquet datasets.") from exc

    files = iter_parquet_files(input_path)
    schema = None
    file_reports: list[dict] = []
    total_rows = 0
    total_size_bytes = 0

    for path in files:
        parquet_file = pq.ParquetFile(path)
        row_count = parquet_file.metadata.num_rows
        size_bytes = path.stat().st_size
        total_rows += row_count
        total_size_bytes += size_bytes
        if schema is None:
            schema = parquet_file.schema_arrow

        file_reports.append(
            {
                "path": str(path),
                "rows": row_count,
                "size_bytes": size_bytes,
            }
        )

    if schema is None:
        raise RuntimeError(f"Could not infer schema from: {input_path}")

    schema_fields = [{"name": field.name, "type": str(field.type)} for field in schema]
    columns = [field["name"] for field in schema_fields]
    recommendations = recommend_analysis_columns(columns)

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path),
        "dataset_variant": detect_dataset_variant(columns),
        "pickup_timestamp_column": resolve_pickup_timestamp_column(columns),
        "file_count": len(files),
        "total_rows": total_rows,
        "total_size_bytes": total_size_bytes,
        "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
        "schema": schema_fields,
        "recommended_columns": [asdict(item) for item in recommendations],
        "files": file_reports,
    }


def build_schema_markdown(report: dict) -> str:
    lines = [
        "# NYC Taxi Schema Report",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Input path: `{report['input_path']}`",
        f"- Dataset variant: `{report['dataset_variant']}`",
        f"- Pickup timestamp column: `{report['pickup_timestamp_column']}`",
        f"- File count: `{report['file_count']}`",
        f"- Total rows: `{report['total_rows']}`",
        f"- Total size (MB): `{report['total_size_mb']}`",
        "",
        "## Recommended analysis columns",
        "",
        "| Role | Column | Why |",
        "|---|---|---|",
    ]

    for item in report["recommended_columns"]:
        lines.append(f"| {item['role']} | `{item['column']}` | {item['reason']} |")

    lines.extend(
        [
            "",
            "## Schema",
            "",
            "| Column | Type |",
            "|---|---|",
        ]
    )

    for field in report["schema"]:
        lines.append(f"| `{field['name']}` | `{field['type']}` |")

    lines.extend(
        [
            "",
            "## Files",
            "",
            "| File | Rows | Size (MB) |",
            "|---|---:|---:|",
        ]
    )

    for file_report in report["files"]:
        size_mb = round(file_report["size_bytes"] / (1024 * 1024), 2)
        lines.append(f"| `{file_report['path']}` | {file_report['rows']} | {size_mb} |")

    return "\n".join(lines) + "\n"


def write_report_files(report: dict, report_dir: Path) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / "schema_report.json"
    markdown_path = report_dir / "schema_report.md"

    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    markdown_path.write_text(build_schema_markdown(report), encoding="utf-8")
    return json_path, markdown_path


def add_download_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dataset", default=DEFAULT_DATASET, choices=DATASET_CHOICES)
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR)
    parser.add_argument(
        "--months",
        nargs="*",
        default=None,
        help="Months to download, e.g. 1 2 3 or 1-3 7,9",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Base directory for raw parquet files")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument(
        "--download-zone-lookup",
        action="store_true",
        help="Also download taxi_zone_lookup.csv",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NYC Taxi data download and schema inspection helpers.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    download_parser = subparsers.add_parser("download", help="Download official NYC Taxi parquet files")
    add_download_arguments(download_parser)

    inspect_parser = subparsers.add_parser("inspect", help="Inspect local parquet files and write schema reports")
    inspect_parser.add_argument("--input-path", default=str(DEFAULT_OUTPUT_DIR))
    inspect_parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))

    prepare_parser = subparsers.add_parser(
        "prepare",
        help="Download parquet files and immediately generate schema reports",
    )
    add_download_arguments(prepare_parser)
    prepare_parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))

    return parser


def run_download(args: argparse.Namespace) -> list[Path]:
    output_dir = Path(args.output_dir)
    months = parse_months(args.months)
    downloaded_files = download_tripdata_files(
        args.dataset,
        args.year,
        months,
        output_dir,
        overwrite=args.overwrite,
    )

    print("=== NYC Taxi Download ===")
    print(f"dataset     : {args.dataset}")
    print(f"year        : {args.year}")
    print(f"months      : {months}")
    print(f"output_dir  : {output_dir}")
    for file in downloaded_files:
        print(f"saved       : {file}")

    if args.download_zone_lookup:
        zone_lookup_path = download_file(
            ZONE_LOOKUP_URL,
            build_zone_lookup_path(output_dir),
            overwrite=args.overwrite,
        )
        print(f"zone_lookup : {zone_lookup_path}")

    return downloaded_files


def run_inspect(input_path: Path, report_dir: Path) -> dict:
    report = inspect_parquet_dataset(input_path)
    json_path, markdown_path = write_report_files(report, report_dir)

    print("=== NYC Taxi Schema Inspection ===")
    print(f"input_path   : {input_path}")
    print(f"dataset_type : {report['dataset_variant']}")
    print(f"file_count   : {report['file_count']}")
    print(f"total_rows   : {report['total_rows']}")
    print(f"pickup_ts    : {report['pickup_timestamp_column']}")
    print(f"json_report  : {json_path}")
    print(f"markdown     : {markdown_path}")

    if report["recommended_columns"]:
        print("recommended  :")
        for item in report["recommended_columns"]:
            print(f"  - {item['role']}: {item['column']}")

    return report


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "download":
        run_download(args)
        return

    if args.command == "inspect":
        run_inspect(Path(args.input_path), Path(args.report_dir))
        return

    if args.command == "prepare":
        run_download(args)
        inspect_input = Path(args.output_dir) / args.dataset / str(args.year)
        run_inspect(inspect_input, Path(args.report_dir))
        return

    raise SystemExit(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
