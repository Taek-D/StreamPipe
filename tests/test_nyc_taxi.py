from pathlib import Path

import pandas as pd

from src.data.nyc_taxi import (
    build_tripdata_url,
    inspect_parquet_dataset,
    parse_months,
    write_report_files,
)


def test_build_tripdata_url_matches_official_pattern() -> None:
    assert (
        build_tripdata_url("yellow", 2023, 1)
        == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    )


def test_parse_months_supports_single_values_commas_and_ranges() -> None:
    assert parse_months(["1", "3-4", "7,9"]) == [1, 3, 4, 7, 9]


def test_inspect_parquet_dataset_detects_yellow_schema_and_recommendations(tmp_path: Path) -> None:
    parquet_path = tmp_path / "yellow_tripdata_2023-01.parquet"
    dataframe = pd.DataFrame(
        {
            "tpep_pickup_datetime": ["2023-01-01 00:00:00", "2023-01-01 01:00:00"],
            "tpep_dropoff_datetime": ["2023-01-01 00:15:00", "2023-01-01 01:20:00"],
            "PULocationID": [161, 236],
            "DOLocationID": [236, 161],
            "passenger_count": [1, 2],
            "trip_distance": [1.2, 3.4],
            "payment_type": [1, 2],
            "fare_amount": [10.5, 20.0],
            "tip_amount": [2.5, 0.0],
            "total_amount": [15.0, 21.3],
        }
    )
    dataframe.to_parquet(parquet_path, index=False)

    report = inspect_parquet_dataset(tmp_path)
    recommended = {item["role"]: item["column"] for item in report["recommended_columns"]}

    assert report["dataset_variant"] == "yellow"
    assert report["pickup_timestamp_column"] == "tpep_pickup_datetime"
    assert report["file_count"] == 1
    assert report["total_rows"] == 2
    assert recommended["pickup_zone"] == "PULocationID"
    assert recommended["tip_amount"] == "tip_amount"


def test_write_report_files_creates_json_and_markdown(tmp_path: Path) -> None:
    report = {
        "generated_at_utc": "2026-03-08T00:00:00+00:00",
        "input_path": "data/raw/nyc_taxi/yellow/2023",
        "dataset_variant": "yellow",
        "pickup_timestamp_column": "tpep_pickup_datetime",
        "file_count": 1,
        "total_rows": 2,
        "total_size_bytes": 128,
        "total_size_mb": 0.0,
        "schema": [{"name": "tpep_pickup_datetime", "type": "string"}],
        "recommended_columns": [
            {
                "role": "pickup_timestamp",
                "column": "tpep_pickup_datetime",
                "reason": "시간대별 승차 패턴 분석 기준 시각",
            }
        ],
        "files": [{"path": "data/raw/nyc_taxi/yellow/2023/yellow_tripdata_2023-01.parquet", "rows": 2, "size_bytes": 128}],
    }

    json_path, markdown_path = write_report_files(report, tmp_path)

    assert json_path.exists()
    assert markdown_path.exists()
    assert '"dataset_variant": "yellow"' in json_path.read_text(encoding="utf-8")
    assert "# NYC Taxi Schema Report" in markdown_path.read_text(encoding="utf-8")
