from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ProjectSettings:
    project_name: str = os.getenv('PROJECT_NAME', 'streampipe')
    spark_app_name: str = os.getenv('SPARK_APP_NAME', 'streampipe')
    spark_master_url: str = os.getenv('SPARK_MASTER_URL', 'spark://localhost:7077')
    kafka_bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_topic: str = os.getenv('KAFKA_TOPIC', 'clickstream.events')
    checkpoint_dir: Path = Path(os.getenv('CHECKPOINT_DIR', 'data/checkpoints/clickstream'))
    raw_taxi_data_dir: Path = Path(os.getenv('RAW_TAXI_DATA_DIR', 'data/raw/nyc_taxi'))
    taxi_zone_lookup_path: Path = Path(os.getenv('TAXI_ZONE_LOOKUP_PATH', 'data/raw/nyc_taxi/reference/taxi_zone_lookup.csv'))
    taxi_pickup_year: int = int(os.getenv('TAXI_PICKUP_YEAR', '2023'))
    processed_data_dir: Path = Path(os.getenv('PROCESSED_DATA_DIR', 'data/processed'))
    delta_lake_dir: Path = Path(os.getenv('DELTA_LAKE_DIR', 'data/processed/delta'))


def get_settings() -> ProjectSettings:
    return ProjectSettings()
