from __future__ import annotations

import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from pyspark.sql import SparkSession


def build_spark_session(
    app_name: str,
    master_url: str | None = None,
    *,
    with_delta: bool = False,
    extra_packages: Sequence[str] | None = None,
    extra_jars: Sequence[str] | None = None,
    extra_configs: Mapping[str, str] | None = None,
) -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = SparkSession.builder.appName(app_name)

    if master_url:
        builder = builder.master(master_url)

    if with_delta:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder, list(extra_packages or ()))
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    elif extra_packages:
        builder = builder.config("spark.jars.packages", ",".join(extra_packages))

    if extra_jars:
        builder = builder.config("spark.jars", ",".join(str(Path(jar)) for jar in extra_jars))

    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
