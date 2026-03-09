.PHONY: up down restart logs ps topic-create topic-list app simulate batch batch-delta delta-migrate delta-demo benchmark benchmark-partitioning benchmark-compare stream stream-demo test batch-docker nyc-download nyc-inspect

DOCKER ?= "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
WINPWD := $(shell wslpath -w "$(CURDIR)")
COMPOSE = $(DOCKER) compose --project-directory "$(WINPWD)" -f "$(WINPWD)\\docker-compose.yml"
JAVA_HOME ?= $(CURDIR)/.jdk
PYTHON ?= .venv/bin/python
KAFKA_PACKAGE ?= org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8
STREAM_CONNECTOR_ARGS ?= --kafka-package $(KAFKA_PACKAGE)
BATCH_INPUT_PATH ?= data/raw/nyc_taxi/yellow/2023
BATCH_ZONE_LOOKUP_PATH ?= data/raw/nyc_taxi/reference/taxi_zone_lookup.csv
BATCH_PICKUP_YEAR ?= 2023
BATCH_OUTPUT_PATH ?= data/processed/batch_taxi_metrics
BATCH_DELTA_OUTPUT_PATH ?= data/processed/delta/batch_taxi_metrics
DELTA_MIGRATION_SOURCE_PATH ?= data/processed/batch_taxi_metrics_jan/summary
DELTA_MIGRATION_TARGET_PATH ?= data/processed/delta/day4/batch_summary_delta_example
DELTA_DEMO_TARGET_PATH ?= data/processed/delta/day4/time_travel_orders_demo
BENCHMARK_OUTPUT_PATH ?= data/benchmarks/pandas_vs_spark_jan2023
PARTITIONING_BENCHMARK_OUTPUT_PATH ?= data/benchmarks/partitioning_effect_demo
PARTITIONING_BENCHMARK_DATA_ROOT ?= data/benchmarks/partitioning_effect_demo_data
BATCH_STREAMING_OUTPUT_PATH ?= data/benchmarks/batch_vs_streaming_jan2023
BENCHMARK_ROW_LIMITS ?= 100000 500000 full
BATCH_INPUT_PATH_DOCKER ?= /workspace/data/raw/nyc_taxi/yellow/2023
BATCH_ZONE_LOOKUP_PATH_DOCKER ?= /workspace/data/raw/nyc_taxi/reference/taxi_zone_lookup.csv
BATCH_OUTPUT_PATH_DOCKER ?= /workspace/data/processed/batch_taxi_metrics_docker

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) down && $(COMPOSE) up -d

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f kafka spark-master spark-worker

topic-create:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic clickstream.events --bootstrap-server kafka:19092 --partitions 1 --replication-factor 1

topic-list:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:19092

app:
	streamlit run app.py

simulate:
	$(PYTHON) -m src.simulator.clickstream_generator --count 5 --sleep 0

nyc-download:
	$(PYTHON) -m src.data.nyc_taxi download --dataset yellow --year 2023 --months 1-12 --output-dir data/raw/nyc_taxi --download-zone-lookup

nyc-inspect:
	$(PYTHON) -m src.data.nyc_taxi inspect --input-path data/raw/nyc_taxi/yellow/2023 --report-dir docs/reports/nyc_taxi_2023_yellow

batch:
	JAVA_HOME="$(JAVA_HOME)" PATH="$(JAVA_HOME)/bin:$$PATH" SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost $(PYTHON) -m src.batch.main --input-path $(BATCH_INPUT_PATH) --zone-lookup-path $(BATCH_ZONE_LOOKUP_PATH) --pickup-year $(BATCH_PICKUP_YEAR) --output-path $(BATCH_OUTPUT_PATH)

batch-delta:
	JAVA_HOME="$(JAVA_HOME)" PATH="$(JAVA_HOME)/bin:$$PATH" SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost $(PYTHON) -m src.batch.main --input-path $(BATCH_INPUT_PATH) --zone-lookup-path $(BATCH_ZONE_LOOKUP_PATH) --pickup-year $(BATCH_PICKUP_YEAR) --output-format delta --output-path $(BATCH_DELTA_OUTPUT_PATH)

delta-migrate:
	JAVA_HOME="$(JAVA_HOME)" PATH="$(JAVA_HOME)/bin:$$PATH" SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost $(PYTHON) -m src.batch.delta_examples migrate --source-path $(DELTA_MIGRATION_SOURCE_PATH) --target-path $(DELTA_MIGRATION_TARGET_PATH) --report-path docs/reports/delta_migration_example

delta-demo:
	JAVA_HOME="$(JAVA_HOME)" PATH="$(JAVA_HOME)/bin:$$PATH" SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost $(PYTHON) -m src.batch.delta_examples demo --target-path $(DELTA_DEMO_TARGET_PATH) --report-path docs/reports/delta_time_travel_demo

benchmark:
	JAVA_HOME="$(JAVA_HOME)" PATH="$(JAVA_HOME)/bin:$$PATH" SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost $(PYTHON) -m src.benchmarks.pandas_vs_spark --input-path $(BATCH_INPUT_PATH) --zone-lookup-path $(BATCH_ZONE_LOOKUP_PATH) --pickup-year $(BATCH_PICKUP_YEAR) --row-limits $(BENCHMARK_ROW_LIMITS) --output-path $(BENCHMARK_OUTPUT_PATH)

benchmark-partitioning:
	JAVA_HOME="$(JAVA_HOME)" PATH="$(JAVA_HOME)/bin:$$PATH" SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost $(PYTHON) -m src.benchmarks.partitioning_effect --output-root $(PARTITIONING_BENCHMARK_DATA_ROOT) --report-path $(PARTITIONING_BENCHMARK_OUTPUT_PATH)

benchmark-compare:
	JAVA_HOME="$(JAVA_HOME)" PATH="$(JAVA_HOME)/bin:$$PATH" SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost $(PYTHON) -m src.benchmarks.batch_vs_streaming --benchmark-report $(BENCHMARK_OUTPUT_PATH).json --output-path $(BATCH_STREAMING_OUTPUT_PATH)

stream:
	JAVA_HOME="$(JAVA_HOME)" PATH="$(JAVA_HOME)/bin:$$PATH" SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost $(PYTHON) -m src.streaming.main --sink console --await-termination $(STREAM_CONNECTOR_ARGS)

stream-demo:
	JAVA_HOME="$(JAVA_HOME)" PATH="$(JAVA_HOME)/bin:$$PATH" SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost $(PYTHON) -m src.streaming.demo $(STREAM_CONNECTOR_ARGS)

batch-docker:
	$(COMPOSE) exec -w /workspace -e PYTHONPATH=/workspace spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/batch/main.py --input-path $(BATCH_INPUT_PATH_DOCKER) --zone-lookup-path $(BATCH_ZONE_LOOKUP_PATH_DOCKER) --pickup-year $(BATCH_PICKUP_YEAR) --output-path $(BATCH_OUTPUT_PATH_DOCKER)

test:
	$(PYTHON) -m pytest
