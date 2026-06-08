#!/bin/bash
set -euo pipefail

# Params injected as env vars from dvc.yaml:
#   SPARK_DRIVER_MEMORY, SPARK_EXECUTOR_MEMORY,
#   SPARK_SQL_SHUFFLE_PARTITIONS, CHURN_INACTIVE_DAYS

docker run --rm \
  --network airflow-network \
  -e MYSQL_HOST=mysql \
  -e MYSQL_DATABASE=RawData \
  -e MYSQL_USER=spark \
  -e MYSQL_PASSWORD=sparkpw \
  -e AIRFLOW_DAG_ID=dvc_pipeline \
  -e PIPELINE_RUN_ID=dvc_local \
  -e PYSPARK_PYTHON=python3 \
  -e SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}" \
  -e SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}" \
  -e SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  -e CHURN_INACTIVE_DAYS="${CHURN_INACTIVE_DAYS:-1}" \
  -e PROCESSED_WRITE_MODE=overwrite \
  pyspark-app:latest \
  /bin/sh -lc 'python3 /app/PySparkAnalysis.py'

mkdir -p data/processed
touch data/processed/.features_done
