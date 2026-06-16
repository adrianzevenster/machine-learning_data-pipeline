#!/bin/bash
set -euo pipefail

# Params injected as env vars from dvc.yaml:
#   MODEL_TRAINING_MAX_ROWS, MODEL_CV_FOLDS, MODEL_NUM_TREES, MODEL_MAX_DEPTH
#   SPARK_SQL_SHUFFLE_PARTITIONS, SPARK_DRIVER_MEMORY, SPARK_EXECUTOR_MEMORY
#   MLFLOW_EXPERIMENT_NAME, MLFLOW_REGISTERED_MODEL_NAME, MODEL_PREDICTIONS_WRITE_MODE

mkdir -p models/current metrics

docker run --rm \
  --network airflow-network \
  -e MYSQL_HOST=mysql \
  -e MYSQL_DATABASE=RawData \
  -e MYSQL_USER=spark \
  -e MYSQL_PASSWORD=sparkpw \
  -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
  -e MLFLOW_REGISTRY_URI=http://mlflow:5000 \
  -e MLFLOW_EXPERIMENT_NAME="${MLFLOW_EXPERIMENT_NAME:-customer_churn}" \
  -e MLFLOW_REGISTERED_MODEL_NAME="${MLFLOW_REGISTERED_MODEL_NAME:-customer_churn_random_forest}" \
  -e MODEL_TRAINING_MAX_ROWS="${MODEL_TRAINING_MAX_ROWS:-250000}" \
  -e MODEL_CV_FOLDS="${MODEL_CV_FOLDS:-2}" \
  -e MODEL_NUM_TREES="${MODEL_NUM_TREES:-50}" \
  -e MODEL_MAX_DEPTH="${MODEL_MAX_DEPTH:-5,10}" \
  -e SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  -e SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}" \
  -e SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}" \
  -e MODEL_ARTIFACT_URI=/dvc/models/current \
  -e MODEL_PREDICTIONS_WRITE_MODE="${MODEL_PREDICTIONS_WRITE_MODE:-append}" \
  -e DVC_METRICS_PATH=/dvc/metrics/train_metrics.json \
  -e DVC_MODEL_CARD_PATH=/dvc/metrics/model_card.json \
  -e DVC_RUN_IDS_PATH=/dvc/metrics/run_ids.env \
  -e PYSPARK_PYTHON=python3 \
  -e GIT_PYTHON_REFRESH=quiet \
  -v "$(pwd)/models/current:/dvc/models/current" \
  -v "$(pwd)/metrics:/dvc/metrics" \
  pyspark-app:latest \
  /bin/sh -lc 'python3 /app/pySparkModel.py'
