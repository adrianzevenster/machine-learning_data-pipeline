#!/bin/bash
set -euo pipefail

# Params injected as env vars from dvc.yaml:
#   MLFLOW_REGISTERED_MODEL_NAME, MODEL_PROMOTION_STAGE,
#   MIN_MODEL_AUC, MIN_PROMOTION_PREDICTION_ROWS

# Load run IDs written by the train stage
# shellcheck source=/dev/null
. "$(pwd)/metrics/run_ids.env"

docker run --rm \
  --network airflow-network \
  -e MYSQL_HOST=mysql \
  -e MYSQL_USER=spark \
  -e MYSQL_PASSWORD=sparkpw \
  -e MYSQL_DATABASE=RawData \
  -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
  -e MLFLOW_REGISTRY_URI=http://mlflow:5000 \
  -e MLFLOW_REGISTERED_MODEL_NAME="${MLFLOW_REGISTERED_MODEL_NAME:-customer_churn_random_forest}" \
  -e MODEL_PROMOTION_STAGE="${MODEL_PROMOTION_STAGE:-Production}" \
  -e MIN_MODEL_AUC="${MIN_MODEL_AUC:-0.5}" \
  -e MIN_PROMOTION_PREDICTION_ROWS="${MIN_PROMOTION_PREDICTION_ROWS:-1}" \
  -e PIPELINE_RUN_ID="$PIPELINE_RUN_ID" \
  -e MODEL_VERSION_ID="$MODEL_VERSION_ID" \
  model-monitoring:latest \
  python /app/promote_model.py
