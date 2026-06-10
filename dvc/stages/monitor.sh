#!/bin/bash
set -euo pipefail

# Load run IDs written by the train stage
# shellcheck source=/dev/null
. "$(pwd)/metrics/run_ids.env"

mkdir -p data/monitoring

docker run --rm \
  --network airflow-network \
  -e MYSQL_HOST=mysql \
  -e MYSQL_USER=spark \
  -e MYSQL_PASSWORD=sparkpw \
  -e MYSQL_DATABASE=RawData \
  -e PIPELINE_RUN_ID="$PIPELINE_RUN_ID" \
  -e MODEL_VERSION_ID="$MODEL_VERSION_ID" \
  -v "$(pwd)/data/monitoring:/app/output/monitoring" \
  model-monitoring:latest \
  python /app/Model_Monitoring.py

docker run --rm \
  -e PIPELINE_RUN_ID="$PIPELINE_RUN_ID" \
  -e DRIFT_ALERT_MIN_ROC_AUC="${DRIFT_ALERT_MIN_ROC_AUC:-0.6}" \
  -e SLACK_WEBHOOK_URL="${SLACK_WEBHOOK_URL:-}" \
  -e MONITORING_OUTPUT_DIR=/app/output/monitoring \
  -v "$(pwd)/data/monitoring:/app/output/monitoring:ro" \
  model-monitoring:latest \
  python /app/check_drift_alert.py || echo "[drift-alert] alert check failed (non-fatal)"

docker run --rm \
  --network airflow-network \
  -e MYSQL_HOST=mysql \
  -e MYSQL_USER="${MYSQL_USER:-spark}" \
  -e MYSQL_PASSWORD="${MYSQL_PASSWORD:-sparkpw}" \
  -e MYSQL_DATABASE="${MYSQL_DATABASE:-RawData}" \
  -e PIPELINE_RUN_ID="$PIPELINE_RUN_ID" \
  model-monitoring:latest \
  python /app/feature_drift.py || echo "[feature-drift] check failed (non-fatal)"
