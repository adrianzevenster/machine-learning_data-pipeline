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
