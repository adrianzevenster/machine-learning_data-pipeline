#!/bin/bash
set -euo pipefail

# Params injected as env vars from dvc.yaml:
#   MIN_RAW_ROWS, DATA_MAX_STALENESS_DAYS

mkdir -p data/validated

docker run --rm \
  --network airflow-network \
  -e MYSQL_HOST=mysql \
  -e MYSQL_USER="${MYSQL_USER:-spark}" \
  -e MYSQL_PASSWORD="${MYSQL_PASSWORD:-sparkpw}" \
  -e MYSQL_DATABASE="${MYSQL_DATABASE:-RawData}" \
  -e MIN_RAW_ROWS="${MIN_RAW_ROWS:-1000}" \
  -e DATA_MAX_STALENESS_DAYS="${DATA_MAX_STALENESS_DAYS:-7}" \
  model-monitoring:latest \
  python /app/quality/validate_raw_schema.py raw

touch data/validated/.validate_raw_done
