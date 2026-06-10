#!/bin/bash
set -euo pipefail

# Params injected as env vars from dvc.yaml:
#   LABEL_DELAY_DAYS, AB_MIN_SAMPLE_SIZE, AB_SIGNIFICANCE_ALPHA, AB_LIFT_ALERT_THRESHOLD

mkdir -p data/ab

# Backfill actual_churn from CDR data into serving_predictions.
# Non-fatal: if the table is empty or doesn't exist yet (fresh env),
# the UPDATE affects 0 rows and the script exits cleanly.
docker run --rm \
  --network airflow-network \
  -e MYSQL_HOST=mysql \
  -e MYSQL_USER="${MYSQL_USER:-spark}" \
  -e MYSQL_PASSWORD="${MYSQL_PASSWORD:-sparkpw}" \
  -e MYSQL_DATABASE="${MYSQL_DATABASE:-RawData}" \
  -e LABEL_DELAY_DAYS="${LABEL_DELAY_DAYS:-30}" \
  model-monitoring:latest \
  python /app/record_ab_outcomes.py || echo "[ab-outcomes] outcome recording failed (non-fatal)"

# Run A/B analysis. Writes data/ab/ab_analysis_report.json.
# Non-fatal: exits cleanly with an empty report when no labelled data exists.
docker run --rm \
  --network airflow-network \
  -e MYSQL_HOST=mysql \
  -e MYSQL_USER="${MYSQL_USER:-spark}" \
  -e MYSQL_PASSWORD="${MYSQL_PASSWORD:-sparkpw}" \
  -e MYSQL_DATABASE="${MYSQL_DATABASE:-RawData}" \
  -e SLACK_WEBHOOK_URL="${SLACK_WEBHOOK_URL:-}" \
  -e AB_MIN_SAMPLE_SIZE="${AB_MIN_SAMPLE_SIZE:-200}" \
  -e AB_SIGNIFICANCE_ALPHA="${AB_SIGNIFICANCE_ALPHA:-0.05}" \
  -e AB_LIFT_ALERT_THRESHOLD="${AB_LIFT_ALERT_THRESHOLD:-0.02}" \
  -e LABEL_DELAY_DAYS="${LABEL_DELAY_DAYS:-30}" \
  -e MONITORING_OUTPUT_DIR=/app/output/ab \
  -v "$(pwd)/data/ab:/app/output/ab" \
  model-monitoring:latest \
  python /app/ab_analysis.py || echo "[ab-analysis] analysis failed (non-fatal)"
