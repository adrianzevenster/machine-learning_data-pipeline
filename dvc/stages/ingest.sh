#!/bin/bash
set -euo pipefail

docker run --rm \
  --network airflow-network \
  -e DB_HOST=mysql \
  -e DB_USER=spark \
  -e DB_PASSWORD=sparkpw \
  -e DB_NAME=RawData \
  -e RAW_DATA_CSV=/data/RawData.csv \
  -v "$(pwd)/airflow-docker/flaskapp/RawData.csv:/data/RawData.csv:ro" \
  flaskapp-flaskapp-app:latest \
  python DataBase.py

mkdir -p data/raw
touch data/raw/.ingest_done
