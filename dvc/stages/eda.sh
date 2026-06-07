#!/bin/bash
set -euo pipefail

mkdir -p data/eda

docker run --rm \
  --network airflow-network \
  -e MYSQL_HOST=mysql \
  -e MYSQL_USER=spark \
  -e MYSQL_PASSWORD=sparkpw \
  -e MYSQL_DATABASE=RawData \
  -v "$(pwd)/data/eda:/app/output" \
  python-app:latest \
  python EDA.py
