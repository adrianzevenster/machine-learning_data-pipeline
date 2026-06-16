PYTHON ?= python3
PYTEST ?= $(PYTHON) -m pytest
COMPOSE_FILE := airflow-docker/docker-compose.yml

.PHONY: up down restart logs ps quality compile compose-check dag-import static-contracts \
        secret-patterns test smoke build-images dvc-repro dvc-status dvc-metrics dvc-params-diff

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

restart:
	docker compose -f $(COMPOSE_FILE) up -d --build

logs:
	docker compose -f $(COMPOSE_FILE) logs -f --tail=100

ps:
	docker compose -f $(COMPOSE_FILE) ps

quality: compile compose-check dag-import static-contracts secret-patterns test

smoke: compose-check dag-import static-contracts

compile:
	$(PYTHON) -m py_compile \
		airflow-docker/dags/docker_container_orchestration.py \
		airflow-docker/ExploratoryDataAnalysis/EDA.py \
		airflow-docker/ExploratoryDataAnalysis/main.py \
		airflow-docker/flaskapp/DataBase.py \
		airflow-docker/flaskapp/request_validation.py \
		airflow-docker/flaskapp/streamingestion.py \
		airflow-docker/model_monitoring/Model_Monitoring.py \
		airflow-docker/model_monitoring/check_drift_alert.py \
		airflow-docker/model_monitoring/feature_drift.py \
		airflow-docker/quality/check_secret_patterns.py \
		airflow-docker/quality/check_static_contracts.py \
		airflow-docker/quality/validate_mysql_tables.py \
		airflow-docker/quality/validate_raw_schema.py \
		airflow-docker/pySpark/model_card.py \
		airflow-docker/pySpark/PySparkAnalysis.py \
		airflow-docker/pySpark/pySparkModel.py \
		airflow-docker/model_monitoring/rollback_model.py \
		airflow-docker/model_monitoring/record_ab_outcomes.py \
		airflow-docker/model_monitoring/ab_analysis.py \
		airflow-docker/serving/prediction_logger.py \
		airflow-docker/serving/main.py

compose-check:
	docker compose --profile build -f $(COMPOSE_FILE) config --quiet

dag-import:
	$(PYTHON) -m py_compile airflow-docker/dags/docker_container_orchestration.py

static-contracts:
	$(PYTHON) airflow-docker/quality/check_static_contracts.py

secret-patterns:
	$(PYTHON) airflow-docker/quality/check_secret_patterns.py

test:
	PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $(PYTEST) -p no:cacheprovider airflow-docker/tests

load-test:
	locust -f airflow-docker/tests/locustfile.py \
		--headless \
		--users 20 \
		--spawn-rate 5 \
		--run-time 60s \
		--host http://localhost:$${SERVING_HOST_PORT:-8000} \
		--csv /tmp/locust_results \
		--only-summary

build-images:
	docker compose --profile build -f $(COMPOSE_FILE) build \
		custom-airflow \
		flaskapp \
		python-app \
		pyspark-app \
		model-monitoring \
		mlflow \
		serving-app

# ── DVC targets ──────────────────────────────────────────────────────────────
# Requires: pip install dvc  and  docker compose up -d mysql mlflow flaskapp

dvc-repro:
	dvc repro

dvc-status:
	dvc status

# Show metrics for the current run and compare to the previous commit
dvc-metrics:
	dvc metrics show --md

dvc-params-diff:
	dvc params diff
