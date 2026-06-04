PYTHON ?= python3
PYTEST ?= $(PYTHON) -m pytest
COMPOSE_FILE := airflow-docker/docker-compose.yml

.PHONY: quality compile compose-check static-contracts secret-patterns test build-images

quality: compile compose-check static-contracts secret-patterns test

compile:
	$(PYTHON) -m py_compile \
		airflow-docker/dags/docker_container_orchestration.py \
		airflow-docker/ExploratoryDataAnalysis/EDA.py \
		airflow-docker/ExploratoryDataAnalysis/main.py \
		airflow-docker/flaskapp/DataBase.py \
		airflow-docker/flaskapp/request_validation.py \
		airflow-docker/flaskapp/streamingestion.py \
		airflow-docker/model_monitoring/Model_Monitoring.py \
		airflow-docker/quality/check_secret_patterns.py \
		airflow-docker/quality/check_static_contracts.py \
		airflow-docker/quality/validate_mysql_tables.py \
		airflow-docker/pySpark/PySparkAnalysis.py \
		airflow-docker/pySpark/pySparkModel.py

compose-check:
	docker compose --profile build -f $(COMPOSE_FILE) config --quiet

static-contracts:
	$(PYTHON) airflow-docker/quality/check_static_contracts.py

secret-patterns:
	$(PYTHON) airflow-docker/quality/check_secret_patterns.py

test:
	PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $(PYTEST) -p no:cacheprovider airflow-docker/tests

build-images:
	docker compose --profile build -f $(COMPOSE_FILE) build \
		custom-airflow \
		flaskapp \
		python-app \
		pyspark-app \
		model-monitoring
