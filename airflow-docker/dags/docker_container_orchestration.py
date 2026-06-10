from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import os
import socket

from airflow import DAG
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

def _on_task_failure(context) -> None:
    """Post a Slack notification when any pipeline task fails."""
    import json
    import urllib.request

    webhook = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook:
        return
    ti = context["task_instance"]
    msg = (
        f":x: *Pipeline task failed*\n"
        f"DAG: `{ti.dag_id}`  Task: `{ti.task_id}`\n"
        f"Run: `{ti.run_id}`\n"
        f"Log: {ti.log_url}"
    )
    try:
        payload = json.dumps({"text": msg}).encode()
        req = urllib.request.Request(
            webhook,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as exc:
        print(f"[failure-alert] Slack notification failed: {exc}")


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_task_failure,
}

MYSQL_HOST = "mysql"
MYSQL_PORT = 3306
MYSQL_USER = os.getenv("APP_MYSQL_USER", os.getenv("MYSQL_USER", "spark"))
MYSQL_PWD = os.getenv("APP_MYSQL_PASSWORD", os.getenv("MYSQL_PASSWORD", "sparkpw"))
MYSQL_DB = os.getenv("MYSQL_DATABASE", "RawData")
SPARK_MYSQL_USER = os.getenv("SPARK_MYSQL_USER", "spark")
SPARK_MYSQL_PWD = os.getenv("SPARK_MYSQL_PASSWORD", "sparkpw")
MLFLOW_HOST = os.getenv("MLFLOW_HOST", "mlflow")
MLFLOW_PORT = int(os.getenv("MLFLOW_PORT", "5000"))
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", f"http://{MLFLOW_HOST}:{MLFLOW_PORT}")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "customer_churn")
MLFLOW_REGISTERED_MODEL_NAME = os.getenv("MLFLOW_REGISTERED_MODEL_NAME", "customer_churn_random_forest")
MODEL_PROMOTION_STAGE = os.getenv("MODEL_PROMOTION_STAGE", "Production")
MIN_MODEL_AUC = os.getenv("MIN_MODEL_AUC", "0.5")
MIN_PROMOTION_PREDICTION_ROWS = os.getenv("MIN_PROMOTION_PREDICTION_ROWS", "1")
MODEL_TRAINING_MAX_ROWS = os.getenv("MODEL_TRAINING_MAX_ROWS", "250000")
MODEL_CV_FOLDS = os.getenv("MODEL_CV_FOLDS", "2")
SPARK_SQL_SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4")

DOCKER_NETWORK = os.getenv("AIRFLOW_DOCKER_NETWORK", "airflow-network")
PIPELINE_RUN_ID = "{{ dag.dag_id }}__{{ ts_nodash }}"
MODEL_VERSION_ID = "{{ dag.dag_id }}__{{ ts_nodash }}__random_forest"
GIT_SHA = os.getenv("GIT_SHA", "unknown")
PIPELINE_SCHEDULE = os.getenv("PIPELINE_SCHEDULE", "0 2 * * 0")

DATA_PIPELINE_ROOT = Variable.get("DATA_PIPELINE_ROOT", default_var=None)
PARQUET_OUT_HOST = Variable.get("PARQUET_OUT_HOST", default_var=None)

EDA_MOUNTS = []
if DATA_PIPELINE_ROOT:
    eda_out = Path(DATA_PIPELINE_ROOT) / "ExploratoryDataAnalysis" / "output"
    if eda_out.exists():
        EDA_MOUNTS = [Mount(source=str(eda_out), target="/app/output", type="bind")]

PYSPARK_ANALYSIS_MOUNTS = []
if PARQUET_OUT_HOST and Path(PARQUET_OUT_HOST).exists():
    PYSPARK_ANALYSIS_MOUNTS = [Mount(source=str(PARQUET_OUT_HOST), target="/out", type="bind")]

MONITORING_MOUNTS = []
if DATA_PIPELINE_ROOT:
    monitoring_out = Path(DATA_PIPELINE_ROOT) / "output"
    if monitoring_out.exists():
        MONITORING_MOUNTS = [Mount(source=str(monitoring_out), target="/app/output", type="bind")]


def _wait_for_mysql() -> bool:
    try:
        with socket.create_connection((MYSQL_HOST, MYSQL_PORT), timeout=2):
            return True
    except OSError:
        return False


def _wait_for_mlflow() -> bool:
    try:
        with socket.create_connection((MLFLOW_HOST, MLFLOW_PORT), timeout=2):
            return True
    except OSError:
        return False


with DAG(
        dag_id="local_dev_pipeline",
        description="Orchestrate MySQL -> Flask -> EDA -> PySpark -> Monitoring",
        default_args=DEFAULT_ARGS,
        schedule_interval=PIPELINE_SCHEDULE,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(hours=4),
        tags=["local", "ml", "monitoring"],
) as dag:

    mysql_ready = PythonSensor(
        task_id="mysql_ready",
        python_callable=_wait_for_mysql,
        poke_interval=5,
        timeout=300,
        mode="poke",
    )

    mlflow_ready = PythonSensor(
        task_id="mlflow_ready",
        python_callable=_wait_for_mlflow,
        poke_interval=5,
        timeout=300,
        mode="poke",
    )

    run_mysql_migrations = DockerOperator(
        task_id="run_mysql_migrations",
        image="model-monitoring:latest",
        container_name="run-mysql-migrations-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
            "MYSQL_MIGRATIONS_DIR": "/app/mysql/migrations",
        },
        command="python /app/quality/run_mysql_migrations.py",
    )

    start_stream = SimpleHttpOperator(
        task_id="start_flask_stream",
        http_conn_id="flask_service",
        endpoint="start_stream",
        method="POST",
        data='{"batch_size":1000,"num_batches":10,"interval":10}',
        headers={"Content-Type": "application/json"},
        retries=6,
        retry_delay=timedelta(seconds=30),
        log_response=True,
    )

    DRIFT_ALERT_MIN_ROC_AUC = os.getenv("DRIFT_ALERT_MIN_ROC_AUC", "0.6")
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
    LABEL_DELAY_DAYS = os.getenv("LABEL_DELAY_DAYS", "30")
    AB_MIN_SAMPLE_SIZE = os.getenv("AB_MIN_SAMPLE_SIZE", "200")
    AB_SIGNIFICANCE_ALPHA = os.getenv("AB_SIGNIFICANCE_ALPHA", "0.05")
    MIN_RAW_ROWS = os.getenv("MIN_RAW_ROWS", "1000")
    DATA_MAX_STALENESS_DAYS = os.getenv("DATA_MAX_STALENESS_DAYS", "7")

    validate_raw_data = DockerOperator(
        task_id="validate_raw_data",
        image="model-monitoring:latest",
        container_name="validate-raw-data-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
            "MIN_RAW_ROWS": "1",
        },
        command="python /app/quality/validate_mysql_tables.py raw",
    )

    validate_raw_schema = DockerOperator(
        task_id="validate_raw_schema",
        image="model-monitoring:latest",
        container_name="validate-raw-schema-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
            "MIN_RAW_ROWS": MIN_RAW_ROWS,
            "DATA_MAX_STALENESS_DAYS": DATA_MAX_STALENESS_DAYS,
        },
        command="python /app/quality/validate_raw_schema.py raw",
    )

    run_eda = DockerOperator(
        task_id="run_eda",
        image="python-app:latest",
        container_name="eda-python-app-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
        },
        mounts=EDA_MOUNTS,
        command="python EDA.py",
    )

    pyspark_db_dns_check = DockerOperator(
        task_id="pyspark_db_dns_check",
        image="busybox:1.36",
        container_name="pyspark-dns-check-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={"MYSQL_HOST": MYSQL_HOST, "MYSQL_PORT": str(MYSQL_PORT)},
        command="sh -lc 'nslookup mysql && nc -vz -w 2 mysql 3306'",
    )

    pyspark_analysis = DockerOperator(
        task_id="pyspark_analysis",
        image="pyspark-app:latest",
        entrypoint="/bin/sh",
        command=["-lc", "python3 /app/PySparkAnalysis.py"],
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        environment={
            "MYSQL_HOST": "mysql",
            "MYSQL_DATABASE": "RawData",
            "MYSQL_USER": SPARK_MYSQL_USER,
            "MYSQL_PASSWORD": SPARK_MYSQL_PWD,
            "AIRFLOW_DAG_ID": "local_dev_pipeline",
            "PIPELINE_RUN_ID": PIPELINE_RUN_ID,
            "CHURN_INACTIVE_DAYS": "1",
            "PROCESSED_WRITE_MODE": "overwrite",
            "PYSPARK_PYTHON": "python3",
            "SPARK_DRIVER_MEMORY": "4g",
            "SPARK_EXECUTOR_MEMORY": "4g",
            "PYSPARK_SUBMIT_ARGS": "--conf spark.sql.shuffle.partitions=8 pyspark-shell",
        },
        mount_tmp_dir=False,
        tty=False,
    )

    pyspark_model = DockerOperator(
        task_id="pyspark_model",
        image="pyspark-app:latest",
        api_version="auto",
        auto_remove=True,
        entrypoint="/bin/sh",
        command=[
            "-lc",
            "if ! grep -q MODEL_TRAINING_MAX_ROWS /app/pySparkModel.py; then "
            "echo '[fatal] pyspark-app image is stale; rebuild pyspark-app:latest'; exit 42; "
            "fi; "
            "python3 /app/pySparkModel.py",
        ],
        environment={
            "MYSQL_HOST": "mysql",
            "MYSQL_DATABASE": "RawData",
            "MYSQL_USER": SPARK_MYSQL_USER,
            "MYSQL_PASSWORD": SPARK_MYSQL_PWD,
            "AIRFLOW_DAG_ID": "local_dev_pipeline",
            "PIPELINE_RUN_ID": PIPELINE_RUN_ID,
            "MODEL_VERSION_ID": MODEL_VERSION_ID,
            "GIT_SHA": GIT_SHA,
            "IMAGE_TAG": "pyspark-app:latest",
            "MODEL_ARTIFACT_URI": f"/tmp/models/{MODEL_VERSION_ID}",
            "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
            "MLFLOW_REGISTRY_URI": MLFLOW_TRACKING_URI,
            "MLFLOW_EXPERIMENT_NAME": MLFLOW_EXPERIMENT_NAME,
            "MLFLOW_REGISTERED_MODEL_NAME": MLFLOW_REGISTERED_MODEL_NAME,
            "GIT_PYTHON_REFRESH": "quiet",
            "MODEL_PREDICTIONS_WRITE_MODE": "append",
            "MODEL_TRAINING_MAX_ROWS": MODEL_TRAINING_MAX_ROWS,
            "MODEL_CV_FOLDS": MODEL_CV_FOLDS,
            "PYSPARK_PYTHON": "python3",
            "SPARK_DRIVER_MEMORY": "4g",
            "SPARK_EXECUTOR_MEMORY": "4g",
            "SPARK_SQL_SHUFFLE_PARTITIONS": SPARK_SQL_SHUFFLE_PARTITIONS,
            "PYSPARK_SUBMIT_ARGS": f"--conf spark.sql.shuffle.partitions={SPARK_SQL_SHUFFLE_PARTITIONS} pyspark-shell",
        },
        network_mode=DOCKER_NETWORK,
        docker_url="unix:///var/run/docker.sock",
        mount_tmp_dir=False,
        mem_limit="4g",
    )

    validate_processed_data = DockerOperator(
        task_id="validate_processed_data",
        image="model-monitoring:latest",
        container_name="validate-processed-data-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
            "MIN_PROCESSED_ROWS": "1",
        },
        command="python /app/quality/validate_mysql_tables.py processed",
    )

    validate_model_predictions = DockerOperator(
        task_id="validate_model_predictions",
        image="model-monitoring:latest",
        container_name="validate-model-predictions-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
            "MIN_PREDICTION_ROWS": "1",
            "PIPELINE_RUN_ID": PIPELINE_RUN_ID,
        },
        command="python /app/quality/validate_mysql_tables.py predictions",
    )

    promote_model = DockerOperator(
        task_id="promote_model",
        image="model-monitoring:latest",
        container_name="promote-model-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
            "PIPELINE_RUN_ID": PIPELINE_RUN_ID,
            "MODEL_VERSION_ID": MODEL_VERSION_ID,
            "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
            "MLFLOW_REGISTRY_URI": MLFLOW_TRACKING_URI,
            "MLFLOW_REGISTERED_MODEL_NAME": MLFLOW_REGISTERED_MODEL_NAME,
            "MODEL_PROMOTION_STAGE": MODEL_PROMOTION_STAGE,
            "MIN_MODEL_AUC": MIN_MODEL_AUC,
            "MIN_PROMOTION_PREDICTION_ROWS": MIN_PROMOTION_PREDICTION_ROWS,
        },
        command="python /app/promote_model.py",
    )

    run_monitoring = DockerOperator(
        task_id="run_monitoring",
        image="model-monitoring:latest",
        container_name="model-monitoring-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
            "PIPELINE_RUN_ID": PIPELINE_RUN_ID,
            "MODEL_VERSION_ID": MODEL_VERSION_ID,
        },
        mounts=MONITORING_MOUNTS,
        command="python /app/Model_Monitoring.py",
    )

    run_feature_drift = DockerOperator(
        task_id="run_feature_drift",
        image="model-monitoring:latest",
        container_name="run-feature-drift-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
            "PIPELINE_RUN_ID": PIPELINE_RUN_ID,
            "MODEL_VERSION_ID": MODEL_VERSION_ID,
        },
        mounts=MONITORING_MOUNTS,
        command="python /app/feature_drift.py",
    )

    check_drift_alert = DockerOperator(
        task_id="check_drift_alert",
        image="model-monitoring:latest",
        container_name="check-drift-alert-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "PIPELINE_RUN_ID": PIPELINE_RUN_ID,
            "MONITORING_OUTPUT_DIR": "/app/output/monitoring",
            "DRIFT_ALERT_MIN_ROC_AUC": DRIFT_ALERT_MIN_ROC_AUC,
            "SLACK_WEBHOOK_URL": SLACK_WEBHOOK_URL,
            "AUTO_RETRAIN_ON_DRIFT": os.getenv("AUTO_RETRAIN_ON_DRIFT", "false"),
            "AIRFLOW_API_URL": "http://airflow-webserver:8080",
            "AIRFLOW_DAG_ID": "local_dev_pipeline",
            "AIRFLOW_ADMIN_USERNAME": os.getenv("AIRFLOW_ADMIN_USERNAME", "airflow"),
            "AIRFLOW_ADMIN_PASSWORD": os.getenv("AIRFLOW_ADMIN_PASSWORD", "airflow"),
        },
        mounts=MONITORING_MOUNTS,
        command="python /app/check_drift_alert.py",
    )

    record_ab_outcomes = DockerOperator(
        task_id="record_ab_outcomes",
        image="model-monitoring:latest",
        container_name="record-ab-outcomes-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
            "LABEL_DELAY_DAYS": LABEL_DELAY_DAYS,
        },
        command="python /app/record_ab_outcomes.py",
    )

    run_ab_analysis = DockerOperator(
        task_id="run_ab_analysis",
        image="model-monitoring:latest",
        container_name="run-ab-analysis-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        do_xcom_push=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
            "SLACK_WEBHOOK_URL": SLACK_WEBHOOK_URL,
            "AB_MIN_SAMPLE_SIZE": AB_MIN_SAMPLE_SIZE,
            "AB_SIGNIFICANCE_ALPHA": AB_SIGNIFICANCE_ALPHA,
            "AB_LIFT_ALERT_THRESHOLD": os.getenv("AB_LIFT_ALERT_THRESHOLD", "0.02"),
            "LABEL_DELAY_DAYS": LABEL_DELAY_DAYS,
            "MONITORING_OUTPUT_DIR": "/app/output/monitoring",
        },
        mounts=MONITORING_MOUNTS,
        command="python /app/ab_analysis.py",
    )

    (
        mysql_ready
        >> run_mysql_migrations
        >> pyspark_db_dns_check
        >> start_stream
        >> validate_raw_data
        >> validate_raw_schema
        >> run_eda
        >> pyspark_analysis
        >> validate_processed_data
        >> mlflow_ready
        >> pyspark_model
        >> validate_model_predictions
        >> promote_model
        >> run_monitoring
        >> run_feature_drift
        >> check_drift_alert
        >> record_ab_outcomes
        >> run_ab_analysis
    )
