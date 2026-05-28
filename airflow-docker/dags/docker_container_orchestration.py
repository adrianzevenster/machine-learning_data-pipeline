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

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MYSQL_HOST = "mysql"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PWD = "a?xBVq1!"
MYSQL_DB = "RawData"
SPARK_MYSQL_USER = os.getenv("SPARK_MYSQL_USER", "spark")
SPARK_MYSQL_PWD = os.getenv("SPARK_MYSQL_PASSWORD", "sparkpw")

DOCKER_NETWORK = os.getenv("AIRFLOW_DOCKER_NETWORK", "airflow-network")

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


with DAG(
        dag_id="local_dev_pipeline",
        description="Orchestrate MySQL -> Flask -> EDA -> PySpark -> Monitoring",
        default_args=DEFAULT_ARGS,
        schedule_interval=None,
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
        command=["-lc", "python3 /app/pySparkModel.py"],
        environment={
            "MYSQL_HOST": "mysql",
            "MYSQL_DATABASE": "RawData",
            "MYSQL_USER": SPARK_MYSQL_USER,
            "MYSQL_PASSWORD": SPARK_MYSQL_PWD,
            "MODEL_PREDICTIONS_WRITE_MODE": "overwrite",
            "PYSPARK_PYTHON": "python3",
            "SPARK_DRIVER_MEMORY": "4g",
            "SPARK_EXECUTOR_MEMORY": "4g",
            "PYSPARK_SUBMIT_ARGS": "--conf spark.sql.shuffle.partitions=4 pyspark-shell",
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
        },
        command="python /app/quality/validate_mysql_tables.py predictions",
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
        },
        mounts=MONITORING_MOUNTS,
        command="python /app/Model_Monitoring.py",
    )

    (
        mysql_ready
        >> pyspark_db_dns_check
        >> start_stream
        >> validate_raw_data
        >> run_eda
        >> pyspark_analysis
        >> validate_processed_data
        >> pyspark_model
        >> validate_model_predictions
        >> run_monitoring
    )
