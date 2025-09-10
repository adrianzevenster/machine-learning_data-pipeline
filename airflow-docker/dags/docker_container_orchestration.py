# /opt/airflow/dags/docker_container_orchestration.py
"""
Airflow DAG: local_dev_pipeline  – PySpark + EDA, no host mkdirs on import.

What it does
------------
1) Waits for MySQL (local-mysql) to be reachable.
2) Calls Flask /start_stream (via Airflow HTTP conn 'flask_service').
3) Runs EDA container (optional host output mount if it exists).
4) Runs PySparkAnalysis.py (entrypoint=/bin/sh; python3).
5) Runs pySparkModel (uses image's entrypoint).

Optional Airflow Variables
--------------------------
- DATA_PIPELINE_ROOT   (abs host path to your project root; used for EDA output mount if present)
- SPARK_PROJECT_ROOT   (abs host path to pySpark folder; NOT required by DAG; PySpark runs from image)
- PARQUET_OUT_HOST     (abs host path to persist analysis outputs; if missing, no host mount is used)
"""

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

# ──────────────────────────────────────────────────────────────────────────────
# Defaults & constants
# ──────────────────────────────────────────────────────────────────────────────
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
MYSQL_PWD  = "a?xBVq1!"
MYSQL_DB   = "RawData"

# Airflow stack network (where web/scheduler/worker run)
DOCKER_NETWORK = os.getenv("AIRFLOW_DOCKER_NETWORK", "airflow-network")

# Optional host paths from Airflow Variables (no mkdir here!)
DATA_PIPELINE_ROOT = Variable.get("DATA_PIPELINE_ROOT", default_var=None)
SPARK_PROJECT_ROOT = Variable.get("SPARK_PROJECT_ROOT", default_var=None)
PARQUET_OUT_HOST   = Variable.get("PARQUET_OUT_HOST", default_var=None)

# Build optional mounts based on existing paths only
EDA_MOUNTS = []
if DATA_PIPELINE_ROOT:
    eda_out = Path(DATA_PIPELINE_ROOT) / "ExploratoryDataAnalysis" / "output"
    if eda_out.exists():
        EDA_MOUNTS = [Mount(source=str(eda_out), target="/app/output", type="bind")]

PYSPARK_ANALYSIS_MOUNTS = []
if PARQUET_OUT_HOST and Path(PARQUET_OUT_HOST).exists():
    PYSPARK_ANALYSIS_MOUNTS = [Mount(source=str(PARQUET_OUT_HOST), target="/out", type="bind")]

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _wait_for_mysql() -> bool:
    try:
        with socket.create_connection((MYSQL_HOST, MYSQL_PORT), timeout=2):
            return True
    except OSError:
        return False

# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────
with DAG(
        dag_id="local_dev_pipeline",
        description="Orchestrate MySQL → Flask → EDA → PySpark",
        default_args=DEFAULT_ARGS,
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
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

    # EDA container (python-app:latest) on Airflow network
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

    # PySparkAnalysis.py — override entrypoint so we can run python3 directly
    pyspark_analysis = DockerOperator(
        task_id="pyspark_analysis",
        image="pyspark-app:latest",
        entrypoint="/bin/sh",
        command=["-lc", "python3 /app/PySparkAnalysis.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        environment={
            "MYSQL_HOST": "mysql",
            "MYSQL_DATABASE": "RawData",
            "MYSQL_USER": "spark",
            "MYSQL_PASSWORD": "sparkpw",
            "PYSPARK_PYTHON": "python3",
            "SPARK_DRIVER_MEMORY": "4g",
            "SPARK_EXECUTOR_MEMORY": "4g",
            "PYSPARK_SUBMIT_ARGS": "--conf spark.sql.shuffle.partitions=8 pyspark-shell",
        },
        mount_tmp_dir=False,  # avoid the tmp bind mount error
        tty=False,
    )



    # PySpark model — let image entrypoint do its thing (as you had)
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
            "MYSQL_USER": "spark",
            "MYSQL_PASSWORD": "sparkpw",
            "PYSPARK_PYTHON": "python3",
        },
        network_mode=DOCKER_NETWORK,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )


    mysql_ready >> pyspark_db_dns_check >> start_stream >> run_eda >> pyspark_analysis >> pyspark_model
