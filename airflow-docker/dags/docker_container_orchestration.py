"""
Airflow DAG: local_dev_pipeline  – FULL VERSION (with PySpark tasks)

• Waits for MySQL (local-mysql) to be reachable.
• POSTs /start_stream to the Flask service.
• Runs EDA container that binds host output folder.
• Executes PySparkAnalysis and PySparkModel in sequence.

Prerequisites
-------------
1. Airflow Connection `flask_service` → HTTP http://flaskapp:5000
2. Airflow Variables:
   - `DATA_PIPELINE_ROOT`: absolute host path to project root.
   - `SPARK_PROJECT_ROOT`: absolute host path to PySpark folder (contains data/, output/, config.json).

"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import socket

from airflow import DAG
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ─── Defaults ─────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── Constants ────────────────────────────────────────────────────────────────
MYSQL_HOST = "local-mysql"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PWD  = "a?xBVq1!"
MYSQL_DB   = "RawData"
DOCKER_NETWORK = "airflow-docker_airflow-network"  # update if your network name differs

# Host path for EDA output (mount into EDA container)
PROJECT_ROOT = Path(Variable.get("DATA_PIPELINE_ROOT"))
EDA_OUTPUT_HOST = PROJECT_ROOT / "ExploratoryDataAnalysis" / "output"
if not EDA_OUTPUT_HOST.is_absolute():
    raise ValueError(f"DATA_PIPELINE_ROOT must be absolute, got: {PROJECT_ROOT}")

# Host paths for PySpark tasks
SPARK_ROOT = Path(Variable.get("SPARK_PROJECT_ROOT", str(PROJECT_ROOT / "pySpark")))
SPARK_DATA_HOST = SPARK_ROOT / "data"
SPARK_OUTPUT_HOST = SPARK_ROOT / "output"         # will be mounted at /app/parquetFiles
SPARK_CONFIG_HOST = SPARK_ROOT / "config.json"

# (No hard parse-time checks here; if a path is wrong/missing Docker will raise
# at task runtime. Ensure your host has these paths and config.json present.)

# ─── MySQL readiness callable (no external deps) ────────────────────────────── (no external deps) ──────────────────────────────

def _wait_for_mysql() -> bool:
    try:
        with socket.create_connection((MYSQL_HOST, MYSQL_PORT), timeout=2):
            return True
    except OSError:
        return False

# ─── DAG Definition ───────────────────────────────────────────────────────────
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
    )

    # EDA container (python-app:latest)
    run_eda = DockerOperator(
        task_id="run_eda",
        image="python-app:latest",
        container_name="eda-python-app-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
        },
        mounts=[
            Mount(source=str(EDA_OUTPUT_HOST), target="/app/output", type="bind"),
        ],
        command="python EDA.py",
    )

    # PySpark Analysis → writes parquet to /app/parquetFiles
    pyspark_analysis = DockerOperator(
        task_id="pyspark_analysis",
        image="pyspark-app:latest",
        container_name="pyspark-analysis-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
        },
        mounts=[
            Mount(source=str(SPARK_DATA_HOST),   target="/app/data",          type="bind"),
            Mount(source=str(SPARK_OUTPUT_HOST), target="/app/parquetFiles",  type="bind"),
            Mount(source=str(SPARK_CONFIG_HOST), target="/app/config.json",   type="bind"),
        ],
        command="python /app/PySparkAnalysis.py",
    )

    # PySpark Model → consumes files from /app/parquetFiles
    pyspark_model = DockerOperator(
        task_id="pyspark_model",
        image="pyspark-app:latest",
        container_name="pyspark-model-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
        },
        mounts=[
            Mount(source=str(SPARK_OUTPUT_HOST), target="/app/parquetFiles", type="bind"),
            Mount(source=str(SPARK_CONFIG_HOST), target="/app/config.json",  type="bind"),
        ],
        command="python /app/pySparkModel.py",
    )

mysql_ready >> start_stream >> run_eda >> pyspark_analysis >> pyspark_model
