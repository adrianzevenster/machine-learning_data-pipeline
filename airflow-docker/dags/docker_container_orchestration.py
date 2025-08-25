from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'local_dev_pipeline',
    default_args=default_args,
    description='CDR Data Processing and ML Pipeline',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
)

# Environment variables for MySQL connection
mysql_env = {
    'MYSQL_HOST': 'local-mysql',
    'MYSQL_PORT': '3306',
    'MYSQL_USER': 'root',
    'MYSQL_PASSWORD': 'a?xBVq1!',
    'MYSQL_DATABASE': 'RawData',
    'PROCESSED_START': '2024-10-01',
    'PROCESSED_END': '2025-12-31',
}

# Docker configuration
docker_config = {
    'image': 'pyspark-app:latest',
    'api_version': 'auto',
    'auto_remove': True,
    'docker_url': 'unix://var/run/docker.sock',
    'network_mode': 'bridge',
    'environment': mysql_env,
}

# Task 1: Process raw CDR data into features
process_data_task = DockerOperator(
    task_id='process_raw_data',
    command='python /app/PySparkAnalysis.py',
    dag=dag,
    **docker_config
)

# Task 2: Train ML model on processed data
train_model_task = DockerOperator(
    task_id='train_model',
    command='python /app/pySparkModel.py',
    dag=dag,
    **docker_config
)

# Task 3: (Optional) Run EDA
eda_task = DockerOperator(
    task_id='run_eda',
    command='python /app/pySparkEDA.py',
    dag=dag,
    trigger_rule='none_failed_or_skipped',  # Run even if other tasks skip
    **docker_config
)

# Set task dependencies
# Process data must complete before training model
process_data_task >> train_model_task

# EDA can run in parallel with model training after data processing
process_data_task >> eda_task