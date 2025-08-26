# Airflow image (no PySpark files here!)
FROM apache/airflow:2.7.1

USER root
RUN apt-get update && apt-get install -y postgresql-client && apt-get clean
USER airflow

WORKDIR /opt/airflow
