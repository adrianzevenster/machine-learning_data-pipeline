# pyspark-app/Dockerfile
FROM python:3.8-slim

RUN apt-get update && apt-get install -y default-jdk curl && rm -rf /var/lib/apt/lists/*
RUN pip install pyspark pandas seaborn matplotlib mysql-connector-python

# JDBC driver
RUN mkdir -p /opt/spark/jars
ADD https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.25/mysql-connector-java-8.0.25.jar /opt/spark/jars/

WORKDIR /app
COPY PySparkAnalysis.py /app/PySparkAnalysis.py
COPY pySparkModel.py    /app/pySparkModel.py

CMD ["python", "/app/PySparkAnalysis.py"]
