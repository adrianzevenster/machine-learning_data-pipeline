FROM apache/spark:3.5.1
USER root

# Python & pip (you already added these)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

ENV PYSPARK_PYTHON=python3

WORKDIR /app
COPY pySpark/PySparkAnalysis.py /app/
COPY pySpark/pySparkModel.py /app/

# your Python deps (keep what you had)
RUN python3 -m pip install --no-cache-dir pandas mysql-connector-python

RUN python3 -m pip install --no-cache-dir pyspark==3.5.1

# ▶ JDBC driver jar (match what your logs expect)
RUN curl -fsSL -o /opt/spark/jars/mysql-connector-java-8.0.25.jar \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.25/mysql-connector-java-8.0.25.jar && \
    ln -s /opt/spark/jars/mysql-connector-java-8.0.25.jar /app/mysql-connector-java-8.0.25.jar

# (Airflow overrides the command; harmless default)
CMD ["/bin/sh", "-lc", "python3 /app/PySparkAnalysis.py"]
