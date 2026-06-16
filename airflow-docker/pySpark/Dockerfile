FROM apache/spark:3.5.1
USER root

# Python & pip (you already added these)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

ENV PYSPARK_PYTHON=python3

WORKDIR /app
COPY requirements.txt /app/

RUN python3 -m pip install --no-cache-dir -r requirements.txt

# MySQL JDBC driver
RUN curl -fsSL -o /opt/spark/jars/mysql-connector-java-8.0.25.jar \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.25/mysql-connector-java-8.0.25.jar && \
    ln -s /opt/spark/jars/mysql-connector-java-8.0.25.jar /app/mysql-connector-java-8.0.25.jar

# Kafka connector JARs for Spark Structured Streaming
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.12.0.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

COPY PySparkAnalysis.py /app/
COPY pySparkModel.py /app/
COPY model_card.py /app/
COPY spark_streaming_consumer.py /app/

CMD ["/bin/sh", "-lc", "python3 /app/PySparkAnalysis.py"]
