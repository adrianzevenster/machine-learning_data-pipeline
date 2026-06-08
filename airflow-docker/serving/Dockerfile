FROM apache/spark:3.5.1
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python
ENV PYSPARK_PYTHON=python3

WORKDIR /app
COPY requirements.txt /app/

RUN python3 -m pip install --no-cache-dir -r requirements.txt

RUN curl -fsSL -o /opt/spark/jars/mysql-connector-java-8.0.25.jar \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.25/mysql-connector-java-8.0.25.jar

COPY main.py /app/

EXPOSE 8000

CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
