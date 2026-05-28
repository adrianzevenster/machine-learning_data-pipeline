FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pandas \
    mysql-connector-python \
    nannyml \
    plotly \
    kaleido

COPY model_monitoring/Model_Monitoring.py /app/Model_Monitoring.py
COPY quality /app/quality

CMD ["python", "/app/Model_Monitoring.py"]
