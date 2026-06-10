FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY model_monitoring/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY model_monitoring/Model_Monitoring.py /app/Model_Monitoring.py
COPY model_monitoring/promote_model.py /app/promote_model.py
COPY model_monitoring/check_drift_alert.py /app/check_drift_alert.py
COPY model_monitoring/feature_drift.py /app/feature_drift.py
COPY model_monitoring/rollback_model.py /app/rollback_model.py
COPY model_monitoring/record_ab_outcomes.py /app/record_ab_outcomes.py
COPY model_monitoring/ab_analysis.py /app/ab_analysis.py
COPY quality /app/quality
COPY mysql/migrations /app/mysql/migrations

CMD ["python", "/app/Model_Monitoring.py"]
