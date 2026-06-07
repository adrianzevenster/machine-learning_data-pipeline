FROM python:3.11-slim

RUN python -m pip install --no-cache-dir mlflow==2.14.3

RUN mkdir -p /mlflow/backend /mlflow/artifacts

EXPOSE 5000

CMD ["mlflow", "server", "--backend-store-uri", "sqlite:////mlflow/backend/mlflow.db", "--default-artifact-root", "/mlflow/artifacts", "--host", "0.0.0.0", "--port", "5000", "--serve-artifacts"]
