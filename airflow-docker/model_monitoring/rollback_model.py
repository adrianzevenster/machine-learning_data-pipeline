import os
import sys
from datetime import datetime

import mlflow
import mysql.connector
from mlflow.tracking import MlflowClient

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_REGISTRY_URI = os.getenv("MLFLOW_REGISTRY_URI", MLFLOW_TRACKING_URI)
MODEL_NAME = os.getenv("MLFLOW_REGISTERED_MODEL_NAME", "customer_churn_random_forest")
MODEL_PROMOTION_STAGE = os.getenv("MODEL_PROMOTION_STAGE", "Production")
ROLLBACK_TO_VERSION = os.getenv("ROLLBACK_TO_VERSION", "")
SERVING_RELOAD_URL = os.getenv("SERVING_RELOAD_URL", "")
PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID", "manual-rollback")


def mysql_config():
    return {
        "host": os.getenv("MYSQL_HOST", "mysql"),
        "user": os.getenv("MYSQL_USER", "spark"),
        "password": os.getenv("MYSQL_PASSWORD", "sparkpw"),
        "database": os.getenv("MYSQL_DATABASE", "RawData"),
    }


def find_current_production(client):
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    prod = [v for v in versions if v.current_stage == MODEL_PROMOTION_STAGE]
    if not prod:
        raise RuntimeError(f"No model currently in '{MODEL_PROMOTION_STAGE}' stage for '{MODEL_NAME}'.")
    return max(prod, key=lambda v: int(v.version))


def find_rollback_target(client, current_version_num: int):
    if ROLLBACK_TO_VERSION:
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        target = next((v for v in versions if v.version == ROLLBACK_TO_VERSION), None)
        if not target:
            raise RuntimeError(f"ROLLBACK_TO_VERSION={ROLLBACK_TO_VERSION} not found in MLflow registry.")
        if int(target.version) >= current_version_num:
            raise RuntimeError(
                f"ROLLBACK_TO_VERSION={ROLLBACK_TO_VERSION} is not older than the current "
                f"Production v{current_version_num}. Refusing rollback."
            )
        return target

    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    candidates = [
        v for v in versions
        if v.current_stage == "Archived" and int(v.version) < current_version_num
    ]
    if not candidates:
        raise RuntimeError(
            "No archived model versions available to roll back to. "
            "At least one prior version must exist in 'Archived' stage."
        )
    return max(candidates, key=lambda v: int(v.version))


def record_rollback(cursor, target):
    cursor.execute(
        """
        INSERT INTO model_deployments (
            model_name, stage, model_version_id, mlflow_run_id,
            mlflow_model_uri, promoted_by_run_id, promoted_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            model_version_id = VALUES(model_version_id),
            mlflow_run_id    = VALUES(mlflow_run_id),
            mlflow_model_uri = VALUES(mlflow_model_uri),
            promoted_by_run_id = VALUES(promoted_by_run_id),
            promoted_at      = VALUES(promoted_at)
        """,
        (
            MODEL_NAME,
            MODEL_PROMOTION_STAGE,
            f"rollback_to_v{target.version}",
            target.run_id,
            f"models:/{MODEL_NAME}/{MODEL_PROMOTION_STAGE}",
            PIPELINE_RUN_ID,
            datetime.utcnow(),
        ),
    )


def trigger_serving_reload():
    if not SERVING_RELOAD_URL:
        return
    import urllib.request
    try:
        req = urllib.request.Request(SERVING_RELOAD_URL, method="POST", data=b"")
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"[rollback] Serving reload triggered: HTTP {resp.status}")
    except Exception as exc:
        print(f"[rollback] Warning: serving reload failed: {exc}")


def run_rollback():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_registry_uri(MLFLOW_REGISTRY_URI)
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI, registry_uri=MLFLOW_REGISTRY_URI)

    current = find_current_production(client)
    current_version_num = int(current.version)
    print(f"[rollback] Current Production: v{current.version} (run_id={current.run_id})")

    target = find_rollback_target(client, current_version_num)
    print(f"[rollback] Target: v{target.version} (stage={target.current_stage}, run_id={target.run_id})")

    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=current.version,
        stage="Archived",
        archive_existing_versions=False,
    )
    print(f"[rollback] v{current.version} -> Archived")

    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=target.version,
        stage=MODEL_PROMOTION_STAGE,
        archive_existing_versions=False,
    )
    print(f"[rollback] v{target.version} -> {MODEL_PROMOTION_STAGE}")

    try:
        with mysql.connector.connect(**mysql_config()) as conn:
            cursor = conn.cursor()
            record_rollback(cursor, target)
            conn.commit()
        print("[rollback] model_deployments updated")
    except Exception as exc:
        print(f"[rollback] Warning: MySQL update failed: {exc}")

    trigger_serving_reload()
    print(f"[rollback] Done — now serving v{target.version} in {MODEL_PROMOTION_STAGE}")


if __name__ == "__main__":
    try:
        run_rollback()
    except Exception as exc:
        print(f"[rollback] FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
