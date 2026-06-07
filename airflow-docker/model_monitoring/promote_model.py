import json
import os
from datetime import datetime

import mlflow
import mysql.connector
from mlflow.tracking import MlflowClient


PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID")
MODEL_VERSION_ID = os.getenv("MODEL_VERSION_ID")
MODEL_NAME = os.getenv("MLFLOW_REGISTERED_MODEL_NAME", os.getenv("MODEL_NAME", "customer_churn_random_forest"))
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_REGISTRY_URI = os.getenv("MLFLOW_REGISTRY_URI", MLFLOW_TRACKING_URI)
PROMOTION_STAGE = os.getenv("MODEL_PROMOTION_STAGE", "Production")
MIN_MODEL_AUC = float(os.getenv("MIN_MODEL_AUC", "0.5"))
MIN_PREDICTION_ROWS = int(os.getenv("MIN_PROMOTION_PREDICTION_ROWS", "1"))


def mysql_config():
    return {
        "host": os.getenv("MYSQL_HOST", "mysql"),
        "user": os.getenv("MYSQL_USER", "spark"),
        "password": os.getenv("MYSQL_PASSWORD", "sparkpw"),
        "database": os.getenv("MYSQL_DATABASE", "RawData"),
    }


def ensure_promotion_schema(cursor):
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'model_versions'
        """
    )
    existing_columns = {row[0] for row in cursor.fetchall()}
    required_columns = {
        "promotion_status": "VARCHAR(32) NOT NULL DEFAULT 'candidate'",
        "promotion_stage": "VARCHAR(32) NOT NULL DEFAULT 'None'",
        "promotion_reason": "TEXT",
        "promoted_at": "TIMESTAMP NULL",
    }
    for column_name, column_definition in required_columns.items():
        if column_name not in existing_columns:
            cursor.execute(f"ALTER TABLE model_versions ADD COLUMN {column_name} {column_definition}")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS model_deployments (
            model_name VARCHAR(128) NOT NULL,
            stage VARCHAR(32) NOT NULL,
            model_version_id VARCHAR(250) NOT NULL,
            mlflow_run_id VARCHAR(250) NOT NULL,
            mlflow_model_uri VARCHAR(512) NOT NULL,
            promoted_by_run_id VARCHAR(250) NOT NULL,
            promoted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (model_name, stage),
            INDEX idx_model_deployments_model_version (model_version_id),
            INDEX idx_model_deployments_run (mlflow_run_id)
        )
        """
    )


def fetch_candidate(cursor):
    cursor.execute(
        """
        SELECT
            mv.model_version_id,
            mv.run_id,
            mv.model_name,
            mv.metrics_json,
            mv.mlflow_run_id,
            mv.mlflow_model_uri,
            pr.prediction_row_count
        FROM model_versions mv
        JOIN pipeline_runs pr
          ON pr.run_id = mv.run_id
        WHERE mv.model_version_id = %s
          AND mv.run_id = %s
        """,
        (MODEL_VERSION_ID, PIPELINE_RUN_ID),
    )
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(f"No model_versions candidate found for run={PIPELINE_RUN_ID} model={MODEL_VERSION_ID}.")

    columns = (
        "model_version_id",
        "run_id",
        "model_name",
        "metrics_json",
        "mlflow_run_id",
        "mlflow_model_uri",
        "prediction_row_count",
    )
    return dict(zip(columns, row))


def parse_auc(metrics_json):
    if not metrics_json:
        return None
    metrics = json.loads(metrics_json)
    auc = metrics.get("auc")
    return float(auc) if auc is not None else None


def reject_candidate(cursor, reason):
    cursor.execute(
        """
        UPDATE model_versions
        SET promotion_status = 'rejected',
            promotion_stage = 'None',
            promotion_reason = %s,
            promoted_at = NULL
        WHERE model_version_id = %s
        """,
        (reason, MODEL_VERSION_ID),
    )
    cursor.execute(
        """
        UPDATE pipeline_runs
        SET status = 'promotion_rejected'
        WHERE run_id = %s
        """,
        (PIPELINE_RUN_ID,),
    )


def find_mlflow_model_version(client, mlflow_run_id):
    versions = client.search_model_versions(f"name = '{MODEL_NAME}'")
    for version in versions:
        if version.run_id == mlflow_run_id:
            return version
    raise RuntimeError(f"No MLflow registered model version found for {MODEL_NAME} run_id={mlflow_run_id}.")


def promote_candidate(cursor, candidate, mlflow_model_version):
    promoted_at = datetime.utcnow()
    mlflow_model_uri = candidate["mlflow_model_uri"] or f"models:/{MODEL_NAME}/{PROMOTION_STAGE}"

    cursor.execute(
        """
        UPDATE model_versions
        SET promotion_status = 'promoted',
            promotion_stage = %s,
            promotion_reason = %s,
            promoted_at = %s
        WHERE model_version_id = %s
        """,
        (
            PROMOTION_STAGE,
            f"Passed promotion thresholds: auc>={MIN_MODEL_AUC}, prediction_rows>={MIN_PREDICTION_ROWS}",
            promoted_at,
            MODEL_VERSION_ID,
        ),
    )
    cursor.execute(
        """
        INSERT INTO model_deployments (
            model_name, stage, model_version_id, mlflow_run_id,
            mlflow_model_uri, promoted_by_run_id, promoted_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            model_version_id = VALUES(model_version_id),
            mlflow_run_id = VALUES(mlflow_run_id),
            mlflow_model_uri = VALUES(mlflow_model_uri),
            promoted_by_run_id = VALUES(promoted_by_run_id),
            promoted_at = VALUES(promoted_at)
        """,
        (
            MODEL_NAME,
            PROMOTION_STAGE,
            MODEL_VERSION_ID,
            candidate["mlflow_run_id"],
            mlflow_model_uri,
            PIPELINE_RUN_ID,
            promoted_at,
        ),
    )
    cursor.execute(
        """
        UPDATE pipeline_runs
        SET status = 'model_promoted'
        WHERE run_id = %s
        """,
        (PIPELINE_RUN_ID,),
    )
    print(
        "[promotion] promoted "
        f"model_version_id={MODEL_VERSION_ID} mlflow_version={mlflow_model_version.version} stage={PROMOTION_STAGE}"
    )


def run_promotion():
    if not PIPELINE_RUN_ID or not MODEL_VERSION_ID:
        raise RuntimeError("PIPELINE_RUN_ID and MODEL_VERSION_ID must be set.")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_registry_uri(MLFLOW_REGISTRY_URI)
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI, registry_uri=MLFLOW_REGISTRY_URI)

    with mysql.connector.connect(**mysql_config()) as connection:
        cursor = connection.cursor()
        ensure_promotion_schema(cursor)
        candidate = fetch_candidate(cursor)

        auc = parse_auc(candidate["metrics_json"])
        prediction_rows = candidate["prediction_row_count"] or 0
        rejection_reasons = []
        if auc is None:
            rejection_reasons.append("AUC metric is missing")
        elif auc < MIN_MODEL_AUC:
            rejection_reasons.append(f"AUC {auc:.4f} is below threshold {MIN_MODEL_AUC:.4f}")
        if prediction_rows < MIN_PREDICTION_ROWS:
            rejection_reasons.append(
                f"prediction rows {prediction_rows} is below threshold {MIN_PREDICTION_ROWS}"
            )
        if not candidate["mlflow_run_id"]:
            rejection_reasons.append("mlflow_run_id is missing")

        if rejection_reasons:
            reason = "; ".join(rejection_reasons)
            reject_candidate(cursor, reason)
            connection.commit()
            raise RuntimeError(f"Model promotion rejected: {reason}")

        mlflow_model_version = find_mlflow_model_version(client, candidate["mlflow_run_id"])
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=mlflow_model_version.version,
            stage=PROMOTION_STAGE,
            archive_existing_versions=True,
        )
        client.set_model_version_tag(
            name=MODEL_NAME,
            version=mlflow_model_version.version,
            key="pipeline_run_id",
            value=PIPELINE_RUN_ID,
        )
        client.set_model_version_tag(
            name=MODEL_NAME,
            version=mlflow_model_version.version,
            key="model_version_id",
            value=MODEL_VERSION_ID,
        )
        promote_candidate(cursor, candidate, mlflow_model_version)
        connection.commit()


if __name__ == "__main__":
    run_promotion()
