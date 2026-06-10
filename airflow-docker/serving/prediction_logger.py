"""
Async MySQL writer for live serving predictions.

Fires a daemon thread per call so the serving endpoint is never blocked.
All failures are logged and swallowed — prediction logging must never
affect the latency or availability of the serving endpoint.
"""
import logging
import os
import threading
import uuid
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_USER = os.getenv("MYSQL_USER", "spark")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "sparkpw")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "RawData")
PREDICTION_LOGGING_ENABLED = os.getenv("PREDICTION_LOGGING_ENABLED", "true").lower() == "true"

try:
    import mysql.connector as _mysql
    _MYSQL_AVAILABLE = True
except ImportError:
    _mysql = None
    _MYSQL_AVAILABLE = False


def _write(
    request_id: str,
    msisdn: Optional[str],
    model_name: str,
    model_stage: str,
    model_variant: str,
    prediction: int,
    probability_churn: float,
    probability_retain: float,
) -> None:
    if not _MYSQL_AVAILABLE:
        return
    try:
        conn = _mysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            connection_timeout=3,
        )
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO serving_predictions (
                request_id, msisdn, model_name, model_stage, model_variant,
                prediction, probability_churn, probability_retain, served_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                request_id,
                msisdn,
                model_name,
                model_stage,
                model_variant,
                prediction,
                probability_churn,
                probability_retain,
                datetime.utcnow(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("Prediction logging to MySQL failed (non-fatal): %s", exc)


def log_prediction(
    msisdn: Optional[str],
    model_name: str,
    model_stage: str,
    model_variant: str,
    prediction: int,
    probability_churn: float,
    probability_retain: float,
) -> None:
    if not PREDICTION_LOGGING_ENABLED:
        return
    request_id = str(uuid.uuid4())
    threading.Thread(
        target=_write,
        args=(request_id, msisdn, model_name, model_stage, model_variant,
              prediction, probability_churn, probability_retain),
        daemon=True,
    ).start()
