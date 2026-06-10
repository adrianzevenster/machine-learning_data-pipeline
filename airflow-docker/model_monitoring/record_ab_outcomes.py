"""
Backfills serving_predictions.actual_churn by joining on DP_CDR_Data.

Run this after each pipeline cycle so that any serving predictions whose
subscribers now have CDR records get their actual churn outcome filled in.
Records with no MSISDN or no matching CDR row are left as NULL.
"""
import os
import sys

import mysql.connector

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_USER = os.getenv("MYSQL_USER", "spark")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "sparkpw")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "RawData")


def mysql_config() -> dict:
    return {
        "host": MYSQL_HOST,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "database": MYSQL_DATABASE,
    }


def record_outcomes() -> int:
    with mysql.connector.connect(**mysql_config()) as conn:
        cursor = conn.cursor()

        # Take the most-recent PSEUDO_CHURNED value per subscriber as ground truth.
        # This is conservative: a subscriber who ever appeared as churned counts as churned.
        cursor.execute(
            """
            UPDATE serving_predictions sp
            INNER JOIN (
                SELECT DP_MSISDN,
                       MAX(PSEUDO_CHURNED) AS actual_churn
                FROM   DP_CDR_Data
                GROUP  BY DP_MSISDN
            ) cdr ON cdr.DP_MSISDN = sp.msisdn
            SET sp.actual_churn        = cdr.actual_churn,
                sp.outcome_recorded_at = NOW()
            WHERE sp.msisdn       IS NOT NULL
              AND sp.actual_churn IS NULL
            """
        )
        updated = cursor.rowcount
        conn.commit()

    print(f"[ab-outcomes] Backfilled outcomes for {updated} serving predictions.")
    return updated


if __name__ == "__main__":
    try:
        record_outcomes()
    except Exception as exc:
        print(f"[ab-outcomes] FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
