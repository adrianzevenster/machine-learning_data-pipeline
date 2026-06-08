"""
A/B test analysis: compare champion vs challenger accuracy on labelled serving predictions.

Reads serving_predictions WHERE actual_churn IS NOT NULL, computes per-variant
accuracy / precision / recall / AUC, writes a JSON report, and sends a Slack
summary when the challenger shows meaningful lift (or meaningful regression).
"""
import json
import os
import sys
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

import mysql.connector

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_USER = os.getenv("MYSQL_USER", "spark")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "sparkpw")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "RawData")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
AB_MIN_SAMPLE_SIZE = int(os.getenv("AB_MIN_SAMPLE_SIZE", "30"))
AB_LIFT_ALERT_THRESHOLD = float(os.getenv("AB_LIFT_ALERT_THRESHOLD", "0.02"))
OUTPUT_DIR = Path(os.getenv("MONITORING_OUTPUT_DIR", "/app/output/monitoring"))


def mysql_config() -> dict:
    return {
        "host": MYSQL_HOST,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "database": MYSQL_DATABASE,
    }


def fetch_aggregate_metrics(cursor) -> List[dict]:
    cursor.execute(
        """
        SELECT
            model_variant,
            COUNT(*)                                                           AS total,
            SUM(CASE WHEN prediction = actual_churn THEN 1 ELSE 0 END)        AS correct,
            AVG(CASE WHEN prediction = actual_churn THEN 1.0 ELSE 0.0 END)    AS accuracy,
            SUM(CASE WHEN prediction=1 AND actual_churn=1 THEN 1 ELSE 0 END)  AS tp,
            SUM(CASE WHEN prediction=1 AND actual_churn=0 THEN 1 ELSE 0 END)  AS fp,
            SUM(CASE WHEN prediction=0 AND actual_churn=1 THEN 1 ELSE 0 END)  AS fn
        FROM   serving_predictions
        WHERE  actual_churn IS NOT NULL
        GROUP  BY model_variant
        ORDER  BY model_variant
        """
    )
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def fetch_raw_scores(cursor, variant: str) -> tuple:
    """Return (y_true, y_score) arrays for AUC computation."""
    cursor.execute(
        """
        SELECT actual_churn, probability_churn
        FROM   serving_predictions
        WHERE  actual_churn IS NOT NULL
          AND  model_variant = %s
        """,
        (variant,),
    )
    rows = cursor.fetchall()
    y_true = [r[0] for r in rows]
    y_score = [float(r[1]) for r in rows]
    return y_true, y_score


def compute_auc(y_true: list, y_score: list) -> Optional[float]:
    try:
        from sklearn.metrics import roc_auc_score
        if len(set(y_true)) < 2:
            return None
        return float(roc_auc_score(y_true, y_score))
    except Exception:
        return None


def enrich_metrics(row: dict, y_true: list, y_score: list) -> dict:
    tp = row.get("tp", 0) or 0
    fp = row.get("fp", 0) or 0
    fn = row.get("fn", 0) or 0
    precision = tp / (tp + fp) if (tp + fp) > 0 else None
    recall = tp / (tp + fn) if (tp + fn) > 0 else None
    f1 = (2 * precision * recall / (precision + recall)
          if precision and recall else None)
    return {
        **{k: float(v) if isinstance(v, (int, float)) and v is not None else v
           for k, v in row.items()},
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "auc": compute_auc(y_true, y_score),
    }


def make_recommendation(champion: dict, challenger: dict) -> str:
    c_acc = champion.get("accuracy") or 0
    ch_acc = challenger.get("accuracy") or 0
    lift = ch_acc - c_acc
    if abs(lift) < AB_LIFT_ALERT_THRESHOLD:
        return "no_significant_difference"
    return "promote_challenger" if lift > 0 else "keep_champion"


def send_slack(champion: dict, challenger: dict, recommendation: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    lift = (challenger.get("accuracy") or 0) - (champion.get("accuracy") or 0)
    emoji = ":white_check_mark:" if recommendation == "promote_challenger" else ":warning:"
    msg = (
        f"{emoji} *A/B analysis complete*\n"
        f"Champion  — accuracy: {champion.get('accuracy', 0):.3f}  "
        f"AUC: {champion.get('auc') or 'n/a'}  n={int(champion.get('total', 0))}\n"
        f"Challenger — accuracy: {challenger.get('accuracy', 0):.3f}  "
        f"AUC: {challenger.get('auc') or 'n/a'}  n={int(challenger.get('total', 0))}\n"
        f"Accuracy lift: {lift:+.3f}  →  *{recommendation}*"
    )
    try:
        payload = json.dumps({"text": msg}).encode()
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:
        print(f"[ab-analysis] Slack notification failed: {exc}")


def run_analysis() -> dict:
    with mysql.connector.connect(**mysql_config()) as conn:
        cursor = conn.cursor()
        rows = fetch_aggregate_metrics(cursor)

        if not rows:
            print("[ab-analysis] No labelled serving predictions yet — nothing to analyse.")
            return {}

        metrics: Dict[str, dict] = {}
        for row in rows:
            variant = row["model_variant"]
            y_true, y_score = fetch_raw_scores(cursor, variant)
            metrics[variant] = enrich_metrics(row, y_true, y_score)

    champion = metrics.get("champion", {})
    challenger = metrics.get("challenger", {})

    for variant, m in metrics.items():
        n = int(m.get("total", 0))
        if n < AB_MIN_SAMPLE_SIZE:
            print(
                f"[ab-analysis] {variant} has only {n} labelled predictions "
                f"(min={AB_MIN_SAMPLE_SIZE}). Skipping recommendation."
            )
            metrics[variant]["recommendation"] = "insufficient_sample_size"

    recommendation = "insufficient_sample_size"
    if (champion.get("total", 0) >= AB_MIN_SAMPLE_SIZE
            and challenger.get("total", 0) >= AB_MIN_SAMPLE_SIZE):
        recommendation = make_recommendation(champion, challenger)
        if recommendation != "no_significant_difference":
            send_slack(champion, challenger, recommendation)

    report = {
        "variants": metrics,
        "recommendation": recommendation,
        "min_sample_size": AB_MIN_SAMPLE_SIZE,
        "lift_alert_threshold": AB_LIFT_ALERT_THRESHOLD,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR / "ab_analysis_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"[ab-analysis] Report written to {report_path}")
    print(f"[ab-analysis] Recommendation: {recommendation}")
    for variant, m in metrics.items():
        print(
            f"[ab-analysis]   {variant}: accuracy={m.get('accuracy', 0):.3f}  "
            f"auc={m.get('auc') or 'n/a'}  n={int(m.get('total', 0))}"
        )

    return report


if __name__ == "__main__":
    try:
        run_analysis()
    except Exception as exc:
        print(f"[ab-analysis] FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
