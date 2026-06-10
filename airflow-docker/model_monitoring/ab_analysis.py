"""
A/B test analysis: compare champion vs challenger accuracy on labelled serving predictions.

Reads serving_predictions WHERE actual_churn IS NOT NULL AND the prediction is
older than LABEL_DELAY_DAYS (settled labels only).  Computes per-variant
accuracy / precision / recall / AUC, runs a two-proportion z-test for statistical
significance, writes a JSON report, and sends a Slack summary.
"""
import json
import math
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
AB_MIN_SAMPLE_SIZE = int(os.getenv("AB_MIN_SAMPLE_SIZE", "200"))
AB_LIFT_ALERT_THRESHOLD = float(os.getenv("AB_LIFT_ALERT_THRESHOLD", "0.02"))
AB_SIGNIFICANCE_ALPHA = float(os.getenv("AB_SIGNIFICANCE_ALPHA", "0.05"))
LABEL_DELAY_DAYS = int(os.getenv("LABEL_DELAY_DAYS", "30"))
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
          AND  served_at <= NOW() - INTERVAL %s DAY
        GROUP  BY model_variant
        ORDER  BY model_variant
        """,
        (LABEL_DELAY_DAYS,),
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
          AND  served_at <= NOW() - INTERVAL %s DAY
          AND  model_variant = %s
        """,
        (LABEL_DELAY_DAYS, variant),
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


def _norm_cdf(z: float) -> float:
    """Standard normal CDF via math.erfc (no scipy dependency)."""
    return 0.5 * math.erfc(-z / math.sqrt(2))


def two_proportion_z_test(n1: int, correct1: int, n2: int, correct2: int) -> dict:
    """Two-sided two-proportion z-test: H0 = champion and challenger have equal accuracy."""
    p1 = correct1 / n1 if n1 > 0 else 0.0
    p2 = correct2 / n2 if n2 > 0 else 0.0
    p_pool = (correct1 + correct2) / (n1 + n2) if (n1 + n2) > 0 else 0.0
    se = math.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2)) if (n1 > 0 and n2 > 0) else 0.0
    if se == 0:
        return {"z_stat": 0.0, "p_value": 1.0, "significant": False}
    z = (p2 - p1) / se
    p_value = 2 * (1 - _norm_cdf(abs(z)))
    return {
        "z_stat": round(z, 4),
        "p_value": round(p_value, 6),
        "significant": p_value < AB_SIGNIFICANCE_ALPHA,
    }


def make_recommendation(champion: dict, challenger: dict) -> tuple:
    """Return (recommendation, significance_test) based on z-test and lift threshold."""
    n_champ = int(champion.get("total", 0) or 0)
    n_chal = int(challenger.get("total", 0) or 0)
    correct_champ = int(champion.get("correct", 0) or 0)
    correct_chal = int(challenger.get("correct", 0) or 0)

    sig = two_proportion_z_test(n_champ, correct_champ, n_chal, correct_chal)
    lift = (challenger.get("accuracy") or 0) - (champion.get("accuracy") or 0)

    if not sig["significant"] or abs(lift) < AB_LIFT_ALERT_THRESHOLD:
        recommendation = "no_significant_difference"
    else:
        recommendation = "promote_challenger" if lift > 0 else "keep_champion"
    return recommendation, sig


def send_slack(champion: dict, challenger: dict, recommendation: str, sig: dict) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    lift = (challenger.get("accuracy") or 0) - (champion.get("accuracy") or 0)
    emoji = ":white_check_mark:" if recommendation == "promote_challenger" else ":warning:"
    msg = (
        f"{emoji} *A/B analysis complete*\n"
        f"Champion   — accuracy: {champion.get('accuracy', 0):.3f}  "
        f"AUC: {champion.get('auc') or 'n/a'}  n={int(champion.get('total', 0))}\n"
        f"Challenger — accuracy: {challenger.get('accuracy', 0):.3f}  "
        f"AUC: {challenger.get('auc') or 'n/a'}  n={int(challenger.get('total', 0))}\n"
        f"Accuracy lift: {lift:+.3f}  z={sig.get('z_stat', 'n/a')}  "
        f"p={sig.get('p_value', 'n/a')}  significant={sig.get('significant')}  "
        f"→  *{recommendation}*"
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
    significance_test: dict = {}
    if (champion.get("total", 0) >= AB_MIN_SAMPLE_SIZE
            and challenger.get("total", 0) >= AB_MIN_SAMPLE_SIZE):
        recommendation, significance_test = make_recommendation(champion, challenger)
        if recommendation != "no_significant_difference":
            send_slack(champion, challenger, recommendation, significance_test)

    report = {
        "variants": metrics,
        "recommendation": recommendation,
        "significance_test": significance_test,
        "min_sample_size": AB_MIN_SAMPLE_SIZE,
        "label_delay_days": LABEL_DELAY_DAYS,
        "significance_alpha": AB_SIGNIFICANCE_ALPHA,
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
