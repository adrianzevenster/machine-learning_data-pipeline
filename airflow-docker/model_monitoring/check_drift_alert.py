import base64
import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

MONITORING_OUTPUT_DIR = os.getenv("MONITORING_OUTPUT_DIR", "/app/output/monitoring")
PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID")
DRIFT_ALERT_MIN_ROC_AUC = float(os.getenv("DRIFT_ALERT_MIN_ROC_AUC", "0.6"))
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

AUTO_RETRAIN_ON_DRIFT = os.getenv("AUTO_RETRAIN_ON_DRIFT", "false").lower() == "true"
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080")
AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "local_dev_pipeline")
AIRFLOW_API_USERNAME = os.getenv("AIRFLOW_ADMIN_USERNAME", "airflow")
AIRFLOW_API_PASSWORD = os.getenv("AIRFLOW_ADMIN_PASSWORD", "airflow")


def find_report_csv() -> Path:
    base = Path(MONITORING_OUTPUT_DIR)
    if PIPELINE_RUN_ID:
        candidate = base / PIPELINE_RUN_ID / "performance_report.csv"
        if candidate.exists():
            return candidate
    csv_files = sorted(base.rglob("performance_report.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No performance_report.csv found under {base}")
    return csv_files[-1]


def check_drift(csv_path: Path) -> tuple:
    df = pd.read_csv(csv_path, header=[0, 1])
    df.columns = ["_".join(str(p) for p in col).strip("_") for col in df.columns]

    period_col = next((c for c in df.columns if c.endswith("_period")), None)
    analysis = df[df[period_col] != "reference"] if period_col else df
    if analysis.empty:
        analysis = df

    triggered = False
    reasons = []

    alert_col = next((c for c in analysis.columns if c.startswith("roc_auc") and c.endswith("_alert")), None)
    if alert_col:
        alerts = analysis[alert_col].astype(str).str.lower() == "true"
        if alerts.any():
            triggered = True
            reasons.append(f"NannyML flagged roc_auc alert in {int(alerts.sum())} chunk(s)")

    value_col = next((c for c in analysis.columns if c.startswith("roc_auc") and c.endswith("_value")), None)
    if value_col:
        values = pd.to_numeric(analysis[value_col], errors="coerce").dropna()
        if len(values):
            mean_auc = float(values.mean())
            if mean_auc < DRIFT_ALERT_MIN_ROC_AUC:
                triggered = True
                reasons.append(f"mean roc_auc {mean_auc:.3f} < threshold {DRIFT_ALERT_MIN_ROC_AUC}")

    return triggered, "; ".join(reasons) if reasons else "no drift detected"


def send_slack_alert(message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        print("[drift-alert] SLACK_WEBHOOK_URL not set; skipping Slack notification.")
        return
    payload = json.dumps({"text": message}).encode()
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        if resp.status != 200:
            print(f"[drift-alert] Slack webhook returned HTTP {resp.status}")


def trigger_retraining(reason: str) -> None:
    if not AUTO_RETRAIN_ON_DRIFT:
        print("[drift-alert] AUTO_RETRAIN_ON_DRIFT=false; skipping automatic retrain.")
        return

    credentials = base64.b64encode(
        f"{AIRFLOW_API_USERNAME}:{AIRFLOW_API_PASSWORD}".encode()
    ).decode()

    payload = json.dumps(
        {
            "logical_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "note": f"Auto-triggered by drift alert: {reason}",
        }
    ).encode()

    url = f"{AIRFLOW_API_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {credentials}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            run_id = body.get("dag_run_id", "unknown")
            print(f"[drift-alert] Triggered retrain DAG run: {run_id}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        print(f"[drift-alert] Airflow API returned HTTP {exc.code}: {body}")
    except Exception as exc:
        print(f"[drift-alert] Failed to trigger retrain: {exc}")


def main() -> None:
    try:
        csv_path = find_report_csv()
    except FileNotFoundError as exc:
        print(f"[drift-alert] {exc}; skipping.")
        return

    triggered, reason = check_drift(csv_path)
    run_label = PIPELINE_RUN_ID or "unknown"

    if triggered:
        print(f"[drift-alert] ALERT (run={run_label}): {reason}")
        send_slack_alert(
            f":rotating_light: *Model drift detected* (run={run_label})\n{reason}"
        )
        trigger_retraining(reason)
    else:
        print(f"[drift-alert] OK (run={run_label}): {reason}")


if __name__ == "__main__":
    main()
