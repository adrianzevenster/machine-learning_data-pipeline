import os
from pathlib import Path

import mysql.connector
import nannyml as nml
import pandas as pd

PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID")
MODEL_VERSION_ID = os.getenv("MODEL_VERSION_ID")

FEATURE_COLS = [
    "M_Out_Call_Count",
    "M_Out_Call_Time",
    "M_Data_Sum",
    "M_Data_Count",
    "M_In_Call_Count",
    "M_In_Call_Time",
]


def mysql_config() -> dict:
    return {
        "host": os.getenv("MYSQL_HOST", "mysql"),
        "user": os.getenv("MYSQL_USER", "spark"),
        "password": os.getenv("MYSQL_PASSWORD", "sparkpw"),
        "database": os.getenv("MYSQL_DATABASE", "RawData"),
    }


def load_features() -> pd.DataFrame:
    cols = ", ".join(f"`{c}`" for c in FEATURE_COLS)
    query = f"SELECT Date, {cols} FROM Processed_Data WHERE Date IS NOT NULL ORDER BY Date"
    with mysql.connector.connect(**mysql_config()) as conn:
        df = pd.read_sql(query, conn)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    return df


def split_reference_analysis(df: pd.DataFrame) -> tuple:
    split_idx = int(len(df) * 0.5)
    if split_idx < 1 or split_idx >= len(df):
        raise ValueError(f"Not enough rows to split ({len(df)} rows).")
    return df.iloc[:split_idx].copy(), df.iloc[split_idx:].copy()


def check_alerts(results_df: pd.DataFrame) -> list:
    flat = results_df.copy()
    flat.columns = ["_".join(str(p) for p in col).strip("_") for col in flat.columns]

    period_col = next((c for c in flat.columns if c.endswith("_period")), None)
    analysis = flat[flat[period_col] != "reference"] if period_col else flat

    alert_cols = [
        c for c in analysis.columns
        if c.endswith("_alert") and not c.startswith("chunk")
    ]
    triggered = []
    for col in alert_cols:
        alerts = analysis[col].astype(str).str.lower() == "true"
        if alerts.any():
            feature = col.replace("_alert", "")
            triggered.append(f"{feature} in {int(alerts.sum())} chunk(s)")
    return triggered


def run_feature_drift_job() -> None:
    run_label = PIPELINE_RUN_ID or "manual"
    output_dir = Path("/app/output") / "feature_drift" / run_label
    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_features()
    print(f"[feature-drift] Loaded {len(df)} rows from Processed_Data")

    if len(df) < 4:
        print("[feature-drift] Not enough rows for drift analysis; skipping.")
        return

    reference, analysis = split_reference_analysis(df)
    print(f"[feature-drift] reference={len(reference)} analysis={len(analysis)}")

    chunk_size = max(1, min(len(analysis), 500))
    calc = nml.UnivariateDriftCalculator(
        column_names=FEATURE_COLS,
        timestamp_column_name="Date",
        chunk_size=chunk_size,
    )
    calc.fit(reference_data=reference)
    results = calc.calculate(data=analysis)

    csv_path = output_dir / "feature_drift_report.csv"
    results.to_df().to_csv(csv_path, index=False)
    print(f"[feature-drift] Report saved to {csv_path}")

    try:
        alerts = check_alerts(results.to_df())
        if alerts:
            print(f"[feature-drift] ALERT: covariate drift detected — {'; '.join(alerts)}")
        else:
            print("[feature-drift] OK: no feature drift detected")
    except Exception as exc:
        print(f"[feature-drift] Could not parse alert columns: {exc}")


if __name__ == "__main__":
    run_feature_drift_job()
