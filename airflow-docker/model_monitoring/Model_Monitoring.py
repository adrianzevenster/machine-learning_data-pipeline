import os
from pathlib import Path

import nannyml as nml
import pandas as pd
import mysql.connector


def load_predictions() -> pd.DataFrame:
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "RawData"),
    )

    query = """
            SELECT label, prediction, probability_0, probability_1, Date
            FROM model_predictions
            WHERE Date IS NOT NULL
            ORDER BY Date \
            """

    df = pd.read_sql(query, connection)
    connection.close()

    if df.empty:
        raise ValueError("No rows found in model_predictions.")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "label", "prediction", "probability_1"])

    if df.empty:
        raise ValueError("No valid rows remained after cleaning.")

    return df


def split_reference_analysis(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    split_idx = int(len(df) * 0.5)

    if split_idx == 0 or split_idx == len(df):
        raise ValueError("Not enough data to split into reference and analysis sets.")

    reference_df = df.iloc[:split_idx].copy()
    analysis_df = df.iloc[split_idx:].copy()

    return reference_df, analysis_df


def split_with_class_coverage(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    reference_df, analysis_df = split_reference_analysis(df)

    if reference_df["label"].nunique() >= 2 and analysis_df["label"].nunique() >= 2:
        return reference_df, analysis_df

    print("Chronological split does not contain both classes in both partitions; using stratified fallback.")
    reference_parts = []
    analysis_parts = []

    for _, class_df in df.groupby("label", sort=False):
        class_split = int(len(class_df) * 0.5)
        if class_split == 0 or class_split == len(class_df):
            raise ValueError("Each class needs at least 2 rows for monitoring.")
        reference_parts.append(class_df.iloc[:class_split])
        analysis_parts.append(class_df.iloc[class_split:])

    reference_df = pd.concat(reference_parts).sort_values("Date").reset_index(drop=True)
    analysis_df = pd.concat(analysis_parts).sort_values("Date").reset_index(drop=True)

    return reference_df, analysis_df


def run_monitoring_job() -> None:
    output_dir = Path("/app/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    predictions_df = load_predictions()

    unique_labels = predictions_df["label"].dropna().unique()
    print(f"Unique labels in full dataset: {unique_labels}")

    if len(unique_labels) < 2:
        print("Single-class data detected in full dataset, skipping calculations.")
        return

    reference_df, analysis_df = split_with_class_coverage(predictions_df)

    ref_labels = reference_df["label"].dropna().unique()
    ana_labels = analysis_df["label"].dropna().unique()

    print(f"Reference rows: {len(reference_df)}")
    print(f"Analysis rows: {len(analysis_df)}")

    if len(ref_labels) < 2:
        raise ValueError("Reference data contains fewer than 2 classes.")

    if len(ana_labels) < 2:
        raise ValueError("Analysis data contains fewer than 2 classes.")

    calculator = nml.PerformanceCalculator(
        problem_type="classification_binary",
        y_true="label",
        y_pred="prediction",
        y_pred_proba="probability_1",
        timestamp_column_name="Date",
        metrics=["roc_auc", "f1", "accuracy"],
        chunk_size=max(1, min(len(analysis_df), 1000)),
    )

    calculator.fit(reference_data=reference_df)
    results = calculator.calculate(data=analysis_df)

    plot_path = output_dir / "performance_plot.png"
    csv_path = output_dir / "performance_report.csv"

    figure = results.plot()
    figure.write_image(str(plot_path))
    results.to_df().to_csv(csv_path, index=False)

    print(f"Monitoring completed successfully.")
    print(f"Plot saved to: {plot_path}")
    print(f"Report saved to: {csv_path}")


if __name__ == "__main__":
    run_monitoring_job()
