import argparse
import os
import sys

import mysql.connector


TABLE_CONTRACTS = {
    "raw": {
        "table": "DP_CDR_Data",
        "required_columns": {
            "id",
            "DP_DATE",
            "DP_MSISDN",
            "DP_MOC_COUNT",
            "DP_MOC_DURATION",
            "DP_MTC_COUNT",
            "DP_MTC_DURATION",
            "DP_MOSMS_COUNT",
            "DP_MTSMS_COUNT",
            "DP_DATA_COUNT",
            "DP_DATA_VOLUME",
            "PSEUDO_CHURNED",
        },
        "min_rows_env": "MIN_RAW_ROWS",
        "default_min_rows": 1,
        "min_labels": 0,
        "label_column": None,
        "date_column": "DP_DATE",
    },
    "processed": {
        "table": "Processed_Data",
        "required_columns": {
            "Date",
            "User",
            "M_Out_Call_Count",
            "M_Out_Call_Time",
            "M_Data_Sum",
            "M_Data_Count",
            "M_In_Call_Count",
            "M_In_Call_Time",
            "M_TENURE_CHURN",
        },
        "min_rows_env": "MIN_PROCESSED_ROWS",
        "default_min_rows": 1,
        "min_labels": 2,
        "label_column": "M_TENURE_CHURN",
        "date_column": "Date",
    },
    "predictions": {
        "table": "model_predictions",
        "required_columns": {
            "pipeline_run_id",
            "model_version_id",
            "label",
            "prediction",
            "probability_0",
            "probability_1",
            "Date",
        },
        "min_rows_env": "MIN_PREDICTION_ROWS",
        "default_min_rows": 1,
        "min_labels": 2,
        "label_column": "label",
        "date_column": "Date",
        "run_id_column": "pipeline_run_id",
    },
}


def connect():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        user=os.getenv("MYSQL_USER", "spark"),
        password=os.getenv("MYSQL_PASSWORD", "sparkpw"),
        database=os.getenv("MYSQL_DATABASE", "RawData"),
    )


def fetch_one(cursor, query, params=None):
    cursor.execute(query, params or ())
    return cursor.fetchone()


def validate_table(contract_name):
    contract = TABLE_CONTRACTS[contract_name]
    table = contract["table"]
    min_rows = int(os.getenv(contract["min_rows_env"], str(contract["default_min_rows"])))

    with connect() as conn:
        cursor = conn.cursor()

        table_exists = fetch_one(
            cursor,
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name = %s
            """,
            (table,),
        )[0]
        if table_exists != 1:
            raise RuntimeError(f"Required table {table} does not exist.")

        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = %s
            """,
            (table,),
        )
        actual_columns = {row[0] for row in cursor.fetchall()}
        missing_columns = sorted(contract["required_columns"] - actual_columns)
        if missing_columns:
            raise RuntimeError(f"{table} is missing required columns: {missing_columns}")

        row_count = fetch_one(cursor, f"SELECT COUNT(*) FROM {table}")[0]
        if row_count < min_rows:
            raise RuntimeError(f"{table} has {row_count} rows; expected at least {min_rows}.")

        date_column = contract["date_column"]
        null_dates = fetch_one(cursor, f"SELECT COUNT(*) FROM {table} WHERE `{date_column}` IS NULL")[0]
        if null_dates:
            raise RuntimeError(f"{table}.{date_column} contains {null_dates} NULL values.")

        label_column = contract["label_column"]
        distinct_labels = None
        if label_column:
            distinct_labels = fetch_one(cursor, f"SELECT COUNT(DISTINCT `{label_column}`) FROM {table}")[0]
            if distinct_labels < contract["min_labels"]:
                raise RuntimeError(
                    f"{table}.{label_column} has {distinct_labels} distinct labels; "
                    f"expected at least {contract['min_labels']}."
                )

        run_id_column = contract.get("run_id_column")
        pipeline_run_id = os.getenv("PIPELINE_RUN_ID")
        if run_id_column:
            null_run_ids = fetch_one(cursor, f"SELECT COUNT(*) FROM {table} WHERE `{run_id_column}` IS NULL")[0]
            if null_run_ids:
                raise RuntimeError(f"{table}.{run_id_column} contains {null_run_ids} NULL values.")

        if contract_name == "predictions" and pipeline_run_id:
            run_prediction_count = fetch_one(
                cursor,
                "SELECT COUNT(*) FROM model_predictions WHERE pipeline_run_id = %s",
                (pipeline_run_id,),
            )[0]
            if run_prediction_count < min_rows:
                raise RuntimeError(
                    f"model_predictions has {run_prediction_count} rows for run {pipeline_run_id}; "
                    f"expected at least {min_rows}."
                )

            model_version_count = fetch_one(
                cursor,
                "SELECT COUNT(*) FROM model_versions WHERE run_id = %s",
                (pipeline_run_id,),
            )[0]
            if model_version_count < 1:
                raise RuntimeError(f"No model_versions row found for run {pipeline_run_id}.")

            pipeline_run_count = fetch_one(
                cursor,
                "SELECT COUNT(*) FROM pipeline_runs WHERE run_id = %s",
                (pipeline_run_id,),
            )[0]
            if pipeline_run_count < 1:
                raise RuntimeError(f"No pipeline_runs row found for run {pipeline_run_id}.")

        print(
            f"[quality] {table}: rows={row_count}"
            + (f" distinct_{label_column}={distinct_labels}" if label_column else "")
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("contract", choices=sorted(TABLE_CONTRACTS))
    args = parser.parse_args()

    try:
        validate_table(args.contract)
    except Exception as exc:
        print(f"[quality][fatal] {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
