import os

import mysql.connector
import pandas as pd

RAW_COLUMNS = [
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
]

INTEGER_COLUMNS = [
    "DP_MOC_COUNT",
    "DP_MTC_COUNT",
    "DP_MOSMS_COUNT",
    "DP_MTSMS_COUNT",
    "DP_DATA_COUNT",
    "PSEUDO_CHURNED",
]

FLOAT_COLUMNS = [
    "DP_MOC_DURATION",
    "DP_MTC_DURATION",
    "DP_DATA_VOLUME",
]

NON_NEGATIVE_FLOAT_COLUMNS = [
    "DP_MOC_DURATION",
    "DP_MTC_DURATION",
]

db_config = {
    "host": os.getenv("DB_HOST", "flaskapp-db"),
    "user": os.getenv("DB_USER", "spark"),
    "password": os.getenv("DB_PASSWORD", "sparkpw"),
    "database": os.getenv("DB_NAME", "RawData"),
}


def get_connection():
    return mysql.connector.connect(**db_config)


def ensure_raw_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS DP_CDR_Data (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            DP_DATE DATETIME NOT NULL,
            DP_MSISDN VARCHAR(64) NOT NULL,
            DP_MOC_COUNT INT NOT NULL DEFAULT 0,
            DP_MOC_DURATION DOUBLE NOT NULL DEFAULT 0,
            DP_MTC_COUNT INT NOT NULL DEFAULT 0,
            DP_MTC_DURATION DOUBLE NOT NULL DEFAULT 0,
            DP_MOSMS_COUNT INT NOT NULL DEFAULT 0,
            DP_MTSMS_COUNT INT NOT NULL DEFAULT 0,
            DP_DATA_COUNT INT NOT NULL DEFAULT 0,
            DP_DATA_VOLUME DOUBLE NOT NULL DEFAULT 0,
            PSEUDO_CHURNED INT NOT NULL,
            INDEX idx_raw_date (DP_DATE),
            INDEX idx_raw_msisdn_date (DP_MSISDN, DP_DATE),
            INDEX idx_raw_label (PSEUDO_CHURNED)
        );
        """
    )


def table_has_data():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (db_config["database"], "DP_CDR_Data"),
        )
        table_exists = cursor.fetchone()[0] > 0
        if table_exists:
            cursor.execute("SELECT COUNT(*) FROM DP_CDR_Data")
            row_count = cursor.fetchone()[0]
            conn.close()
            return row_count > 0
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    return False


def prepare_raw_chunk(chunk):
    missing_columns = sorted(set(RAW_COLUMNS) - set(chunk.columns))
    if missing_columns:
        raise ValueError(f"Raw CSV is missing required columns: {missing_columns}")

    prepared = chunk[RAW_COLUMNS].copy()
    prepared["DP_DATE"] = pd.to_datetime(prepared["DP_DATE"], errors="coerce")
    prepared["DP_MSISDN"] = prepared["DP_MSISDN"].astype(str)

    for column in INTEGER_COLUMNS:
        prepared[column] = (
            pd.to_numeric(prepared[column], errors="coerce")
            .fillna(0)
            .clip(lower=0)
            .astype(int)
        )

    for column in NON_NEGATIVE_FLOAT_COLUMNS:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce").fillna(0.0).clip(lower=0)

    prepared["DP_DATA_VOLUME"] = (
        pd.to_numeric(prepared["DP_DATA_VOLUME"], errors="coerce")
        .fillna(0.0)
        .abs()
    )

    invalid_dates = prepared["DP_DATE"].isna().sum()
    if invalid_dates:
        raise ValueError(f"Raw CSV contains {invalid_dates} rows with invalid DP_DATE values.")

    prepared["DP_DATE"] = prepared["DP_DATE"].dt.to_pydatetime()
    prepared = prepared.where(pd.notnull(prepared), None)

    return prepared


def create_table_from_csv(csv_file):
    chunksize = 10000
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"USE `{db_config['database']}`")
        ensure_raw_table(cursor)
        conn.commit()
        conn.close()

        for chunk in pd.read_csv(csv_file, chunksize=chunksize):
            chunk = prepare_raw_chunk(chunk)
            conn = get_connection()
            cursor = conn.cursor()

            placeholders = ", ".join(["%s"] * len(RAW_COLUMNS))
            columns = ", ".join([f"`{column}`" for column in RAW_COLUMNS])
            insert_sql = f"INSERT INTO DP_CDR_Data ({columns}) VALUES ({placeholders})"
            cursor.executemany(insert_sql, chunk.values.tolist())

            conn.commit()
            conn.close()
    except Exception as e:
        print(f"Error populating table: {e}")


if __name__ == "__main__":
    if table_has_data():
        print("Table already populated. Skipping database initialization.")
        exit(0)

    print("Populating the database...")
    create_table_from_csv(os.getenv("RAW_DATA_CSV", "/app/RawData.csv"))
