import argparse
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

SAMPLE_SIZE = int(os.getenv("SCHEMA_VALIDATION_SAMPLE_SIZE", "10000"))
MIN_RAW_ROWS = int(os.getenv("MIN_RAW_ROWS", "1000"))
DATA_MAX_STALENESS_DAYS = int(os.getenv("DATA_MAX_STALENESS_DAYS", "7"))


def _mysql_config() -> dict:
    return {
        "host": os.getenv("MYSQL_HOST", "mysql"),
        "user": os.getenv("MYSQL_USER", "spark"),
        "password": os.getenv("MYSQL_PASSWORD", "sparkpw"),
        "database": os.getenv("MYSQL_DATABASE", "RawData"),
    }


CDR_SCHEMA = DataFrameSchema(
    {
        "DP_DATE": Column(str, nullable=False),
        "DP_MSISDN": Column(
            str,
            checks=Check(lambda s: s.str.len() > 0, element_wise=False, error="DP_MSISDN must not be empty"),
            nullable=False,
        ),
        "DP_MOC_COUNT": Column(int, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "DP_MOC_DURATION": Column(float, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "DP_MTC_COUNT": Column(int, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "DP_MTC_DURATION": Column(float, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "DP_MOSMS_COUNT": Column(int, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "DP_MTSMS_COUNT": Column(int, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "DP_DATA_COUNT": Column(int, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "DP_DATA_VOLUME": Column(float, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "PSEUDO_CHURNED": Column(int, checks=Check.isin([0, 1]), nullable=False),
    },
    coerce=True,
)

PROCESSED_SCHEMA = DataFrameSchema(
    {
        "Date": Column(str, nullable=False),
        "User": Column(str, nullable=False),
        "M_Out_Call_Count": Column(float, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "M_Out_Call_Time": Column(float, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "M_Data_Sum": Column(float, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "M_Data_Count": Column(float, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "M_In_Call_Count": Column(float, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "M_In_Call_Time": Column(float, checks=Check.greater_than_or_equal_to(0), nullable=False),
        "M_TENURE_CHURN": Column(float, checks=Check.isin([0.0, 1.0]), nullable=False),
    },
    coerce=True,
)

SCHEMAS = {
    "raw": ("DP_CDR_Data", CDR_SCHEMA),
    "processed": ("Processed_Data", PROCESSED_SCHEMA),
}


def _check_volume(table_name: str, conn) -> None:
    """Fail fast if the table has fewer rows than MIN_RAW_ROWS."""
    count_df = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM {table_name}", conn)
    total = int(count_df["cnt"].iloc[0])
    if total < MIN_RAW_ROWS:
        raise SystemExit(
            f"[volume][fatal] {table_name} has {total} rows — "
            f"minimum required is {MIN_RAW_ROWS} (MIN_RAW_ROWS). "
            "Pipeline halted to prevent training on insufficient data."
        )
    print(f"[volume] {table_name}: {total} rows (>= {MIN_RAW_ROWS} OK)")


def _check_freshness(table_name: str, date_col: str, conn) -> None:
    """Fail fast if the most recent date in date_col is stale."""
    max_df = pd.read_sql(f"SELECT MAX({date_col}) AS max_date FROM {table_name}", conn)
    max_date_raw = max_df["max_date"].iloc[0]
    if max_date_raw is None:
        raise SystemExit(f"[freshness][fatal] {table_name}.{date_col} is entirely NULL — no data.")
    max_date = pd.to_datetime(max_date_raw).to_pydatetime().replace(tzinfo=None)
    cutoff = datetime.utcnow() - timedelta(days=DATA_MAX_STALENESS_DAYS)
    if max_date < cutoff:
        raise SystemExit(
            f"[freshness][fatal] {table_name}.{date_col} newest value is {max_date.date()} — "
            f"older than {DATA_MAX_STALENESS_DAYS} days (DATA_MAX_STALENESS_DAYS). "
            "Pipeline halted to prevent training on stale data."
        )
    print(f"[freshness] {table_name}: newest {date_col}={max_date.date()} (within {DATA_MAX_STALENESS_DAYS} days OK)")


# Maps target → (table, date column) for volume+freshness checks.
# Only raw CDR data has a meaningful date gate; processed data inherits freshness.
_FRESHNESS_COLS = {
    "raw": ("DP_CDR_Data", "DP_DATE"),
}


def validate(target: str) -> None:
    import mysql.connector

    table_name, schema = SCHEMAS[target]
    with mysql.connector.connect(**_mysql_config()) as conn:
        _check_volume(table_name, conn)
        if target in _FRESHNESS_COLS:
            _, date_col = _FRESHNESS_COLS[target]
            _check_freshness(table_name, date_col, conn)

        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT {SAMPLE_SIZE}", conn)

    if df.empty:
        print(f"[schema] {table_name}: no rows to validate; skipping.")
        return

    schema.validate(df, lazy=True)
    print(f"[schema] {table_name}: {len(df)} rows validated OK")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=sorted(SCHEMAS))
    args = parser.parse_args()

    try:
        validate(args.target)
    except pa.errors.SchemaErrors as exc:
        print(
            f"[schema][fatal] Validation failed for {args.target}:\n{exc.failure_cases.to_string()}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    except Exception as exc:
        print(f"[schema][fatal] {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
