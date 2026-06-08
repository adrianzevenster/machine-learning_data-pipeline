import argparse
import os
import sys

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

SAMPLE_SIZE = int(os.getenv("SCHEMA_VALIDATION_SAMPLE_SIZE", "10000"))


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


def validate(target: str) -> None:
    import mysql.connector

    table_name, schema = SCHEMAS[target]
    with mysql.connector.connect(**_mysql_config()) as conn:
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
