import importlib.util
import sys
import types
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATOR_PATH = REPO_ROOT / "quality" / "validate_mysql_tables.py"


class FakeCursor:
    def __init__(self, table_exists=1, columns=None, row_count=5, null_dates=0, distinct_labels=2):
        self.table_exists = table_exists
        self.columns = columns or {
            "Date",
            "User",
            "M_Out_Call_Count",
            "M_Out_Call_Time",
            "M_Data_Sum",
            "M_Data_Count",
            "M_In_Call_Count",
            "M_In_Call_Time",
            "M_TENURE_CHURN",
        }
        self.row_count = row_count
        self.null_dates = null_dates
        self.distinct_labels = distinct_labels
        self._result = None
        self._rows = []

    def execute(self, query, params=()):
        normalized_query = " ".join(query.split()).lower()
        if "information_schema.tables" in normalized_query:
            self._result = (self.table_exists,)
        elif "information_schema.columns" in normalized_query:
            self._rows = [(column,) for column in self.columns]
        elif "count(distinct" in normalized_query:
            self._result = (self.distinct_labels,)
        elif " is null" in normalized_query:
            self._result = (self.null_dates,)
        elif "select count(*) from" in normalized_query:
            self._result = (self.row_count,)
        else:
            raise AssertionError(f"Unexpected query: {query}")

    def fetchone(self):
        return self._result

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self._cursor


def load_validator(cursor):
    mysql_module = types.ModuleType("mysql")
    connector_module = types.ModuleType("mysql.connector")
    connector_module.connect = lambda **kwargs: FakeConnection(cursor)
    mysql_module.connector = connector_module

    original_mysql = sys.modules.get("mysql")
    original_connector = sys.modules.get("mysql.connector")
    sys.modules["mysql"] = mysql_module
    sys.modules["mysql.connector"] = connector_module

    try:
        spec = importlib.util.spec_from_file_location("validate_mysql_tables", VALIDATOR_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if original_mysql is None:
            sys.modules.pop("mysql", None)
        else:
            sys.modules["mysql"] = original_mysql

        if original_connector is None:
            sys.modules.pop("mysql.connector", None)
        else:
            sys.modules["mysql.connector"] = original_connector


def test_validate_processed_table_accepts_valid_contract():
    validator = load_validator(FakeCursor())

    validator.validate_table("processed")


def test_validate_processed_table_rejects_missing_required_columns():
    columns = {
        "Date",
        "User",
        "M_Out_Call_Count",
        "M_Out_Call_Time",
        "M_Data_Sum",
        "M_Data_Count",
        "M_In_Call_Count",
        "M_In_Call_Time",
    }
    validator = load_validator(FakeCursor(columns=columns))

    with pytest.raises(RuntimeError, match="missing required columns"):
        validator.validate_table("processed")


def test_validate_predictions_rejects_single_label_data():
    columns = {"pipeline_run_id", "model_version_id", "label", "prediction", "probability_0", "probability_1", "Date"}
    validator = load_validator(FakeCursor(columns=columns, distinct_labels=1))

    with pytest.raises(RuntimeError, match="distinct labels"):
        validator.validate_table("predictions")
