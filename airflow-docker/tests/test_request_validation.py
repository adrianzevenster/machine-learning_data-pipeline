import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "flaskapp"))

from request_validation import parse_positive_int


def test_parse_positive_int_uses_default_when_missing():
    assert parse_positive_int({}, "batch_size", 1000, 10000) == 1000


def test_parse_positive_int_accepts_numeric_strings():
    assert parse_positive_int({"interval": "30"}, "interval", 60, 3600) == 30


@pytest.mark.parametrize("value", ["abc", None, object()])
def test_parse_positive_int_rejects_non_integer_values(value):
    with pytest.raises(ValueError, match="batch_size must be an integer"):
        parse_positive_int({"batch_size": value}, "batch_size", 1000, 10000)


@pytest.mark.parametrize("value", [0, -1])
def test_parse_positive_int_rejects_non_positive_values(value):
    with pytest.raises(ValueError, match="num_batches must be greater than 0"):
        parse_positive_int({"num_batches": value}, "num_batches", 10, 1000)


def test_parse_positive_int_rejects_values_over_maximum():
    with pytest.raises(ValueError, match="interval must be less than or equal to 60"):
        parse_positive_int({"interval": 61}, "interval", 10, 60)
