import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Stub mysql.connector
_mysql_mod = types.ModuleType("mysql")
_mysql_connector = types.ModuleType("mysql.connector")
_mysql_mod.connector = _mysql_connector
sys.modules.setdefault("mysql", _mysql_mod)
sys.modules.setdefault("mysql.connector", _mysql_connector)

# Stub sklearn in case it's not installed in CI
_sklearn = types.ModuleType("sklearn")
_sklearn_metrics = types.ModuleType("sklearn.metrics")
_sklearn_metrics.roc_auc_score = MagicMock(return_value=0.82)
_sklearn.metrics = _sklearn_metrics
sys.modules.setdefault("sklearn", _sklearn)
sys.modules.setdefault("sklearn.metrics", _sklearn_metrics)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _import_ab_analysis():
    spec = importlib.util.spec_from_file_location(
        "ab_analysis", REPO_ROOT / "model_monitoring" / "ab_analysis.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ab = _import_ab_analysis()

_CHAMPION_ROW = {
    "model_variant": "champion",
    "total": 100,
    "correct": 75,
    "accuracy": 0.75,
    "tp": 30,
    "fp": 10,
    "fn": 15,
}
_CHALLENGER_ROW = {
    "model_variant": "challenger",
    "total": 50,
    "correct": 44,
    "accuracy": 0.88,
    "tp": 20,
    "fp": 4,
    "fn": 2,
}


# ── enrich_metrics ────────────────────────────────────────────────────────────

def test_enrich_metrics_precision():
    m = ab.enrich_metrics(_CHAMPION_ROW.copy(), [0, 1, 1], [0.1, 0.9, 0.8])
    assert m["precision"] == pytest.approx(30 / 40)


def test_enrich_metrics_recall():
    m = ab.enrich_metrics(_CHAMPION_ROW.copy(), [0, 1, 1], [0.1, 0.9, 0.8])
    assert m["recall"] == pytest.approx(30 / 45)


def test_enrich_metrics_f1():
    m = ab.enrich_metrics(_CHAMPION_ROW.copy(), [0, 1, 1], [0.1, 0.9, 0.8])
    assert m["f1"] is not None
    assert 0.0 < m["f1"] < 1.0


def test_enrich_metrics_no_positives_gives_none():
    row = {**_CHAMPION_ROW, "tp": 0, "fp": 0, "fn": 0}
    m = ab.enrich_metrics(row, [0, 0], [0.1, 0.2])
    assert m["precision"] is None
    assert m["recall"] is None


# ── make_recommendation ───────────────────────────────────────────────────────

def test_recommendation_promote_challenger_when_lift_exceeds_threshold(monkeypatch):
    monkeypatch.setattr(ab, "AB_LIFT_ALERT_THRESHOLD", 0.02)
    rec, sig = ab.make_recommendation(
        {"accuracy": 0.75, "total": 1000, "correct": 750},
        {"accuracy": 0.88, "total": 1000, "correct": 880},
    )
    assert rec == "promote_challenger"
    assert sig["significant"] is True


def test_recommendation_keep_champion_when_challenger_worse(monkeypatch):
    monkeypatch.setattr(ab, "AB_LIFT_ALERT_THRESHOLD", 0.02)
    rec, sig = ab.make_recommendation(
        {"accuracy": 0.88, "total": 1000, "correct": 880},
        {"accuracy": 0.75, "total": 1000, "correct": 750},
    )
    assert rec == "keep_champion"
    assert sig["significant"] is True


def test_recommendation_no_significant_difference_within_threshold(monkeypatch):
    monkeypatch.setattr(ab, "AB_LIFT_ALERT_THRESHOLD", 0.02)
    rec, sig = ab.make_recommendation(
        {"accuracy": 0.80, "total": 1000, "correct": 800},
        {"accuracy": 0.81, "total": 1000, "correct": 810},
    )
    assert rec == "no_significant_difference"
    assert sig["significant"] is False


# ── run_analysis (integration with mocked cursor) ────────────────────────────

def _make_cursor(agg_rows, raw_champion=None, raw_challenger=None):
    cursor = MagicMock()
    raw_champion = raw_champion or [(1, 0.9), (0, 0.2)]
    raw_challenger = raw_challenger or [(1, 0.85), (0, 0.15)]

    agg_dicts = agg_rows
    agg_col_names = list(agg_dicts[0].keys()) if agg_dicts else []
    agg_tuples = [tuple(d.values()) for d in agg_dicts]

    desc_agg = [(col,) for col in agg_col_names]
    raw_col_names = [("actual_churn",), ("probability_churn",)]

    call_count = [0]

    def side_effect_description():
        return desc_agg if call_count[0] == 0 else raw_col_names

    side_effects_fetchall = [
        agg_tuples,
        raw_champion,
        raw_challenger,
    ]
    fetchall_call = [0]

    def fetchall_side():
        result = side_effects_fetchall[fetchall_call[0]]
        fetchall_call[0] += 1
        return result

    cursor.fetchall.side_effect = fetchall_side

    descriptions = [desc_agg, raw_col_names, raw_col_names]
    desc_call = [0]

    def desc_prop():
        result = descriptions[desc_call[0]]
        desc_call[0] += 1
        return result

    type(cursor).description = property(lambda self: desc_prop())
    return cursor


def test_run_analysis_returns_promote_challenger(monkeypatch, tmp_path):
    monkeypatch.setattr(ab, "AB_MIN_SAMPLE_SIZE", 30)
    monkeypatch.setattr(ab, "AB_LIFT_ALERT_THRESHOLD", 0.02)
    monkeypatch.setattr(ab, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(ab, "SLACK_WEBHOOK_URL", "")

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    agg_rows = [_CHAMPION_ROW, _CHALLENGER_ROW]
    agg_tuples = [tuple(d.values()) for d in agg_rows]
    raw_data = [(1, 0.9), (0, 0.2)] * 25

    fetchall_results = [agg_tuples, raw_data, raw_data]
    fetchall_idx = [0]

    def fetchall():
        r = fetchall_results[fetchall_idx[0]]
        fetchall_idx[0] += 1
        return r

    mock_cursor.fetchall.side_effect = fetchall
    agg_cols = [(col,) for col in _CHAMPION_ROW.keys()]
    raw_cols = [("actual_churn",), ("probability_churn",)]
    descriptions = [agg_cols, raw_cols, raw_cols]
    desc_idx = [0]
    mock_cursor.description = property(lambda self: None)

    desc_values = [agg_cols, raw_cols, raw_cols]
    d_idx = [0]

    original_execute = mock_cursor.execute

    def execute_side(sql, params=None):
        mock_cursor._last_desc = desc_values[min(d_idx[0], 2)]
        d_idx[0] += 1

    mock_cursor.execute.side_effect = execute_side
    type(mock_cursor).description = property(lambda self: self._last_desc)
    mock_cursor._last_desc = agg_cols

    _mysql_connector.connect = MagicMock(return_value=mock_conn)

    report = ab.run_analysis()
    assert isinstance(report, dict)


def test_run_analysis_returns_empty_when_no_data(monkeypatch):
    monkeypatch.setattr(ab, "OUTPUT_DIR", Path("/tmp"))
    monkeypatch.setattr(ab, "SLACK_WEBHOOK_URL", "")

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = []
    _mysql_connector.connect = MagicMock(return_value=mock_conn)

    report = ab.run_analysis()
    assert report == {}


def test_run_analysis_insufficient_sample_skips_recommendation(monkeypatch, tmp_path):
    monkeypatch.setattr(ab, "AB_MIN_SAMPLE_SIZE", 200)
    monkeypatch.setattr(ab, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(ab, "SLACK_WEBHOOK_URL", "")

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    small_row = {**_CHAMPION_ROW, "total": 10}
    agg_tuples = [tuple(small_row.values())]
    raw_data = [(1, 0.9), (0, 0.2)]
    fetchall_results = [agg_tuples, raw_data]
    idx = [0]

    def fetchall():
        r = fetchall_results[idx[0]]
        idx[0] += 1
        return r

    mock_cursor.fetchall.side_effect = fetchall
    agg_cols = [(col,) for col in small_row.keys()]
    raw_cols = [("actual_churn",), ("probability_churn",)]

    d_idx = [0]
    desc_vals = [agg_cols, raw_cols]

    def execute_side(sql, params=None):
        mock_cursor._last_desc = desc_vals[min(d_idx[0], 1)]
        d_idx[0] += 1

    mock_cursor.execute.side_effect = execute_side
    type(mock_cursor).description = property(lambda self: self._last_desc)
    mock_cursor._last_desc = agg_cols
    _mysql_connector.connect = MagicMock(return_value=mock_conn)

    report = ab.run_analysis()
    assert report.get("recommendation") == "insufficient_sample_size"
