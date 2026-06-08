import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Stub mlflow
_mlflow = types.ModuleType("mlflow")
_mlflow.set_tracking_uri = MagicMock()
_mlflow.set_registry_uri = MagicMock()
_mlflow_tracking = types.ModuleType("mlflow.tracking")
_mlflow_tracking.MlflowClient = MagicMock()
_mlflow.tracking = _mlflow_tracking
sys.modules.setdefault("mlflow", _mlflow)
sys.modules.setdefault("mlflow.tracking", _mlflow_tracking)
# Ensure attributes are set on whichever mlflow stub is in sys.modules
sys.modules["mlflow"].tracking = _mlflow_tracking

# Stub mysql.connector
_mysql_mod = types.ModuleType("mysql")
_mysql_connector = types.ModuleType("mysql.connector")
_mock_conn = MagicMock()
_mysql_connector.connect = MagicMock(return_value=_mock_conn)
_mysql_mod.connector = _mysql_connector
sys.modules.setdefault("mysql", _mysql_mod)
sys.modules.setdefault("mysql.connector", _mysql_connector)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _import_rollback():
    spec = importlib.util.spec_from_file_location(
        "rollback_model", REPO_ROOT / "model_monitoring" / "rollback_model.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


rollback = _import_rollback()


def _version(num, stage, run_id=None):
    v = MagicMock()
    v.version = str(num)
    v.current_stage = stage
    v.run_id = run_id or f"run_{num}"
    return v


# ── find_current_production ───────────────────────────────────────────────────

def test_find_current_production_returns_latest():
    client = MagicMock()
    client.search_model_versions.return_value = [
        _version(3, "Production"),
        _version(2, "Archived"),
        _version(1, "Archived"),
    ]
    result = rollback.find_current_production(client)
    assert result.version == "3"


def test_find_current_production_raises_when_none():
    client = MagicMock()
    client.search_model_versions.return_value = [_version(2, "Archived")]
    with pytest.raises(RuntimeError, match="No model currently in"):
        rollback.find_current_production(client)


# ── find_rollback_target ──────────────────────────────────────────────────────

def test_find_rollback_target_picks_highest_archived(monkeypatch):
    client = MagicMock()
    client.search_model_versions.return_value = [
        _version(3, "Production"),
        _version(2, "Archived"),
        _version(1, "Archived"),
    ]
    monkeypatch.setattr(rollback, "ROLLBACK_TO_VERSION", "")
    result = rollback.find_rollback_target(client, 3)
    assert result.version == "2"


def test_find_rollback_target_raises_when_no_archived(monkeypatch):
    client = MagicMock()
    client.search_model_versions.return_value = [_version(3, "Production")]
    monkeypatch.setattr(rollback, "ROLLBACK_TO_VERSION", "")
    with pytest.raises(RuntimeError, match="No archived model versions"):
        rollback.find_rollback_target(client, 3)


def test_find_rollback_target_pinned_version(monkeypatch):
    client = MagicMock()
    client.search_model_versions.return_value = [
        _version(3, "Production"),
        _version(1, "Archived"),
    ]
    monkeypatch.setattr(rollback, "ROLLBACK_TO_VERSION", "1")
    result = rollback.find_rollback_target(client, 3)
    assert result.version == "1"


def test_find_rollback_target_pinned_newer_raises(monkeypatch):
    client = MagicMock()
    client.search_model_versions.return_value = [
        _version(3, "Production"),
        _version(5, "Staging"),
    ]
    monkeypatch.setattr(rollback, "ROLLBACK_TO_VERSION", "5")
    with pytest.raises(RuntimeError, match="not older than"):
        rollback.find_rollback_target(client, 3)


def test_find_rollback_target_pinned_not_found_raises(monkeypatch):
    client = MagicMock()
    client.search_model_versions.return_value = [_version(3, "Production")]
    monkeypatch.setattr(rollback, "ROLLBACK_TO_VERSION", "99")
    with pytest.raises(RuntimeError, match="not found in MLflow registry"):
        rollback.find_rollback_target(client, 3)


# ── run_rollback (integration) ────────────────────────────────────────────────

def test_run_rollback_archives_current_and_promotes_target(monkeypatch):
    mock_client = MagicMock()
    mock_client.search_model_versions.return_value = [
        _version(3, "Production"),
        _version(2, "Archived"),
    ]
    _mlflow_tracking.MlflowClient.return_value = mock_client
    monkeypatch.setattr(rollback, "ROLLBACK_TO_VERSION", "")
    monkeypatch.setattr(rollback, "SERVING_RELOAD_URL", "")

    rollback.run_rollback()

    calls = mock_client.transition_model_version_stage.call_args_list
    stages_set = [c.kwargs["stage"] for c in calls]
    assert "Archived" in stages_set
    assert rollback.MODEL_PROMOTION_STAGE in stages_set

    versions_touched = [c.kwargs["version"] for c in calls]
    assert "3" in versions_touched   # current archived
    assert "2" in versions_touched   # previous promoted


def test_run_rollback_skips_serving_reload_when_url_empty(monkeypatch):
    mock_client = MagicMock()
    mock_client.search_model_versions.return_value = [
        _version(3, "Production"),
        _version(2, "Archived"),
    ]
    _mlflow_tracking.MlflowClient.return_value = mock_client
    monkeypatch.setattr(rollback, "ROLLBACK_TO_VERSION", "")
    monkeypatch.setattr(rollback, "SERVING_RELOAD_URL", "")

    import urllib.request as _urllib_request
    original_urlopen = _urllib_request.urlopen
    called = []
    _urllib_request.urlopen = lambda *a, **kw: called.append(True)

    rollback.run_rollback()
    _urllib_request.urlopen = original_urlopen

    assert called == [], "urlopen should not be called when SERVING_RELOAD_URL is empty"
