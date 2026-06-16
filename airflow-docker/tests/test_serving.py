import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from fastapi import HTTPException

# Stub mlflow before serving/main.py is imported so no Spark JVM is needed in CI
_mlflow = types.ModuleType("mlflow")
_mlflow.set_tracking_uri = MagicMock()

_mlflow_pyfunc = types.ModuleType("mlflow.pyfunc")


class _StubPyFuncModel:
    pass


_mlflow_pyfunc.PyFuncModel = _StubPyFuncModel
_mlflow_pyfunc.load_model = MagicMock()
_mlflow.pyfunc = _mlflow_pyfunc

_mlflow_tracking = types.ModuleType("mlflow.tracking")
_mock_client = MagicMock()
_mock_client.search_model_versions.return_value = []   # no versions → skip SHAP setup
_mlflow_tracking.MlflowClient = MagicMock(return_value=_mock_client)
_mlflow.tracking = _mlflow_tracking

sys.modules.setdefault("mlflow", _mlflow)
sys.modules.setdefault("mlflow.pyfunc", _mlflow_pyfunc)
sys.modules.setdefault("mlflow.tracking", _mlflow_tracking)
# Ensure attributes are set on whichever mlflow stub is in sys.modules
sys.modules["mlflow"].pyfunc = _mlflow_pyfunc
sys.modules["mlflow"].tracking = _mlflow_tracking

# Stub prediction_logger so serving/main.py doesn't need a live MySQL in tests
_prediction_logger_stub = types.ModuleType("prediction_logger")
_prediction_logger_stub.log_prediction = MagicMock()
sys.modules["prediction_logger"] = _prediction_logger_stub

REPO_ROOT = Path(__file__).resolve().parents[1]


def _import_serving_main():
    spec = importlib.util.spec_from_file_location(
        "serving_main", REPO_ROOT / "serving" / "main.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


serving = _import_serving_main()


def _make_mock_model():
    def _predict(df):
        n = len(df)
        return pd.DataFrame(
            {
                "prediction": [0] * n,
                "probability": [np.array([0.7, 0.3])] * n,
            }
        )

    m = MagicMock()
    m.predict.side_effect = _predict
    return m


VALID_FEATURES = {
    "M_Out_Call_Count": 5,
    "M_Out_Call_Time": 120.0,
    "M_Data_Sum": 50000000,
    "M_Data_Count": 3,
    "M_In_Call_Count": 8,
    "M_In_Call_Time": 200.0,
}


class _Response:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        if hasattr(self._body, "model_dump"):
            return self._body.model_dump()
        return self._body


class _DirectClient:
    def get(self, path):
        try:
            if path == "/health":
                return _Response(200, serving.health())
            if path == "/model/info":
                return _Response(200, serving.model_info())
            raise AssertionError(f"Unexpected GET path: {path}")
        except HTTPException as exc:
            return _Response(exc.status_code, {"detail": exc.detail})

    def post(self, path, json=None):
        payload = json or {}
        try:
            if path == "/predict":
                return _Response(200, serving.predict(serving.Features(**payload)))
            if path == "/predict/batch":
                records = [serving.Features(**record) for record in payload.get("records", [])]
                request = serving.BatchPredictRequest(
                    records=records,
                    routing_msisdn=payload.get("routing_msisdn"),
                )
                return _Response(200, serving.predict_batch(request))
            if path == "/predict/explain":
                return _Response(200, serving.predict_explain(serving.Features(**payload)))
            if path == "/model/reload":
                return _Response(200, serving.model_reload())
            if path == "/model/shadow/reload":
                return _Response(200, serving.shadow_reload())
            raise AssertionError(f"Unexpected POST path: {path}")
        except HTTPException as exc:
            return _Response(exc.status_code, {"detail": exc.detail})


@pytest.fixture(autouse=True)
def _reset_model_state():
    serving._model = None
    serving._model_uri = None
    serving._shadow_model = None
    serving._shadow_model_uri = None
    serving.SHADOW_MODEL_ENABLED = False
    serving.AB_TESTING_ENABLED = False
    serving.AB_TRAFFIC_SPLIT = 0.1
    yield
    serving._model = None
    serving._model_uri = None
    serving._shadow_model = None
    serving._shadow_model_uri = None
    serving.SHADOW_MODEL_ENABLED = False
    serving.AB_TESTING_ENABLED = False
    serving.AB_TRAFFIC_SPLIT = 0.1


@pytest.fixture()
def client():
    _mlflow_pyfunc.load_model.side_effect = RuntimeError("no model in CI")
    yield _DirectClient()
    _mlflow_pyfunc.load_model.side_effect = None


@pytest.fixture()
def loaded_client():
    _mlflow_pyfunc.load_model.side_effect = None
    _mlflow_pyfunc.load_model.return_value = _make_mock_model()
    serving._model = _make_mock_model()
    serving._model_uri = f"models:/{serving.MODEL_NAME}/{serving.MODEL_STAGE}"
    yield _DirectClient()


# ── no-model tests ────────────────────────────────────────────────────────────

def test_health_no_model(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["model_loaded"] is False


def test_predict_no_model_returns_503(client):
    resp = client.post("/predict", json=VALID_FEATURES)
    assert resp.status_code == 503


def test_model_info_no_model_returns_503(client):
    resp = client.get("/model/info")
    assert resp.status_code == 503


# ── loaded-model tests ────────────────────────────────────────────────────────

def test_health_with_model_loaded(loaded_client):
    resp = loaded_client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["model_loaded"] is True


def test_predict_returns_valid_churn_score(loaded_client):
    resp = loaded_client.post("/predict", json=VALID_FEATURES)
    assert resp.status_code == 200
    body = resp.json()
    assert "result" in body
    result = body["result"]
    assert result["prediction"] in (0, 1)
    assert 0.0 <= result["probability_churn"] <= 1.0
    assert 0.0 <= result["probability_retain"] <= 1.0
    assert abs(result["probability_churn"] + result["probability_retain"] - 1.0) < 1e-6


def test_predict_probabilities_sum_to_one(loaded_client):
    resp = loaded_client.post("/predict", json=VALID_FEATURES)
    result = resp.json()["result"]
    total = result["probability_churn"] + result["probability_retain"]
    assert abs(total - 1.0) < 1e-6


def test_predict_batch_returns_all_records(loaded_client):
    resp = loaded_client.post(
        "/predict/batch",
        json={
            "records": [
                {**VALID_FEATURES},
                {"M_Out_Call_Count": 0, "M_Out_Call_Time": 0, "M_Data_Sum": 0,
                 "M_Data_Count": 0, "M_In_Call_Count": 0, "M_In_Call_Time": 0},
            ]
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["record_count"] == 2
    assert len(body["results"]) == 2


def test_predict_batch_empty_records_returns_422(loaded_client):
    resp = loaded_client.post("/predict/batch", json={"records": []})
    assert resp.status_code == 422


def test_predict_explain_no_shap_returns_prediction(loaded_client):
    resp = loaded_client.post("/predict/explain", json=VALID_FEATURES)
    assert resp.status_code == 200
    body = resp.json()
    assert "result" in body
    assert body["result"]["prediction"] in (0, 1)
    # SHAP explainer not set up in CI (no real MLflow artifacts)
    assert body["shap_values"] is None


def test_model_info_returns_uri(loaded_client):
    resp = loaded_client.get("/model/info")
    assert resp.status_code == 200
    body = resp.json()
    assert "model_name" in body
    assert "model_stage" in body
    assert "model_uri" in body


def test_model_reload_succeeds(loaded_client):
    resp = loaded_client.post("/model/reload")
    assert resp.status_code == 200
    assert "model_uri" in resp.json()


# ── A/B testing tests ─────────────────────────────────────────────────────────

def test_ab_disabled_response_has_champion_variant(loaded_client):
    resp = loaded_client.post("/predict", json=VALID_FEATURES)
    assert resp.status_code == 200
    assert resp.json()["model_variant"] == "champion"


def test_ab_enabled_no_challenger_falls_back_to_champion(loaded_client):
    serving.AB_TESTING_ENABLED = True
    # _shadow_model is None (reset by autouse)
    resp = loaded_client.post("/predict", json=VALID_FEATURES)
    assert resp.status_code == 200
    assert resp.json()["model_variant"] == "champion"


def test_ab_enabled_100pct_split_returns_challenger(loaded_client):
    serving.AB_TESTING_ENABLED = True
    serving.AB_TRAFFIC_SPLIT = 1.0
    serving._shadow_model = _make_mock_model()
    resp = loaded_client.post("/predict", json=VALID_FEATURES)
    assert resp.status_code == 200
    assert resp.json()["model_variant"] == "challenger"


def test_ab_msisdn_routing_is_deterministic():
    serving.AB_TESTING_ENABLED = True
    serving.AB_TRAFFIC_SPLIT = 0.5
    serving._shadow_model = _make_mock_model()
    msisdn = "27821234567"
    variants = {serving._route_variant(msisdn) for _ in range(20)}
    assert len(variants) == 1, "Same MSISDN must always route to the same variant"


def test_ab_batch_includes_variant(loaded_client):
    serving.AB_TESTING_ENABLED = True
    serving.AB_TRAFFIC_SPLIT = 1.0
    serving._shadow_model = _make_mock_model()
    resp = loaded_client.post(
        "/predict/batch",
        json={"records": [VALID_FEATURES, VALID_FEATURES], "routing_msisdn": "27829999999"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["model_variant"] == "challenger"
    assert body["record_count"] == 2


# ── shadow deployment tests ───────────────────────────────────────────────────

def test_shadow_disabled_predict_returns_normally(loaded_client):
    assert not serving.SHADOW_MODEL_ENABLED
    resp = loaded_client.post("/predict", json=VALID_FEATURES)
    assert resp.status_code == 200


def test_shadow_enabled_predict_returns_champion_result(loaded_client):
    serving.SHADOW_MODEL_ENABLED = True
    serving._shadow_model = _make_mock_model()
    resp = loaded_client.post("/predict", json=VALID_FEATURES)
    assert resp.status_code == 200
    assert resp.json()["result"]["prediction"] in (0, 1)


def test_shadow_reload_disabled_returns_404(loaded_client):
    resp = loaded_client.post("/model/shadow/reload")
    assert resp.status_code == 404


def test_shadow_reload_enabled_returns_200(loaded_client):
    serving.SHADOW_MODEL_ENABLED = True
    _mlflow_pyfunc.load_model.return_value = _make_mock_model()
    resp = loaded_client.post("/model/shadow/reload")
    assert resp.status_code == 200
    assert "shadow_model_uri" in resp.json()


def test_health_reports_shadow_loaded(loaded_client):
    serving._shadow_model = _make_mock_model()
    resp = loaded_client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["shadow_loaded"] is True


# ── fixture data integrity ────────────────────────────────────────────────────

def test_fixture_csv_has_correct_schema():
    fixture = Path(__file__).parent / "fixtures" / "sample_cdr_data.csv"
    assert fixture.exists(), "Fixture CSV is missing"
    df = pd.read_csv(fixture)
    required = {
        "DP_DATE", "DP_MSISDN", "DP_MOC_COUNT", "DP_MOC_DURATION",
        "DP_MTC_COUNT", "DP_MTC_DURATION", "DP_MOSMS_COUNT", "DP_MTSMS_COUNT",
        "DP_DATA_COUNT", "DP_DATA_VOLUME", "PSEUDO_CHURNED",
    }
    assert required <= set(df.columns)
    assert len(df) == 200
    assert set(df["PSEUDO_CHURNED"].unique()) == {0, 1}
    assert (df[["DP_MOC_COUNT", "DP_MTC_COUNT", "DP_DATA_COUNT"]] >= 0).all().all()
