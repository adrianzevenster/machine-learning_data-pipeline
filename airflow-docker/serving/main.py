import hashlib
import json
import logging
import os
import random
import threading
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import mlflow.pyfunc
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from mlflow.tracking import MlflowClient
from prometheus_client import Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field

try:
    import shap as _shap
    _SHAP_AVAILABLE = True
except ImportError:
    _shap = None
    _SHAP_AVAILABLE = False

try:
    from prediction_logger import log_prediction as _log_prediction
except (ImportError, ModuleNotFoundError):
    def _log_prediction(*args, **kwargs):  # noqa: F811
        pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.getenv("MLFLOW_REGISTERED_MODEL_NAME", "customer_churn_random_forest")
MODEL_STAGE = os.getenv("MODEL_PROMOTION_STAGE", "Production")
SHADOW_MODEL_ENABLED = os.getenv("SHADOW_MODEL_ENABLED", "false").lower() == "true"
SHADOW_MODEL_STAGE = os.getenv("SHADOW_MODEL_STAGE", "Staging")
AB_TESTING_ENABLED = os.getenv("AB_TESTING_ENABLED", "false").lower() == "true"
AB_TRAFFIC_SPLIT = float(os.getenv("AB_TRAFFIC_SPLIT", "0.1"))

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

FEATURE_COLS = [
    "M_Out_Call_Count",
    "M_Out_Call_Time",
    "M_Data_Sum",
    "M_Data_Count",
    "M_In_Call_Count",
    "M_In_Call_Time",
]

_model: Optional[mlflow.pyfunc.PyFuncModel] = None
_model_uri: Optional[str] = None
_model_lock = threading.Lock()
_feature_importances: Optional[Dict[str, float]] = None
_shap_explainer: Optional[Any] = None
_shadow_model: Optional[mlflow.pyfunc.PyFuncModel] = None
_shadow_model_uri: Optional[str] = None

# ── Prometheus metrics ────────────────────────────────────────────────────────

_PREDICTIONS_TOTAL = Counter(
    "churn_predictions_total",
    "Total predictions made, labelled by predicted class",
    ["predicted_class"],
)
_MODEL_RELOADS_TOTAL = Counter(
    "churn_model_reloads_total",
    "Number of model hot-reloads since container start",
)
_MODEL_INFO = Gauge(
    "churn_model_info",
    "Active model metadata; value is always 1, use labels for identity",
    ["model_name", "model_stage"],
)
_SHADOW_PREDICTIONS_TOTAL = Counter(
    "churn_shadow_predictions_total",
    "Challenger (shadow) model predictions — not returned to callers",
    ["predicted_class"],
)
_SHADOW_AGREEMENT = Gauge(
    "churn_shadow_champion_agreement",
    "Agreement ratio between champion and shadow for the last prediction batch (0.0–1.0)",
)
_AB_PREDICTIONS_TOTAL = Counter(
    "churn_ab_predictions_total",
    "A/B test predictions split by variant and predicted class",
    ["variant", "predicted_class"],
)


# ── Model + explanation loading ───────────────────────────────────────────────

def _load_explanation_artifacts(run_id: str) -> None:
    global _feature_importances, _shap_explainer

    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

    try:
        local = client.download_artifacts(run_id, "explanation/feature_importances.json", "/tmp/mlflow_expl")
        with open(local) as f:
            _feature_importances = json.load(f)
        logger.info("Feature importances loaded: %s", list(_feature_importances.keys()))
    except Exception as exc:
        logger.warning("Could not load feature importances: %s", exc)

    if not _SHAP_AVAILABLE:
        logger.info("shap not installed; skipping KernelExplainer setup.")
        return

    try:
        local = client.download_artifacts(run_id, "explanation/shap_background.csv", "/tmp/mlflow_expl")
        background = pd.read_csv(local)[FEATURE_COLS]

        def _predict_proba(data: np.ndarray) -> np.ndarray:
            with _model_lock:
                m = _model
            df = pd.DataFrame(data, columns=FEATURE_COLS)
            df["M_TENURE_CHURN"] = 0.0
            result = m.predict(df)
            return result["probability"].apply(lambda p: float(p[1])).values

        _shap_explainer = _shap.KernelExplainer(_predict_proba, background)
        logger.info("SHAP KernelExplainer ready (%d background samples)", len(background))
    except Exception as exc:
        logger.warning("Could not build SHAP explainer: %s", exc)


def _load_shadow_model() -> None:
    global _shadow_model, _shadow_model_uri
    if not SHADOW_MODEL_ENABLED and not AB_TESTING_ENABLED:
        return
    uri = f"models:/{MODEL_NAME}/{SHADOW_MODEL_STAGE}"
    logger.info("Loading shadow/challenger model from %s", uri)
    try:
        loaded = mlflow.pyfunc.load_model(uri)
        with _model_lock:
            _shadow_model = loaded
            _shadow_model_uri = uri
        logger.info("Shadow/challenger model loaded: %s", uri)
    except Exception as exc:
        logger.warning("Could not load shadow/challenger model from %s: %s", uri, exc)


def _load_model() -> None:
    global _model, _model_uri
    uri = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
    logger.info("Loading model from %s", uri)
    loaded = mlflow.pyfunc.load_model(uri)
    with _model_lock:
        _model = loaded
        _model_uri = uri
    _MODEL_RELOADS_TOTAL.inc()
    _MODEL_INFO.labels(model_name=MODEL_NAME, model_stage=MODEL_STAGE).set(1)
    logger.info("Model loaded: %s", uri)

    try:
        client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        prod = next((v for v in versions if v.current_stage == MODEL_STAGE), None)
        if prod:
            _load_explanation_artifacts(prod.run_id)
        else:
            logger.warning("No '%s' version in MLflow registry; skipping explanation artifacts.", MODEL_STAGE)
    except Exception as exc:
        logger.warning("Could not resolve MLflow run for explanation artifacts: %s", exc)

    _load_shadow_model()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        _load_model()
    except Exception as exc:
        logger.warning(
            "No '%s' model available at startup: %s. Call POST /model/reload once promoted.",
            MODEL_STAGE,
            exc,
        )
    yield


app = FastAPI(
    title="Churn Prediction Service",
    description="Serves the promoted customer_churn_random_forest model from MLflow.",
    version="1.0.0",
    lifespan=lifespan,
)

Instrumentator().instrument(app).expose(app)


# ── Request / response schemas ────────────────────────────────────────────────

class Features(BaseModel):
    M_Out_Call_Count: float = Field(..., description="Number of outgoing calls in the aggregation window")
    M_Out_Call_Time:  float = Field(..., description="Total duration of outgoing calls (seconds)")
    M_Data_Sum:       float = Field(..., description="Total data volume consumed (bytes)")
    M_Data_Count:     int   = Field(..., description="Number of data sessions")
    M_In_Call_Count:  int   = Field(..., description="Number of incoming calls")
    M_In_Call_Time:   float = Field(..., description="Total duration of incoming calls (seconds)")
    msisdn:           Optional[str] = Field(None, description="Subscriber ID for consistent A/B routing")


class Prediction(BaseModel):
    prediction:         int   = Field(..., description="0 = not churned, 1 = churned")
    probability_churn:  float = Field(..., description="P(churn=1)")
    probability_retain: float = Field(..., description="P(churn=0)")


class PredictResponse(BaseModel):
    result:        Prediction
    model_variant: str
    model_name:    str
    model_stage:   str
    model_uri:     str


class BatchPredictRequest(BaseModel):
    records:         List[Features]
    routing_msisdn:  Optional[str] = Field(None, description="MSISDN used to route the whole batch to a consistent variant")


class BatchPredictResponse(BaseModel):
    results:       List[Prediction]
    record_count:  int
    model_variant: str
    model_name:    str
    model_stage:   str
    model_uri:     str


class ExplainResponse(BaseModel):
    result:               Prediction
    model_variant:        str
    feature_importances:  Optional[Dict[str, float]]
    shap_values:          Optional[Dict[str, float]]
    model_name:           str
    model_stage:          str
    model_uri:            str


# ── Routing + model selection ─────────────────────────────────────────────────

def _route_variant(msisdn: Optional[str]) -> str:
    """Return 'champion' or 'challenger' based on A/B config and MSISDN hash."""
    if not AB_TESTING_ENABLED:
        return "champion"
    with _model_lock:
        has_challenger = _shadow_model is not None
    if not has_challenger:
        logger.warning("A/B testing enabled but no challenger model loaded; routing to champion.")
        return "champion"
    if msisdn:
        bucket = int(hashlib.md5(msisdn.encode()).hexdigest(), 16) % 100
        return "challenger" if bucket < int(AB_TRAFFIC_SPLIT * 100) else "champion"
    return "challenger" if random.random() < AB_TRAFFIC_SPLIT else "champion"


def _select_model_for_variant(variant: str) -> mlflow.pyfunc.PyFuncModel:
    champion = _require_model()
    if variant == "challenger":
        with _model_lock:
            shadow = _shadow_model
        if shadow is not None:
            return shadow
        logger.warning("Challenger selected but shadow model not available; falling back to champion.")
    return champion


# ── Internal helpers ──────────────────────────────────────────────────────────

def _require_model() -> mlflow.pyfunc.PyFuncModel:
    with _model_lock:
        if _model is None:
            raise HTTPException(
                status_code=503,
                detail=f"No '{MODEL_STAGE}' model loaded. POST /model/reload after promoting a model.",
            )
        return _model


def _run_shadow_inference(df: pd.DataFrame, champion_preds: List[Prediction]) -> None:
    with _model_lock:
        shadow = _shadow_model
    if shadow is None:
        return
    try:
        result = shadow.predict(df)
        agreements = 0
        for i, (_, row) in enumerate(result.iterrows()):
            shadow_pred = int(row["prediction"])
            _SHADOW_PREDICTIONS_TOTAL.labels(predicted_class=str(shadow_pred)).inc()
            if i < len(champion_preds):
                agreements += 1 if shadow_pred == champion_preds[i].prediction else 0
        if champion_preds:
            _SHADOW_AGREEMENT.set(agreements / len(champion_preds))
    except Exception as exc:
        logger.warning("Shadow inference failed (non-fatal): %s", exc)


def _run_inference(
    model: mlflow.pyfunc.PyFuncModel,
    df: pd.DataFrame,
    variant: str = "champion",
) -> List[Prediction]:
    df = df.copy()
    df["M_TENURE_CHURN"] = 0.0
    result = model.predict(df)
    predictions = []
    for _, row in result.iterrows():
        prob = row["probability"]
        pred = Prediction(
            prediction=int(row["prediction"]),
            probability_churn=float(prob[1]),
            probability_retain=float(prob[0]),
        )
        _PREDICTIONS_TOTAL.labels(predicted_class=str(pred.prediction)).inc()
        if AB_TESTING_ENABLED:
            _AB_PREDICTIONS_TOTAL.labels(variant=variant, predicted_class=str(pred.prediction)).inc()
        predictions.append(pred)
    # Shadow fires only in pure shadow mode — A/B routes to one model, no background twin
    if SHADOW_MODEL_ENABLED and not AB_TESTING_ENABLED:
        threading.Thread(
            target=_run_shadow_inference, args=(df.copy(), predictions), daemon=True
        ).start()
    return predictions


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
def health():
    return {
        "status": "ok",
        "model_loaded": _model is not None,
        "shadow_loaded": _shadow_model is not None,
        "ab_testing_enabled": AB_TESTING_ENABLED,
    }


@app.get("/model/info", tags=["ops"])
def model_info():
    _require_model()
    return {
        "model_name": MODEL_NAME,
        "model_stage": MODEL_STAGE,
        "model_uri": _model_uri,
        "feature_importances": _feature_importances,
        "shap_available": _shap_explainer is not None,
        "shadow_enabled": SHADOW_MODEL_ENABLED,
        "shadow_model_uri": _shadow_model_uri,
        "ab_testing_enabled": AB_TESTING_ENABLED,
        "ab_traffic_split": AB_TRAFFIC_SPLIT,
    }


@app.post("/model/reload", tags=["ops"])
def model_reload():
    """Hot-reload the Production model and its explanation artifacts from MLflow."""
    try:
        _load_model()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {"status": "reloaded", "model_uri": _model_uri}


@app.post("/model/shadow/reload", tags=["ops"])
def shadow_reload():
    """Hot-reload the shadow/challenger model from MLflow Staging."""
    if not SHADOW_MODEL_ENABLED and not AB_TESTING_ENABLED:
        raise HTTPException(
            status_code=404,
            detail="Neither shadow deployment nor A/B testing is enabled.",
        )
    _load_shadow_model()
    return {"status": "reloaded", "shadow_model_uri": _shadow_model_uri}


@app.post("/predict", response_model=PredictResponse, tags=["inference"])
def predict(request: Features):
    variant = _route_variant(request.msisdn)
    model = _select_model_for_variant(variant)
    input_df = pd.DataFrame([{k: v for k, v in request.model_dump().items() if k != "msisdn"}])
    predictions = _run_inference(model, input_df, variant)
    pred = predictions[0]
    _log_prediction(
        msisdn=request.msisdn,
        model_name=MODEL_NAME,
        model_stage=MODEL_STAGE,
        model_variant=variant,
        prediction=pred.prediction,
        probability_churn=pred.probability_churn,
        probability_retain=pred.probability_retain,
    )
    return PredictResponse(
        result=pred,
        model_variant=variant,
        model_name=MODEL_NAME,
        model_stage=MODEL_STAGE,
        model_uri=_model_uri,
    )


@app.post("/predict/batch", response_model=BatchPredictResponse, tags=["inference"])
def predict_batch(request: BatchPredictRequest):
    if not request.records:
        raise HTTPException(status_code=422, detail="records list must not be empty")
    variant = _route_variant(request.routing_msisdn)
    model = _select_model_for_variant(variant)
    input_df = pd.DataFrame([
        {k: v for k, v in r.model_dump().items() if k != "msisdn"}
        for r in request.records
    ])
    predictions = _run_inference(model, input_df, variant)
    for features, pred in zip(request.records, predictions):
        _log_prediction(
            msisdn=features.msisdn,
            model_name=MODEL_NAME,
            model_stage=MODEL_STAGE,
            model_variant=variant,
            prediction=pred.prediction,
            probability_churn=pred.probability_churn,
            probability_retain=pred.probability_retain,
        )
    return BatchPredictResponse(
        results=predictions,
        record_count=len(predictions),
        model_variant=variant,
        model_name=MODEL_NAME,
        model_stage=MODEL_STAGE,
        model_uri=_model_uri,
    )


@app.post("/predict/explain", response_model=ExplainResponse, tags=["inference"])
def predict_explain(request: Features):
    """Return a prediction with global feature importances and per-prediction SHAP values."""
    variant = _route_variant(request.msisdn)
    model = _select_model_for_variant(variant)
    input_df = pd.DataFrame([{k: v for k, v in request.model_dump().items() if k != "msisdn"}])
    predictions = _run_inference(model, input_df, variant)
    pred = predictions[0]
    _log_prediction(
        msisdn=request.msisdn,
        model_name=MODEL_NAME,
        model_stage=MODEL_STAGE,
        model_variant=variant,
        prediction=pred.prediction,
        probability_churn=pred.probability_churn,
        probability_retain=pred.probability_retain,
    )

    shap_values: Optional[Dict[str, float]] = None
    if _shap_explainer is not None:
        try:
            sv = _shap_explainer.shap_values(input_df[FEATURE_COLS], nsamples=100, silent=True)
            shap_values = {col: float(v) for col, v in zip(FEATURE_COLS, sv[0])}
        except Exception as exc:
            logger.warning("SHAP computation failed: %s", exc)

    return ExplainResponse(
        result=pred,
        model_variant=variant,
        feature_importances=_feature_importances,
        shap_values=shap_values,
        model_name=MODEL_NAME,
        model_stage=MODEL_STAGE,
        model_uri=_model_uri,
    )
