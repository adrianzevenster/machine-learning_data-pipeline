import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import List, Optional

import mlflow.pyfunc
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.getenv("MLFLOW_REGISTERED_MODEL_NAME", "customer_churn_random_forest")
MODEL_STAGE = os.getenv("MODEL_PROMOTION_STAGE", "Production")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# The 6 features produced by PySparkAnalysis.py → Processed_Data table.
# M_TENURE_CHURN is excluded (it is the label, not a feature).
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


def _load_model() -> None:
    global _model, _model_uri
    uri = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
    logger.info("Loading model from %s", uri)
    loaded = mlflow.pyfunc.load_model(uri)
    with _model_lock:
        _model = loaded
        _model_uri = uri
    logger.info("Model loaded: %s", uri)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        _load_model()
    except Exception as exc:
        logger.warning("No '%s' model available at startup: %s. Call POST /model/reload once promoted.", MODEL_STAGE, exc)
    yield


app = FastAPI(
    title="Churn Prediction Service",
    description="Serves the promoted customer_churn_random_forest model from MLflow.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Request / response schemas ────────────────────────────────────────────────

class Features(BaseModel):
    M_Out_Call_Count: float = Field(..., description="Number of outgoing calls in the aggregation window")
    M_Out_Call_Time:  float = Field(..., description="Total duration of outgoing calls (seconds)")
    M_Data_Sum:       float = Field(..., description="Total data volume consumed (bytes)")
    M_Data_Count:     int   = Field(..., description="Number of data sessions")
    M_In_Call_Count:  int   = Field(..., description="Number of incoming calls")
    M_In_Call_Time:   float = Field(..., description="Total duration of incoming calls (seconds)")


class Prediction(BaseModel):
    prediction:         int   = Field(..., description="0 = not churned, 1 = churned")
    probability_churn:  float = Field(..., description="P(churn=1)")
    probability_retain: float = Field(..., description="P(churn=0)")


class PredictResponse(BaseModel):
    result:      Prediction
    model_name:  str
    model_stage: str
    model_uri:   str


class BatchPredictRequest(BaseModel):
    records: List[Features]


class BatchPredictResponse(BaseModel):
    results:     List[Prediction]
    record_count: int
    model_name:  str
    model_stage: str
    model_uri:   str


# ── Internal helpers ──────────────────────────────────────────────────────────

def _require_model() -> mlflow.pyfunc.PyFuncModel:
    with _model_lock:
        if _model is None:
            raise HTTPException(
                status_code=503,
                detail=f"No '{MODEL_STAGE}' model loaded. POST /model/reload after promoting a model.",
            )
        return _model


def _run_inference(model: mlflow.pyfunc.PyFuncModel, df: pd.DataFrame) -> pd.DataFrame:
    # The training pipeline includes a StringIndexer fitted on M_TENURE_CHURN.
    # It must be present at inference time even though its output is not used
    # for prediction — the fitted RF ignores the label column entirely.
    df = df.copy()
    df["M_TENURE_CHURN"] = 0.0

    result = model.predict(df)

    # MLflow's Spark pyfunc returns the full Spark DataFrame as pandas.
    # The probability column is a numpy array [prob_class_0, prob_class_1].
    predictions = []
    for _, row in result.iterrows():
        prob = row["probability"]
        prob_0 = float(prob[0])
        prob_1 = float(prob[1])
        predictions.append(
            Prediction(
                prediction=int(row["prediction"]),
                probability_churn=prob_1,
                probability_retain=prob_0,
            )
        )
    return predictions


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok", "model_loaded": _model is not None}


@app.get("/model/info", tags=["ops"])
def model_info():
    _require_model()
    return {"model_name": MODEL_NAME, "model_stage": MODEL_STAGE, "model_uri": _model_uri}


@app.post("/model/reload", tags=["ops"])
def model_reload():
    """Hot-reload the Production model from MLflow without restarting the container."""
    try:
        _load_model()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {"status": "reloaded", "model_uri": _model_uri}


@app.post("/predict", response_model=PredictResponse, tags=["inference"])
def predict(request: Features):
    model = _require_model()
    input_df = pd.DataFrame([request.model_dump()])
    predictions = _run_inference(model, input_df)
    return PredictResponse(
        result=predictions[0],
        model_name=MODEL_NAME,
        model_stage=MODEL_STAGE,
        model_uri=_model_uri,
    )


@app.post("/predict/batch", response_model=BatchPredictResponse, tags=["inference"])
def predict_batch(request: BatchPredictRequest):
    if not request.records:
        raise HTTPException(status_code=422, detail="records list must not be empty")
    model = _require_model()
    input_df = pd.DataFrame([r.model_dump() for r in request.records])
    predictions = _run_inference(model, input_df)
    return BatchPredictResponse(
        results=predictions,
        record_count=len(predictions),
        model_name=MODEL_NAME,
        model_stage=MODEL_STAGE,
        model_uri=_model_uri,
    )
