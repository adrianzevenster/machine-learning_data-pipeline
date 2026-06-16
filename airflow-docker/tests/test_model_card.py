import importlib.util
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_CARD_PATH = REPO_ROOT / "pySpark" / "model_card.py"


def load_model_card_module():
    spec = importlib.util.spec_from_file_location("model_card", MODEL_CARD_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_model_card_captures_lineage_and_features():
    model_card = load_model_card_module()

    card = model_card.build_model_card(
        pipeline_run_id="run-1",
        model_version_id="model-1",
        model_name="customer_churn_random_forest",
        registered_model_name="customer_churn_random_forest",
        algorithm="RandomForestClassifier",
        git_sha="abc123",
        image_tag="pyspark-app:latest",
        mlflow_run_id="mlflow-1",
        mlflow_model_uri="runs:/mlflow-1/model",
        artifact_uri="/tmp/model",
        data_start="2026-06-01",
        data_end="2026-06-10",
        features=["f1", "f2"],
        feature_hash="hash123",
        metrics={"auc": 0.7},
        params={"numTrees": 50},
        row_counts={"train_rows": 10, "test_rows": 5, "prediction_rows": 5},
        promotion_policy={"min_model_auc": "0.65"},
    )

    assert card["schema_version"] == "1.0"
    assert card["identity"]["model_version_id"] == "model-1"
    assert card["lineage"]["git_sha"] == "abc123"
    assert card["features"]["count"] == 2
    assert card["features"]["hash"] == "hash123"
    assert card["metrics"]["auc"] == 0.7
    assert card["promotion_policy"]["min_model_auc"] == "0.65"
