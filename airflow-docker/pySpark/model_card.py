from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional


def build_model_card(
    *,
    pipeline_run_id: str,
    model_version_id: str,
    model_name: str,
    registered_model_name: str,
    algorithm: str,
    git_sha: str,
    image_tag: str,
    mlflow_run_id: str,
    mlflow_model_uri: str,
    artifact_uri: str,
    data_start: Optional[str],
    data_end: Optional[str],
    features: Iterable[str],
    feature_hash: str,
    metrics: Dict[str, Any],
    params: Dict[str, Any],
    row_counts: Dict[str, int],
    promotion_policy: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    feature_list = list(features)
    return {
        "schema_version": "1.0",
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "identity": {
            "pipeline_run_id": pipeline_run_id,
            "model_version_id": model_version_id,
            "model_name": model_name,
            "registered_model_name": registered_model_name,
            "algorithm": algorithm,
        },
        "lineage": {
            "git_sha": git_sha,
            "image_tag": image_tag,
            "mlflow_run_id": mlflow_run_id,
            "mlflow_model_uri": mlflow_model_uri,
            "artifact_uri": artifact_uri,
            "data_start": data_start or None,
            "data_end": data_end or None,
        },
        "features": {
            "count": len(feature_list),
            "hash": feature_hash,
            "columns": feature_list,
        },
        "metrics": metrics,
        "parameters": params,
        "row_counts": row_counts,
        "promotion_policy": promotion_policy or {},
    }
