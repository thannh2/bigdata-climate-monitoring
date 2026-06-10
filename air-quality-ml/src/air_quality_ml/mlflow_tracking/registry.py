from __future__ import annotations

from typing import Optional

import mlflow


def try_register_logged_model(model_name: str, run_id: str, artifact_path: str = "model") -> Optional[str]:
    model_uri = f"runs:/{run_id}/{artifact_path}"
    try:
        mv = mlflow.register_model(model_uri=model_uri, name=model_name)
        return str(mv.version)
    except Exception:
        return None


def try_transition_stage(model_name: str, version: str, stage: str) -> bool:
    try:
        client = mlflow.tracking.MlflowClient()
        client.transition_model_version_stage(
            name=model_name,
            version=version,
            stage=stage,
            archive_existing_versions=False,
        )
        return True
    except Exception:
        return False
