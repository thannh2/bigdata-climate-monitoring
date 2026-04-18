from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import mlflow


def _write_temp_json(file_name: str, payload: Any) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="aqml_artifacts_"))
    out = temp_dir / file_name
    out.write_text(json.dumps(payload, ensure_ascii=True, indent=2, default=str), encoding="utf-8")
    return out


def log_json_artifact(file_name: str, payload: Any, artifact_path: str = "artifacts") -> None:
    path = _write_temp_json(file_name, payload)
    mlflow.log_artifact(str(path), artifact_path=artifact_path)


def log_feature_importance(feature_importance: list[dict[str, Any]]) -> None:
    log_json_artifact("feature_importance.json", feature_importance, artifact_path="analysis")
