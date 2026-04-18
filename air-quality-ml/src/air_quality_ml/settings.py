from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseModel, Field


class SparkConfig(BaseModel):
    app_name: str = "air-quality-ml"
    master: str = "local[*]"
    shuffle_partitions: int = 16
    session_timezone: str = "UTC"


class DataPaths(BaseModel):
    silver_weather_path: str
    silver_air_path: str
    gold_features_path: str
    gold_targets_path: str
    gold_predictions_path: str
    gold_eval_path: str
    monitoring_path: str


class MlflowConfig(BaseModel):
    tracking_uri: str = "http://localhost:5000"
    experiment_root: str = "air-quality"
    registry_uri: Optional[str] = None


class SplitConfig(BaseModel):
    timestamp_col: str = "event_hour"
    train_end: str
    val_end: str


class PromotionConfig(BaseModel):
    regression_mae_improve_ratio: float = 0.95
    max_mae_high_pollution_ratio: float = 1.0
    classification_min_recall: float = 0.80
    classification_min_auprc_ratio: float = 1.0
    classification_max_fnr: float = 0.20


class FeatureDefaults(BaseModel):
    horizons: list[int] = Field(default_factory=lambda: [1, 3, 6])
    alert_pm25_threshold: float = 35.0
    high_pollution_threshold: float = 75.0


class BaseSettings(BaseModel):
    project_name: str = "air-quality-ml"
    environment: str = "dev"
    owner: str = "unknown"
    spark: SparkConfig
    data: DataPaths
    mlflow: MlflowConfig
    features: FeatureDefaults
    split: SplitConfig
    promotion: PromotionConfig


class JobConfig(BaseModel):
    task: str
    horizon: int
    target_col: str
    prediction_col: str = "prediction"
    probability_col: Optional[str] = "probability"
    model_type: str
    model_name: str
    experiment_name: str
    training: Dict[str, Any] = Field(default_factory=dict)
    params: Dict[str, Any] = Field(default_factory=dict)
    threshold_tuning: Dict[str, Any] = Field(default_factory=dict)
    register_if_pass: bool = True


def read_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML at {path} must be a mapping")
    return data


def load_base_settings(config_path: str | Path) -> BaseSettings:
    config_file = Path(config_path)
    return BaseSettings.model_validate(read_yaml(config_file))


def load_job_config(config_path: str | Path) -> JobConfig:
    config_file = Path(config_path)
    return JobConfig.model_validate(read_yaml(config_file))


def resolve_path(base_dir: str | Path, path_value: str) -> Path:
    p = Path(path_value)
    if p.is_absolute():
        return p
    return (Path(base_dir) / p).resolve()
