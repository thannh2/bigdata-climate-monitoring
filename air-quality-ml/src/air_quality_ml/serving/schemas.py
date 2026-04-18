from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class PredictionPayload(BaseModel):
    station_id: str
    region: str
    city: str
    event_time: datetime
    prediction_time: datetime
    horizon: int
    model_name: str
    model_version: str
    mlflow_run_id: Optional[str] = None
    pred_pm25: Optional[float] = None
    pred_alert_prob: Optional[float] = None
    pred_alert: Optional[int] = None
    alert_level: Optional[int] = None
