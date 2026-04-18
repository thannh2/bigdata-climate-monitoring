from __future__ import annotations

from typing import Any


def build_prediction_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "station_id": row.get("station_id"),
        "region": row.get("region"),
        "city": row.get("city"),
        "event_time": row.get("event_hour"),
        "prediction_time": row.get("prediction_time"),
        "horizon": row.get("horizon"),
        "model_name": row.get("model_name"),
        "model_version": row.get("model_version"),
        "mlflow_run_id": row.get("mlflow_run_id"),
        "pred_pm25": row.get("prediction"),
        "pred_alert_prob": row.get("pred_prob"),
        "pred_alert": row.get("pred_alert"),
        "alert_level": row.get("alert_level"),
    }
