from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def add_pm25_targets(df: DataFrame, horizons: Iterable[int]) -> DataFrame:
    w = Window.partitionBy("station_id").orderBy("event_hour")
    out = df
    for h in horizons:
        out = out.withColumn(f"target_pm25_{h}h", F.lead("pm25", int(h)).over(w))
    return out


def _threshold_for_horizon(threshold: float | dict[str, float], horizon: int) -> float:
    if isinstance(threshold, dict):
        if str(horizon) in threshold:
            return float(threshold[str(horizon)])
        return float(threshold.get("default", 35.0))
    return float(threshold)


def add_alert_targets(
    df: DataFrame,
    horizons: Iterable[int],
    threshold: float | dict[str, float],
) -> DataFrame:
    out = df
    for h in horizons:
        pm25_target_col = f"target_pm25_{h}h"
        if pm25_target_col not in out.columns:
            out = out.withColumn(pm25_target_col, F.lead("pm25", int(h)).over(Window.partitionBy("station_id").orderBy("event_hour")))

        alert_col = f"target_alert_{h}h"
        h_threshold = _threshold_for_horizon(threshold, int(h))
        out = out.withColumn(
            alert_col,
            F.when(F.col(pm25_target_col) >= F.lit(h_threshold), F.lit(1)).otherwise(F.lit(0)).cast("int"),
        )

    return out
