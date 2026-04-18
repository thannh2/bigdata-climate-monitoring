from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def add_lags(df: DataFrame, by_col: str, order_col: str, feature_col: str, lags: Iterable[int]) -> DataFrame:
    w = Window.partitionBy(by_col).orderBy(order_col)
    out = df
    for lag_h in lags:
        out = out.withColumn(f"{feature_col}_lag_{lag_h}h", F.lag(feature_col, int(lag_h)).over(w))
    return out
