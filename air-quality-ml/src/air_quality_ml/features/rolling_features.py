from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def add_rolling_mean(df: DataFrame, by_col: str, order_col: str, feature_col: str, window_size: int, suffix: str) -> DataFrame:
    if window_size < 1:
        raise ValueError("window_size must be >= 1")

    w = Window.partitionBy(by_col).orderBy(order_col).rowsBetween(-(window_size - 1), 0)
    return df.withColumn(f"{feature_col}_mean_{suffix}", F.avg(feature_col).over(w))


def add_rolling_std(df: DataFrame, by_col: str, order_col: str, feature_col: str, window_size: int, suffix: str) -> DataFrame:
    if window_size < 2:
        raise ValueError("window_size must be >= 2")

    w = Window.partitionBy(by_col).orderBy(order_col).rowsBetween(-(window_size - 1), 0)
    return df.withColumn(f"{feature_col}_std_{suffix}", F.stddev(feature_col).over(w))
