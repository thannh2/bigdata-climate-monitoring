from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def time_based_split(df: DataFrame, timestamp_col: str, train_end: str, val_end: str) -> tuple[DataFrame, DataFrame, DataFrame]:
    train = df.filter(F.col(timestamp_col) <= F.to_timestamp(F.lit(train_end)))
    val = df.filter(
        (F.col(timestamp_col) > F.to_timestamp(F.lit(train_end)))
        & (F.col(timestamp_col) <= F.to_timestamp(F.lit(val_end)))
    )
    test = df.filter(F.col(timestamp_col) > F.to_timestamp(F.lit(val_end)))
    return train, val, test
