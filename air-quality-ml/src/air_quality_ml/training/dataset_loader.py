from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_training_table(spark: SparkSession, path: str, max_rows: int | None = None) -> DataFrame:
    df = spark.read.parquet(path)
    if max_rows and max_rows > 0:
        return df.limit(int(max_rows))
    return df


def prepare_training_frame(df: DataFrame, target_col: str, dropna_label: bool = True) -> DataFrame:
    if target_col not in df.columns:
        raise ValueError(f"Target column not found: {target_col}")

    out = df
    if dropna_label:
        out = out.filter(F.col(target_col).isNotNull())

    out = out.filter(F.col("event_hour").isNotNull())
    return out
