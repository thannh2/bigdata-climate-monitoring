from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from air_quality_ml.utils.parquet_io import read_dataset_safe


def load_training_table(
    spark: SparkSession,
    path: str,
    dataset_format: str = "parquet",
    max_rows: int | None = None,
) -> DataFrame:
    df = read_dataset_safe(spark, path, dataset_format=dataset_format)
    if max_rows and max_rows > 0:
        return df.limit(int(max_rows))
    return df


def prepare_training_frame(df: DataFrame, target_col: str, dropna_label: bool = True) -> DataFrame:
    if target_col not in df.columns:
        raise ValueError(f"Target column not found: {target_col}")

    out = df
    if dropna_label:
        out = out.filter(F.col(target_col).isNotNull())

    # Sử dụng timestamp thay vì event_hour
    out = out.filter(F.col("timestamp").isNotNull())
    return out
