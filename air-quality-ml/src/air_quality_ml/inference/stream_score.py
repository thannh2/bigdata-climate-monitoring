from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def stream_score_job(spark: SparkSession, source_df: DataFrame) -> DataFrame:
    """Skeleton for structured-streaming scoring.

    Ban co the mo rong ham nay de:
    - doc tu Kafka/Delta source streaming,
    - load model tu MLflow Registry,
    - score tren micro-batch,
    - ghi ket qua vao lake va MongoDB.
    """
    return source_df
