from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def missing_rate(df: DataFrame, columns: list[str]) -> DataFrame:
    metrics = [
        F.avg(F.when(F.col(c).isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias(f"missing_rate_{c}")
        for c in columns
        if c in df.columns
    ]
    return df.agg(*metrics)


def duplicate_rate(df: DataFrame, key_cols: list[str]) -> DataFrame:
    total = df.count()
    if total == 0:
        return df.sparkSession.createDataFrame([(0, 0, 0.0)], ["total_rows", "duplicate_rows", "duplicate_rate"])

    dup_rows = (
        df.groupBy(*key_cols)
        .count()
        .filter(F.col("count") > 1)
        .agg(F.sum(F.col("count") - F.lit(1)).alias("duplicate_rows"))
        .collect()[0]["duplicate_rows"]
        or 0
    )
    return df.sparkSession.createDataFrame(
        [(int(total), int(dup_rows), float(dup_rows) / float(total))],
        ["total_rows", "duplicate_rows", "duplicate_rate"],
    )


def ingestion_delay_stats(df: DataFrame, event_col: str = "event_hour", ingestion_col: str = "ingestion_time") -> DataFrame:
    delay_col = F.unix_timestamp(F.col(ingestion_col)) - F.unix_timestamp(F.col(event_col))
    return df.select(delay_col.alias("event_delay_seconds")).agg(
        F.avg("event_delay_seconds").alias("avg_delay_seconds"),
        F.expr("percentile_approx(event_delay_seconds, 0.95)").alias("p95_delay_seconds"),
        F.max("event_delay_seconds").alias("max_delay_seconds"),
    )


def station_silence(df: DataFrame, station_col: str = "station_id", event_col: str = "event_hour") -> DataFrame:
    latest = df.groupBy(station_col).agg(F.max(event_col).alias("last_event_time"))
    return latest.withColumn(
        "silence_minutes",
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("last_event_time"))) / F.lit(60.0),
    )
