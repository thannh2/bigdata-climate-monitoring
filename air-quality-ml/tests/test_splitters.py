from __future__ import annotations

from pyspark.sql import functions as F

from air_quality_ml.training.splitters import time_based_split


def test_time_based_split_returns_non_empty_partitions(spark):
    data = [
        ("s1", "2026-01-01 00:00:00", 10.0),
        ("s1", "2026-02-01 00:00:00", 11.0),
        ("s1", "2026-03-01 00:00:00", 12.0),
    ]
    df = spark.createDataFrame(data, ["station_id", "timestamp", "value"]).withColumn("timestamp", F.to_timestamp("timestamp"))

    train, val, test = time_based_split(
        df,
        timestamp_col="timestamp",
        train_end="2026-01-31 23:59:59",
        val_end="2026-02-28 23:59:59",
    )

    assert train.count() == 1
    assert val.count() == 1
    assert test.count() == 1
