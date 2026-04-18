from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


NUMERIC_COLS = [
    "latitude",
    "longitude",
    "aqi",
    "pm25",
    "pm10",
    "co",
    "no2",
    "so2",
    "o3",
]


def normalize_air_quality(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingestion_time", F.to_timestamp("ingestion_time"))
        .withColumn("event_hour", F.date_trunc("hour", F.col("event_time")))
    )

    for c in NUMERIC_COLS:
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast("double"))

    return out
