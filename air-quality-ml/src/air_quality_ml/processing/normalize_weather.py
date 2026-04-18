from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


NUMERIC_COLS = [
    "latitude",
    "longitude",
    "temperature_c",
    "humidity",
    "pressure_hpa",
    "surface_pressure_hpa",
    "wind_speed_mps",
    "wind_direction_deg",
    "precipitation_mm",
    "cloud_cover_pct",
    "shortwave_radiation_wm2",
    "soil_temperature_0_to_7cm_c",
]


def normalize_weather(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingestion_time", F.to_timestamp("ingestion_time"))
        .withColumn("event_hour", F.date_trunc("hour", F.col("event_time")))
    )

    for c in NUMERIC_COLS:
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast("double"))

    return out
