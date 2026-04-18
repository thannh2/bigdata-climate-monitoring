from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def deduplicate_latest(df: DataFrame, key_cols: list[str], order_col: str) -> DataFrame:
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def join_weather_air(weather_df: DataFrame, air_df: DataFrame) -> DataFrame:
    weather_latest = deduplicate_latest(weather_df, ["station_id", "event_hour"], "ingestion_time")
    air_latest = deduplicate_latest(air_df, ["station_id", "event_hour"], "ingestion_time")

    w = weather_latest.alias("w")
    a = air_latest.alias("a")

    joined = w.join(
        a,
        on=[F.col("w.station_id") == F.col("a.station_id"), F.col("w.event_hour") == F.col("a.event_hour")],
        how="inner",
    )

    return joined.select(
        F.coalesce(F.col("a.station_id"), F.col("w.station_id")).alias("station_id"),
        F.coalesce(F.col("a.region"), F.col("w.region")).alias("region"),
        F.coalesce(F.col("a.city"), F.col("w.city")).alias("city"),
        F.coalesce(F.col("a.latitude"), F.col("w.latitude")).alias("latitude"),
        F.coalesce(F.col("a.longitude"), F.col("w.longitude")).alias("longitude"),
        F.col("w.event_hour").alias("event_hour"),
        F.greatest(F.col("w.ingestion_time"), F.col("a.ingestion_time")).alias("ingestion_time"),
        F.col("w.temperature_c"),
        F.col("w.humidity"),
        F.col("w.pressure_hpa"),
        F.col("w.surface_pressure_hpa"),
        F.col("w.wind_speed_mps"),
        F.col("w.wind_direction_deg"),
        F.col("w.precipitation_mm"),
        F.col("w.cloud_cover_pct"),
        F.col("w.shortwave_radiation_wm2"),
        F.col("w.soil_temperature_0_to_7cm_c"),
        F.col("w.weather_main"),
        F.col("w.weather_desc"),
        F.col("a.aqi"),
        F.col("a.quality_level"),
        F.col("a.pm25"),
        F.col("a.pm10"),
        F.col("a.co"),
        F.col("a.no2"),
        F.col("a.so2"),
        F.col("a.o3"),
        F.col("w.data_version").alias("weather_data_version"),
        F.col("a.data_version").alias("air_data_version"),
    )
