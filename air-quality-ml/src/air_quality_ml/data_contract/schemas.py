from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


REQUIRED_COLUMNS = [
    "station_id",
    "region",
    "city",
    "event_time",
    "ingestion_time",
    "latitude",
    "longitude",
    "data_version",
]


WEATHER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("ingestion_time", TimestampType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure_hpa", DoubleType(), True),
        StructField("surface_pressure_hpa", DoubleType(), True),
        StructField("wind_speed_mps", DoubleType(), True),
        StructField("wind_direction_deg", DoubleType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("cloud_cover_pct", DoubleType(), True),
        StructField("shortwave_radiation_wm2", DoubleType(), True),
        StructField("soil_temperature_0_to_7cm_c", DoubleType(), True),
        StructField("weather_main", StringType(), True),
        StructField("weather_desc", StringType(), True),
        StructField("data_version", StringType(), True),
    ]
)


AIR_QUALITY_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("ingestion_time", TimestampType(), True),
        StructField("aqi", DoubleType(), True),
        StructField("quality_level", StringType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("co", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("so2", DoubleType(), True),
        StructField("o3", DoubleType(), True),
        StructField("data_version", StringType(), True),
    ]
)
