from __future__ import annotations

import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_hourly_features(df: DataFrame) -> DataFrame:
    w_station = Window.partitionBy("station_id").orderBy("event_hour")
    w_6h = w_station.rowsBetween(-5, 0)
    w_12h = w_station.rowsBetween(-11, 0)

    out = (
        df.withColumn("hour_of_day", F.hour("event_hour"))
        .withColumn("day_of_week", F.dayofweek("event_hour"))
        .withColumn("hour_sin", F.sin(F.col("hour_of_day") * F.lit(2.0 * math.pi / 24.0)))
        .withColumn("hour_cos", F.cos(F.col("hour_of_day") * F.lit(2.0 * math.pi / 24.0)))
        .withColumn("dow_sin", F.sin(F.col("day_of_week") * F.lit(2.0 * math.pi / 7.0)))
        .withColumn("dow_cos", F.cos(F.col("day_of_week") * F.lit(2.0 * math.pi / 7.0)))
        .withColumn("lat_rad", F.radians("latitude"))
        .withColumn("lon_rad", F.radians("longitude"))
        .withColumn("coord_x", F.cos("lat_rad") * F.cos("lon_rad"))
        .withColumn("coord_y", F.cos("lat_rad") * F.sin("lon_rad"))
        .withColumn("coord_z", F.sin("lat_rad"))
        .withColumn("wind_u", F.col("wind_speed_mps") * F.cos(F.radians("wind_direction_deg")))
        .withColumn("wind_v", F.col("wind_speed_mps") * F.sin(F.radians("wind_direction_deg")))
        .withColumn("dew_point", F.col("temperature_c") - (F.lit(100.0) - F.col("humidity")) / F.lit(5.0))
        .withColumn(
            "is_stagnant_air",
            F.when(
                (F.col("wind_speed_mps") <= F.lit(2.0)) & (F.coalesce(F.col("precipitation_mm"), F.lit(0.0)) < F.lit(0.5)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("pm25_lag_1h", F.lag("pm25", 1).over(w_station))
        .withColumn("pm25_lag_3h", F.lag("pm25", 3).over(w_station))
        .withColumn("pm25_lag_6h", F.lag("pm25", 6).over(w_station))
        .withColumn("aqi_lag_1h", F.lag("aqi", 1).over(w_station))
        .withColumn("aqi_lag_6h", F.lag("aqi", 6).over(w_station))
        .withColumn("pressure_delta_3h", F.col("pressure_hpa") - F.lag("pressure_hpa", 3).over(w_station))
        .withColumn("temp_mean_6h", F.avg("temperature_c").over(w_6h))
        .withColumn("humidity_mean_6h", F.avg("humidity").over(w_6h))
        .withColumn("wind_speed_mean_6h", F.avg("wind_speed_mps").over(w_6h))
        .withColumn("pm25_acc_12h", F.sum("pm25").over(w_12h))
        .drop("lat_rad", "lon_rad")
    )

    return out
