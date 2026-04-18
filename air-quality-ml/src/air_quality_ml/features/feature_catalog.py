from __future__ import annotations

from pyspark.sql import DataFrame


BASE_NUMERIC_FEATURES = [
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
    "pm25",
    "pm10",
    "aqi",
    "co",
    "no2",
    "so2",
    "o3",
]

ENGINEERED_NUMERIC_FEATURES = [
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "coord_x",
    "coord_y",
    "coord_z",
    "wind_u",
    "wind_v",
    "dew_point",
    "is_stagnant_air",
    "pressure_delta_3h",
    "pm25_lag_1h",
    "pm25_lag_3h",
    "pm25_lag_6h",
    "aqi_lag_1h",
    "aqi_lag_6h",
    "temp_mean_6h",
    "humidity_mean_6h",
    "wind_speed_mean_6h",
    "pm25_acc_12h",
]

CATEGORICAL_FEATURES = [
    "region",
    "city",
    "weather_main",
]


def get_default_feature_columns(df: DataFrame, target_col: str) -> tuple[list[str], list[str]]:
    excluded = {
        target_col,
        "event_hour",
        "ingestion_time",
        "station_id",
        "weather_data_version",
        "air_data_version",
    }

    numeric = [c for c in (BASE_NUMERIC_FEATURES + ENGINEERED_NUMERIC_FEATURES) if c in df.columns and c not in excluded]
    categorical = [c for c in CATEGORICAL_FEATURES if c in df.columns and c not in excluded]
    return numeric, categorical
