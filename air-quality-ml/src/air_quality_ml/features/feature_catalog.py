from __future__ import annotations

from pyspark.sql import DataFrame


# L0 - Raw features từ transform_feature.py
BASE_NUMERIC_FEATURES = [
    "latitude",
    "longitude",
    "temp_c",
    "humidity",
    "pressure",
    "wind_speed",
    "wind_dir",
    "precipitation",
    "cloud_cover",
    "shortwave_radiation",
    "soil_temperature",
    "pm2_5",
    "us_aqi",
]

# L1 - Engineered features cơ bản
L1_FEATURES = [
    "coord_X",
    "coord_Y",
    "coord_Z",
    "wind_U",
    "wind_V",
    "air_density",
    "dew_point",
    "hour_sin",
    "hour_cos",
]

# L2 - Domain-specific features
L2_FEATURES = [
    "theta_e",
    "is_stagnant_air",
    "cooling_degree_days",
]

# L3 - Time-series features
L3_FEATURES = [
    "pressure_delta_3h",
    "wind_shear_U",
    "wind_shear_V",
    "temp_mean_6h",
    "pm25_acc_12h",
]

ENGINEERED_NUMERIC_FEATURES = L1_FEATURES + L2_FEATURES + L3_FEATURES

CATEGORICAL_FEATURES = [
    "region",
    "city",
]


def get_default_feature_columns(df: DataFrame, target_col: str) -> tuple[list[str], list[str]]:
    """
    Lấy danh sách features mặc định, loại bỏ metadata và target columns
    """
    excluded = {
        target_col,
        "timestamp",
        "station_id",
        "year",
        "month",
        "hour",
        "elevation",
    }
    
    # Loại bỏ tất cả target columns
    excluded.update([c for c in df.columns if c.startswith("target_")])

    numeric = [c for c in (BASE_NUMERIC_FEATURES + ENGINEERED_NUMERIC_FEATURES) if c in df.columns and c not in excluded]
    categorical = [c for c in CATEGORICAL_FEATURES if c in df.columns and c not in excluded]
    return numeric, categorical
