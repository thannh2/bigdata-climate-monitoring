from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from air_quality_ml.utils.parquet_io import read_parquet_safe


def load_and_prepare_features(
    spark, 
    features_path: str, 
    use_pandas_workaround: Optional[bool] = None
) -> DataFrame:
    """
    Load features đã transform từ processing/transform_feature.py
    
    Args:
        spark: SparkSession
        features_path: Path to features parquet
        use_pandas_workaround: If True, use pandas to read (workaround for Windows winutils issue).
                              If None (default), auto-detect based on OS.
    
    Dữ liệu đã có:
    - station_id, timestamp, latitude, longitude
    - L0: temp_c, humidity, pressure, wind_speed, wind_dir, precipitation, cloud_cover, etc.
    - L1: coord_X/Y/Z, wind_U/V, air_density, dew_point, hour_sin/cos
    - L2: theta_e, is_stagnant_air, cooling_degree_days
    - L3: pressure_delta_3h, wind_shear_U/V, temp_mean_6h, pm25_acc_12h
    - L4: target_pm25_[1,6,12,24]h, target_temp_*, target_inversion_*, etc.
    """
    # Use centralized parquet reader with Windows compatibility
    df = read_parquet_safe(spark, features_path, use_pandas_workaround)
    
    # Đảm bảo timestamp đúng format
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

    if "year" not in df.columns:
        df = df.withColumn("year", F.year("timestamp"))
    if "month" not in df.columns:
        df = df.withColumn("month", F.month("timestamp"))
    if "hour" not in df.columns:
        df = df.withColumn("hour", F.hour("timestamp"))
    
    # Thêm cột region/city nếu chưa có (map từ station_id)
    # Giả định: station_id có format như "Hanoi", "Da Nang", etc.
    if "region" not in df.columns:
        df = df.withColumn(
            "region",
            F.when(F.col("station_id").isin("Hanoi", "Hai Phong"), F.lit("bac"))
            .when(F.col("station_id").isin("Hue", "Da Nang"), F.lit("trung"))
            .when(F.col("station_id").isin("HCMC", "Can Tho"), F.lit("nam"))
            .otherwise(F.lit("unknown"))
        )
    
    if "city" not in df.columns:
        df = df.withColumn("city", F.col("station_id"))
    
    return df


def select_features_for_training(
    df: DataFrame,
    target_col: str,
    exclude_targets: bool = True
) -> DataFrame:
    """
    Chọn features phù hợp cho training, loại bỏ các cột không cần thiết
    """
    # Các cột metadata không dùng làm feature
    exclude_cols = {
        "timestamp", "station_id", "year", "month", "hour",
        "elevation",  # Giả lập, không có thật
    }
    
    # Loại bỏ các target khác nếu cần
    if exclude_targets:
        target_cols = [c for c in df.columns if c.startswith("target_")]
        exclude_cols.update(target_cols)
        # Giữ lại target hiện tại
        exclude_cols.discard(target_col)
    
    # Chọn các cột còn lại
    feature_cols = [c for c in df.columns if c not in exclude_cols]
    
    return df.select(*feature_cols)


def add_alert_target_from_pm25(
    df: DataFrame,
    pm25_target_col: str,
    alert_target_col: str,
    threshold: float = 35.0
) -> DataFrame:
    """
    Tạo target alert từ target PM2.5 nếu chưa có
    """
    if alert_target_col in df.columns:
        return df
    
    return df.withColumn(
        alert_target_col,
        F.when(F.col(pm25_target_col) >= F.lit(threshold), F.lit(1))
        .otherwise(F.lit(0))
        .cast("int")
    )
