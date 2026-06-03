import math
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def apply_l1_l2_transforms(df):
    """Tính toán Lớp 1 và Lớp 2 (Stateless) - Dùng chung cho Batch & Stream"""
    lat_rad = F.col("latitude") * math.pi / 180
    lon_rad = F.col("longitude") * math.pi / 180
    wind_dir_rad = F.col("wind_dir") * math.pi / 180

    df = df.withColumn("coord_X", F.cos(lat_rad) * F.cos(lon_rad)) \
           .withColumn("coord_Y", F.cos(lat_rad) * F.sin(lon_rad)) \
           .withColumn("coord_Z", F.sin(lat_rad)) \
           .withColumn("wind_U", -F.col("wind_speed") * F.sin(wind_dir_rad)) \
           .withColumn("wind_V", -F.col("wind_speed") * F.cos(wind_dir_rad))

    df = df.withColumn("air_density", (F.col("pressure") * 100) / (287.05 * (F.col("temp_c") + 273.15)))

    b, c = 17.625, 243.04
    gamma = F.log(F.col("humidity") / 100.0) + (b * F.col("temp_c")) / (c + F.col("temp_c"))
    df = df.withColumn("dew_point", (c * gamma) / (b - gamma))

    # Cập nhật thời gian thực
    if "hour" in df.columns and "day_of_year" in df.columns:
        df = df.withColumn("hour_sin", F.sin(2 * math.pi * F.col("hour") / 24)) \
               .withColumn("hour_cos", F.cos(2 * math.pi * F.col("hour") / 24)) \
               .withColumn("day_sin",  F.sin(2 * math.pi * F.col("day_of_year") / 365))

    e_vapor = 6.112 * F.exp((17.67 * F.col("dew_point")) / (F.col("dew_point") + 243.5))
    mixing_ratio = 0.622 * e_vapor / (F.col("pressure") - e_vapor)
    theta = (F.col("temp_c") + 273.15) * F.pow(1000.0 / F.col("pressure"), 0.286)
    
    df = df.withColumn("theta_e", theta * F.exp((2675.0 * mixing_ratio) / (F.col("dew_point") + 273.15)))
    df = df.withColumn("is_stagnant_air", F.when((F.col("wind_speed") < 1.0) & (F.col("pressure") > 1010), 1).otherwise(0))
    df = df.withColumn("cooling_degree_days", F.when(F.col("temp_c") > 24, F.col("temp_c") - 24).otherwise(0))
    
    return df

def apply_l3_transforms(df):
    """Tính toán Lớp 3 (Stateful/Window)"""
    window_spec = Window.partitionBy("station_id").orderBy("timestamp")
    window_6h = Window.partitionBy("station_id").orderBy("timestamp").rowsBetween(-5, 0)
    window_12h = Window.partitionBy("station_id").orderBy("timestamp").rowsBetween(-11, 0)

    return df.withColumn("pressure_delta_3h", F.col("pressure") - F.lag("pressure", 3).over(window_spec)) \
             .withColumn("wind_shear_U", F.col("wind_U") - F.lag("wind_U", 1).over(window_spec)) \
             .withColumn("wind_shear_V", F.col("wind_V") - F.lag("wind_V", 1).over(window_spec)) \
             .withColumn("temp_mean_6h", F.avg("temp_c").over(window_6h)) \
             .withColumn("pm25_acc_12h", F.sum("pm2_5").over(window_12h))