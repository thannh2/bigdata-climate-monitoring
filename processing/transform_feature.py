import math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("Weather_Feature_Transformation_Full") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def col_or_null(df, column_name: str):
    return F.col(column_name) if column_name in df.columns else F.lit(None)

def main():
    spark = create_spark_session()
    
    input_hdfs = "hdfs://namenode:8020/user/data_engineer/raw_jsonl/*.jsonl"
    output_hdfs = "hdfs://namenode:8020/user/data_engineer/feature_store/feature/"

    print("[*] Đang đọc dữ liệu thô từ HDFS...")
    df = spark.read.json(input_hdfs)

    print("[*] Đang xử lý Lớp 0 (Bóc tách dữ liệu Kafka)...")
    # 1. Bóc tách dữ liệu từ lớp vỏ 'value' của Kafka
    df = df.select("value.*")

    # 2. Chuẩn hóa tên cột để đưa vào Feature Table
    df = df.select(
        F.col("station_id"),
        F.col("event_time").alias("timestamp"),
        F.col("latitude"), 
        F.col("longitude"),
        F.lit(0.0).alias("elevation"), # Giả lập cột elevation nếu Kafka không có
        F.col("temperature_c").alias("temp_c"),
        F.col("humidity"),
        F.col("pressure_hpa").alias("pressure"),
        F.col("wind_speed_mps").alias("wind_speed"),
        col_or_null(df, "wind_direction_deg").alias("wind_dir"),
        col_or_null(df, "precipitation_mm").alias("precipitation"),
        col_or_null(df, "cloud_cover_pct").alias("cloud_cover"),
        col_or_null(df, "shortwave_radiation_wm2").alias("shortwave_radiation"),
        col_or_null(df, "soil_temperature_0_to_7cm_c").alias("soil_temperature"),
        F.col("pm25").alias("pm2_5"),
        F.col("aqi").alias("us_aqi")
    ).na.fill(0)

    # 3. Ép kiểu thời gian
    df = df.withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
           .withColumn("hour", F.hour(F.col("timestamp"))) \
           .withColumn("month", F.month(F.col("timestamp"))) \
           .withColumn("year", F.year(F.col("timestamp")))

    print("[*] Đang tính toán Lớp 1 (L1) - Vật lý cơ bản...")
    # ================= L1 FEATURES =================
    lat_rad = F.col("latitude") * math.pi / 180
    lon_rad = F.col("longitude") * math.pi / 180
    
    df = df.withColumn("coord_X", F.cos(lat_rad) * F.cos(lon_rad)) \
           .withColumn("coord_Y", F.cos(lat_rad) * F.sin(lon_rad)) \
           .withColumn("coord_Z", F.sin(lat_rad))

    wind_dir_rad = F.col("wind_dir") * math.pi / 180
    df = df.withColumn("wind_U", -F.col("wind_speed") * F.sin(wind_dir_rad)) \
           .withColumn("wind_V", -F.col("wind_speed") * F.cos(wind_dir_rad))

    df = df.withColumn("air_density", (F.col("pressure") * 100) / (287.05 * (F.col("temp_c") + 273.15)))

    b, c = 17.625, 243.04
    gamma = F.log(F.col("humidity") / 100.0) + (b * F.col("temp_c")) / (c + F.col("temp_c"))
    df = df.withColumn("dew_point", (c * gamma) / (b - gamma))

    df = df.withColumn("hour_sin", F.sin(2 * math.pi * F.col("hour") / 24)) \
           .withColumn("hour_cos", F.cos(2 * math.pi * F.col("hour") / 24))

    print("[*] Đang tính toán Lớp 2 (L2) - Chỉ số kết hợp...")
    # ================= L2 FEATURES =================
    e_vapor = 6.112 * F.exp((17.67 * F.col("dew_point")) / (F.col("dew_point") + 243.5))
    mixing_ratio = 0.622 * e_vapor / (F.col("pressure") - e_vapor)
    theta = (F.col("temp_c") + 273.15) * F.pow(1000.0 / F.col("pressure"), 0.286)
    
    df = df.withColumn("theta_e", theta * F.exp((2675.0 * mixing_ratio) / (F.col("dew_point") + 273.15)))

    df = df.withColumn("is_stagnant_air", F.when((F.col("wind_speed") < 1.0) & (F.col("pressure") > 1010), 1).otherwise(0))
    df = df.withColumn("cooling_degree_days", F.when(F.col("temp_c") > 24, F.col("temp_c") - 24).otherwise(0))

    print("[*] Đang tính toán Lớp 3 (L3) - Trí nhớ thời gian...")
    # ================= L3 FEATURES =================
    window_spec = Window.partitionBy("station_id").orderBy("timestamp")
    window_6h = Window.partitionBy("station_id").orderBy("timestamp").rowsBetween(-5, 0)
    window_12h = Window.partitionBy("station_id").orderBy("timestamp").rowsBetween(-11, 0)

    df = df.withColumn("pressure_delta_3h", F.col("pressure") - F.lag("pressure", 3).over(window_spec)) \
           .withColumn("wind_shear_U", F.col("wind_U") - F.lag("wind_U", 1).over(window_spec)) \
           .withColumn("wind_shear_V", F.col("wind_V") - F.lag("wind_V", 1).over(window_spec)) \
           .withColumn("temp_mean_6h", F.avg("temp_c").over(window_6h)) \
           .withColumn("pm25_acc_12h", F.sum("pm2_5").over(window_12h))

    print("[*] Đang tính toán Lớp 4 (L4) - Biến mục tiêu (Targets)...")
    # ================= L4 FEATURES =================
    target_hours = [1, 2, 3, 4, 5, 6] 
    
    for n in target_hours:
        df = df.withColumn(f"target_temp_{n}h", F.lead("temp_c", n).over(window_spec))
        df = df.withColumn(f"target_pm25_{n}h", F.lead("pm2_5", n).over(window_spec))
        df = df.withColumn(f"target_cloud_cover_{n}h", F.lead("cloud_cover", n).over(window_spec))
        df = df.withColumn(f"target_precipitation_{n}h", F.lead("precipitation", n).over(window_spec))
        df = df.withColumn(f"target_wind_speed_{n}h", F.lead("wind_speed", n).over(window_spec))
        df = df.withColumn(f"target_pressure_{n}h", F.lead("pressure", n).over(window_spec))

    df_final = df.dropna(subset=[f"target_pm25_{target_hours[-1]}h"])

    print(f"[*] Đang lưu Dữ liệu Vàng xuống HDFS tại: {output_hdfs}")
    df_final.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_hdfs)

    print("[+] Hoàn tất Transform toàn bộ Feature Table!")
    spark.stop()

if __name__ == "__main__":
    main()
