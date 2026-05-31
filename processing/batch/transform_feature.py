import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from processing.utils.utils_features import apply_l1_l2_transforms, apply_l3_transforms
from processing.utils.utils_l4_target import apply_l4_targets

def main():
    spark = SparkSession.builder \
        .appName("Weather_Feature_Transformation_Batch") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    input_hdfs = "hdfs://namenode:8020/user/data_engineer/raw_jsonl/*.jsonl"
    output_hdfs = "hdfs://namenode:8020/user/data_engineer/feature_store/feature/"

    print("[*] Đang đọc dữ liệu thô từ HDFS...")
    df = spark.read.json(input_hdfs).select("value.*")

    # Chuẩn hóa cột L0
    df = df.select(
        F.col("station_id"),
        F.col("event_time").alias("timestamp"),
        F.col("latitude"), F.col("longitude"),
        F.lit(0.0).alias("elevation"),
        F.col("temperature_c").alias("temp_c"),
        F.col("humidity"), F.col("pressure_hpa").alias("pressure"),
        F.col("wind_speed_mps").alias("wind_speed"),
        F.col("wind_direction_deg").alias("wind_dir"),
        F.col("precipitation_mm").alias("precipitation"),
        F.col("cloud_cover_pct").alias("cloud_cover"),
        F.col("shortwave_radiation_wm2").alias("shortwave_radiation"),
        F.col("soil_temperature_0_to_7cm_c").alias("soil_temperature"),
        F.col("pm25").alias("pm2_5"), F.col("aqi").alias("us_aqi")
    ).na.fill(0)

    # Ép kiểu thời gian
    df = df.withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
           .withColumn("hour", F.hour(F.col("timestamp"))) \
           .withColumn("month", F.month(F.col("timestamp"))) \
           .withColumn("day_of_year", F.dayofyear(F.col("timestamp"))) \
           .withColumn("year", F.year(F.col("timestamp")))

    print("[*] Áp dụng các phép biến đổi L1, L2, L3, L4...")
    df = apply_l1_l2_transforms(df)
    df = apply_l3_transforms(df)
    df_final = apply_l4_targets(df)

    print(f"[*] Lưu Dữ liệu Vàng xuống HDFS: {output_hdfs}")
    df_final.write.mode("overwrite").partitionBy("year", "month").parquet(output_hdfs)
    print("[+] Hoàn tất Batch Job!")
    spark.stop()

if __name__ == "__main__":
    main()