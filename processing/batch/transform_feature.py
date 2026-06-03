import sys
import os
from dotenv import load_dotenv 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from processing.utils.utils_features import apply_l1_l2_transforms, apply_l3_transforms
from processing.utils.utils_l4_target import apply_l4_targets
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

# ==========================================
# CÁC HÀM XỬ LÝ THÀNH PHẦN (TRANSFORMATIONS)
# ==========================================

def extract_and_spatial_merge(df_raw: DataFrame) -> DataFrame:
    """Kiểm tra định dạng JSON, chuẩn hóa L0 và thực hiện Spatial Merge."""
    if "value" in df_raw.columns:
        df = df_raw.select("value.*")
    else:
        df = df_raw

    df_selected = df.select(
        F.lower(F.trim(F.col("city"))).alias("station_id"), 
        F.col("event_time").alias("timestamp"),
        F.col("latitude"), 
        F.col("longitude"),
        F.lit(0.0).alias("elevation"),
        F.col("temperature_c").alias("temp_c"),
        F.col("humidity"), 
        F.col("pressure_hpa").alias("pressure"),
        F.col("wind_speed_mps").alias("wind_speed"),
        F.col("wind_direction_deg").alias("wind_dir"),
        F.col("precipitation_mm").alias("precipitation"),
        F.col("cloud_cover_pct").alias("cloud_cover"),
        F.col("shortwave_radiation_wm2").alias("shortwave_radiation"),
        F.col("soil_temperature_0_to_7cm_c").alias("soil_temperature"),
        F.col("pm25").alias("pm2_5") if "pm25" in df.columns else F.lit(None).cast("double").alias("pm2_5"),
        F.col("aqi").alias("us_aqi") if "aqi" in df.columns else F.lit(None).cast("double").alias("us_aqi")
    )

    df_time_features = df_selected.withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
        .withColumn("hour", F.hour(F.col("timestamp"))) \
        .withColumn("month", F.month(F.col("timestamp"))) \
        .withColumn("day_of_year", F.dayofyear(F.col("timestamp"))) \
        .withColumn("year", F.year(F.col("timestamp")))

    return df_time_features.dropna(subset=["station_id", "timestamp"])


def apply_forward_fill(df: DataFrame) -> DataFrame:
    """Xử lý dữ liệu khuyết bằng kỹ thuật Forward Fill theo trạm đo."""
    df = df.repartition("station_id")
    
    ffill_window = Window.partitionBy("station_id") \
                         .orderBy("timestamp") \
                         .rowsBetween(Window.unboundedPreceding, 0)

    sensor_cols = [
        "temp_c", "humidity", "pressure", "wind_speed", "wind_dir", 
        "precipitation", "cloud_cover", "shortwave_radiation", 
        "soil_temperature", "pm2_5", "us_aqi"
    ]

    for col_name in sensor_cols:
        df = df.withColumn(
            col_name, 
            F.last(F.col(col_name), ignorenulls=True).over(ffill_window)
        )

    return df.dropna(subset=sensor_cols)


# ==========================================
# CÁC HÀM XUẤT DỮ LIỆU (SINKS)
# ==========================================

def write_to_hdfs(df: DataFrame, output_path: str, load_mode: str):
    """Ghi dữ liệu xuống HDFS với chế độ động."""
    print(f"[*] Lưu Dữ liệu Vàng xuống HDFS (Mode: {load_mode}): {output_path}")
    df.coalesce(1).write \
        .mode(load_mode) \
        .partitionBy("year", "month", "day_of_year") \
        .parquet(output_path)

def write_to_mongodb(df: DataFrame, mongo_uri: str, load_mode: str):
    """Ghi dữ liệu vào MongoDB với chế độ động."""
    print(f"[*] Đang ghi dữ liệu vào MongoDB (Mode: {load_mode})...")
    
    serving_cols = [
        "station_id", "timestamp", "latitude", "longitude",
        "temp_c", "humidity", "pressure", "wind_speed", "wind_dir",
        "precipitation", "cloud_cover", "shortwave_radiation", "soil_temperature",
        "pm2_5", "us_aqi",
        "is_stagnant_air", "cooling_degree_days", "pm25_acc_12h"
    ]
    
    df.select(*serving_cols).write \
        .format("mongodb") \
        .option("spark.mongodb.write.connection.uri", mongo_uri) \
        .mode(load_mode) \
        .save()
    print("[+] Đã đồng bộ lên MongoDB thành công!")


# ==========================================
# LUỒNG ĐIỀU PHỐI CHÍNH (ORCHESTRATOR)
# ==========================================

def main():
    # 1. Tải cấu hình từ file .env
    load_dotenv()
    LOAD_MODE = os.getenv("SPARK_LOAD_MODE", "append")

    INPUT_HDFS = "hdfs://namenode:8020/user/data_engineer/raw_jsonl/*.jsonl"
    OUTPUT_HDFS = "hdfs://namenode:8020/user/data_engineer/feature_store/feature/"
    
    # Trích xuất thông tin tài khoản Spark từ biến môi trường
    MONGO_USER = os.getenv("MONGO_SPARK_USER")
    MONGO_PASS = os.getenv("MONGO_SPARK_PASS")
    # Nếu chạy script này ngoài Docker (trên máy host), hãy gán cứng host là "localhost"
    # Nếu chạy bên trong một container thuộc cùng network, dùng os.getenv("MONGO_HOST")
    MONGO_HOST = os.getenv("MONGO_HOST")
    MONGO_PORT = os.getenv("MONGO_PORT")
    MONGO_DB = os.getenv("MONGO_DB_NAME")

    # Cấu hình chuỗi kết nối động
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.climate_observations?authSource=admin"   
    # 2. Khởi tạo SparkSession
    spark = SparkSession.builder \
        .appName("Weather_Feature_Transformation_Batch") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.windowExec.buffer.in.memory.threshold", "100000") \
        .getOrCreate()

    print("[*] Đang đọc dữ liệu thô từ HDFS...")
    df_raw = spark.read.json(INPUT_HDFS)

    # 3. Thực thi Pipeline
    print("[*] Tiền xử lý và Spatial Merge...")
    df_l0 = extract_and_spatial_merge(df_raw)

    print("[*] Xử lý Dữ liệu khuyết (Forward Fill)...")
    df_filled = apply_forward_fill(df_l0)

    print("[*] Áp dụng các tầng Feature Engineering (L1 -> L4)...")
    df_l1_l2 = apply_l1_l2_transforms(df_filled)
    df_l3 = apply_l3_transforms(df_l1_l2)
    df_final = apply_l4_targets(df_l3)

    # 4. Ghi kết quả
    write_to_hdfs(df_final, OUTPUT_HDFS, LOAD_MODE)
    write_to_mongodb(df_final, MONGO_URI, LOAD_MODE)

    print("[+] Hoàn tất Batch Job!")
    spark.stop()

if __name__ == "__main__":
    main()