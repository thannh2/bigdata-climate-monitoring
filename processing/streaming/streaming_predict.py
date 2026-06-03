import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from processing.schemas.schemas import get_l0_unified_schema
from processing.utils.utils_features import apply_l1_l2_transforms
from processing.utils.utils_streaming_l3 import apply_streaming_l3_stateful

def read_kafka_source(spark: SparkSession, servers: str, topic: str):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

def parse_and_rename_l0(kafka_df):
    """Giải mã JSON bằng Unified Schema và áp dụng Spatial Merge"""
    schema = get_l0_unified_schema()
    
    parsed_df = kafka_df.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    return parsed_df.select(
        # 2. HỢP NHẤT KHÔNG GIAN: Dùng city làm khóa gom nhóm thay vì station_id thô
        F.lower(F.trim(F.col("city"))).alias("station_id"), 
        F.col("event_time").alias("timestamp"),
        F.col("latitude"), F.col("longitude"),
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
        
        # Ánh xạ cột AQI
        F.col("pm25").alias("pm2_5"),
        F.col("aqi").alias("us_aqi")
    ).withColumn("hour", F.hour(F.col("timestamp"))) \
     .withColumn("day_of_year", F.dayofyear(F.col("timestamp")))

def write_console_sink(df):
    return df.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

def write_mongo_batch(batch_df, batch_id):
    """Ghi từng Micro-batch của luồng Streaming vào MongoDB."""
    MONGO_URI = "mongodb://spark_ingestion:writepassword456@localhost:27017/climate_db.realtime_alerts?authSource=admin"
    
    # Chọn các cột cảnh báo quan trọng
    alert_cols = [
        "station_id", "timestamp", "temp_c", "wind_speed", "precipitation", 
        "pm2_5", "us_aqi"
        # Thêm các cột target_storm_prob hoặc Alert_Code đã tính ở L4
    ]
    
    batch_df.select(*alert_cols).write \
        .format("mongodb") \
        .option("spark.mongodb.write.connection.uri", MONGO_URI) \
        .mode("append") \
        .save()

def main():
    spark = SparkSession.builder \
        .appName("Weather_Streaming_Inference") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/weather_stream") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    KAFKA_SERVERS = "localhost:9092"
    
    # 3. ĐỌC ĐA CHỦ ĐỀ: Truyền danh sách topic cách nhau bằng dấu phẩy
    KAFKA_TOPIC = "weather.raw.stream,air_quality.raw.stream"
    
    print("[*] Đang khởi động Pipeline...")
    
    raw_stream = read_kafka_source(spark, KAFKA_SERVERS, KAFKA_TOPIC)
    
    l0_stream = parse_and_rename_l0(raw_stream)
    
    l1_l2_stream = apply_l1_l2_transforms(l0_stream)
    
    watermarked_stream = l1_l2_stream.withWatermark("timestamp", "2 hours")
    
    # Hàm này sẽ tự động chạy .ffill() để lấp đầy các ô trống giữa 2 luồng dữ liệu
    l3_stream = apply_streaming_l3_stateful(watermarked_stream)
    
    predictions_stream = l3_stream # Tạm gán để test
    

    # 4. Ghi ra Sink (Console để debug + MongoDB để lưu trữ)
    query = write_console_sink(predictions_stream)
    print("[*] Bắt đầu đẩy dữ liệu vào MongoDB (realtime_alerts)...")
    
    # Ghi vào MongoDB bằng foreachBatch
    query = predictions_stream.writeStream \
        .foreachBatch(write_mongo_batch) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()