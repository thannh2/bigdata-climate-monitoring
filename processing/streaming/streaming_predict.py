import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.ml import PipelineModel

from processing.utils.utils_streaming_l3 import apply_streaming_l3_stateful
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def get_l0_weather_schema():
    """Schema L0 mapping 1:1 với NormalizedWeatherRecord từ Kafka"""
    return StructType([
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
        StructField("data_version", StringType(), True)
    ])
    ])

def main():
    # 1. Khởi tạo SparkSession
    spark = SparkSession.builder \
        .appName("Weather_Streaming_Predict") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

    # Giảm log rác của Spark trên console
    spark.sparkContext.setLogLevel("WARN")

    # 2. Cấu hình Kafka (Được trích xuất từ Ingestion config)
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "weather.raw.stream"
    
    # Load Mô hình (Thay thế bằng đường dẫn HDFS thực tế của bạn)
    # model_path = "hdfs://namenode:8020/models/weather_classification_model"
    # model = PipelineModel.load(model_path)

    # 3. Đọc dữ liệu từ Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Parse JSON Payload
    l0_schema = get_l0_weather_schema()
    parsed_stream = kafka_stream.select(
        F.from_json(F.col("value").cast("string"), l0_schema).alias("data")
    ).select("data.*")

    # 5. Áp dụng Feature Engineering (Watermark là bắt buộc với Streaming)
    watermarked_stream = parsed_stream.withWatermark("timestamp", "2 hours")
    
    # Gọi hàm xử lý L1-L3 của bạn
    # features_stream = apply_streaming_l3_stateful(watermarked_stream)
    features_stream = watermarked_stream # Tạm gán để chạy thử

    # 6. Chạy Model Inference
    # predictions = model.transform(features_stream)
    predictions = features_stream # Tạm gán để chạy thử

    # 7. Ghi kết quả ra Console để Debug
    print("Đang khởi động luồng đọc Kafka...")
    query = predictions.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()