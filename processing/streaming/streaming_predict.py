import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schemas.schemas import get_kafka_weather_schema
from processing.utils.utils_features import apply_l1_l2_transforms
from processing.utils.utils_streaming_l3 import apply_streaming_l3_stateful
from processing.utils.utils_rule_based import apply_wmo_rules 

def main():
    spark = SparkSession.builder \
        .appName("Weather_Realtime_Prediction") \
        .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/weather_db.realtime_view") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_schema = get_kafka_weather_schema()

    # Đọc luồng
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather_realtime_topic") \
        .option("startingOffsets", "latest").load()

    # Ép kiểu (L0)
    df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), kafka_schema).alias("data")).select("data.*") \
        .withColumnRenamed("temperature_c", "temp_c") \
        .withColumnRenamed("pressure_hpa", "pressure") \
        .withColumnRenamed("wind_speed_mps", "wind_speed") \
        .withColumnRenamed("wind_direction_deg", "wind_dir") \
        .withColumnRenamed("precipitation_mm", "precipitation") \
        .withColumnRenamed("cloud_cover_pct", "cloud_cover") \
        .withColumnRenamed("shortwave_radiation_wm2", "shortwave_radiation") \
        .withColumnRenamed("pm25", "pm2_5") \
        .withColumn("timestamp", F.to_timestamp(F.col("event_time"))) \
        .withColumn("hour", F.hour(F.col("timestamp"))) \
        .withColumn("day_of_year", F.dayofyear(F.col("timestamp")))

    # Biến đổi L1, L2
    df = apply_l1_l2_transforms(df)

    # Biến đổi L3 (Native Stateful)
    df_l3 = apply_streaming_l3_stateful(df)

    # Inference Code (Giả lập)
    predictions_df = df_l3 \
        .withColumn("pred_cloud_cover", F.col("cloud_cover") + 5) \
        .withColumn("pred_precipitation", F.col("precipitation")) \
        .withColumn("pred_temp", F.col("temp_c") + 0.5) \
        .withColumn("pred_wind_speed", F.col("wind_speed") + 1.0) \
        .withColumn("pred_solar_rad", F.col("shortwave_radiation"))

    # Hậu xử lý Mã ABCD
    final_df = apply_wmo_rules(predictions_df)

    # Ghi trực tiếp ra MongoDB
    query = final_df.writeStream \
        .format("mongo") \
        .option("checkpointLocation", "hdfs://namenode:8020/user/data_engineer/checkpoints/streaming_predict/") \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()