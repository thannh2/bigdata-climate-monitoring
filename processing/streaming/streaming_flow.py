import os
import sys
import math

if "ARROW_PRE_0_15_IPC_FORMAT" in os.environ:
    del os.environ["ARROW_PRE_0_15_IPC_FORMAT"]

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-java-options -Djava.security.manager=allow pyspark-shell"

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from processing.configs import config
from processing.schemas.schemas import get_weather_schema, get_air_schema
from processing.utils.utils_features import apply_l1_l2_transforms
from processing.ml.inference import load_all_forecast_models, apply_ml_predictions

STATION_HISTORY = {}
GLOBAL_ML_MODELS = None
IS_FIRST_WRITE = True
HISTORY_COLS = [
    "timestamp", "latitude", "longitude", "region", "city",
    "pressure", "wind_U", "wind_V", "temp_c", "pm2_5",
    "humidity", "wind_speed", "wind_dir", "precipitation",
    "cloud_cover", "shortwave_radiation", "soil_temperature"
]
HISTORY_SIZE = 12

def read_kafka_source(spark: SparkSession):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_SERVERS) \
        .option("subscribe", config.KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

def parse_split_and_join(raw_stream):
    json_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str")

    weather_stream = json_stream \
        .filter(F.col("json_str").contains("weather")) \
        .select(F.from_json("json_str", get_weather_schema()).alias("data")).select("data.*") \
        .withColumn("w_station_id", F.lower(F.trim(F.col("city")))) \
        .withColumn("w_time", F.col("event_time").cast(TimestampType())) \
        .withWatermark("w_time", "15 minutes")

    air_stream = json_stream \
        .filter(F.col("json_str").contains("air")) \
        .select(F.from_json("json_str", get_air_schema()).alias("data")).select("data.*") \
        .withColumn("a_station_id", F.lower(F.trim(F.col("city")))) \
        .withColumn("a_time", F.col("event_time").cast(TimestampType())) \
        .withWatermark("a_time", "15 minutes")

    joined_stream = weather_stream.join(
        air_stream,
        F.expr("""
            w_station_id = a_station_id AND
            w_time >= a_time - interval 15 minutes AND
            w_time <= a_time + interval 15 minutes
        """),
        how="outer"
    )

    unified_stream = joined_stream.select(
        F.coalesce(weather_stream["latitude"], air_stream["latitude"]).alias("latitude"),
        F.coalesce(weather_stream["longitude"], air_stream["longitude"]).alias("longitude"),
        F.coalesce(weather_stream["region"], air_stream["region"], F.lit("unknown")).alias("region"),
        F.coalesce(weather_stream["city"], air_stream["city"], F.lit("unknown")).alias("city"),
        
        F.col("temperature_c").alias("temp_c"),
        F.col("humidity").alias("humidity"),
        F.col("pressure_hpa").alias("pressure"),
        F.col("wind_speed_mps").alias("wind_speed"),
        F.col("wind_direction_deg").alias("wind_dir"),
        F.col("precipitation_mm").alias("precipitation"),
        F.col("cloud_cover_pct").alias("cloud_cover"),
        F.col("shortwave_radiation_wm2").alias("shortwave_radiation"),
        F.col("soil_temperature_0_to_7cm_c").alias("soil_temperature"),
        F.col("pm25").alias("pm2_5"),
        F.col("aqi").alias("us_aqi"),
        F.coalesce(weather_stream["w_station_id"], air_stream["a_station_id"]).alias("station_id"),
        F.coalesce(weather_stream["w_time"], air_stream["a_time"]).alias("timestamp")
    )

    unified_stream = unified_stream.withColumn("hour", F.hour(F.col("timestamp"))) \
        .withColumn("day_of_year", F.dayofyear(F.col("timestamp"))) \
        .withColumn("hour_sin", F.sin(2 * math.pi * F.col("hour") / 24)) \
        .withColumn("hour_cos", F.cos(2 * math.pi * F.col("hour") / 24)) \
        .withColumn("day_sin", F.sin(2 * math.pi * F.col("day_of_year") / 365)) \
        .withColumn("day_cos", F.cos(2 * math.pi * F.col("day_of_year") / 365))


    unified_stream = unified_stream.dropna(subset=["station_id", "timestamp", "latitude", "longitude"])
    return unified_stream

def apply_l3_with_driver_state(batch_pdf: pd.DataFrame) -> pd.DataFrame:
    global STATION_HISTORY
    result_parts = []

    for station_id, group in batch_pdf.groupby("station_id"):
        group = group.sort_values("timestamp").reset_index(drop=True)
        history = STATION_HISTORY.get(station_id, pd.DataFrame(columns=HISTORY_COLS))

        if not history.empty:
            hist_subset = history[HISTORY_COLS].copy()
            for col in group.columns:
                if col not in hist_subset.columns:
                    hist_subset[col] = None
            combined = pd.concat([hist_subset, group], ignore_index=True)
        else:
            combined = group.copy()

        combined = combined.sort_values("timestamp").reset_index(drop=True)

        for col in ["pressure", "wind_U", "wind_V", "temp_c", "pm2_5"]:
            if col in combined.columns:
                combined[col] = combined[col].ffill().bfill()

        combined["pressure_delta_3h"] = combined["pressure"].diff(3).fillna(0.0).astype("float64")
        combined["wind_shear_U"]      = combined["wind_U"].diff(1).fillna(0.0).astype("float64")
        combined["wind_shear_V"]      = combined["wind_V"].diff(1).fillna(0.0).astype("float64")
        combined["temp_mean_6h"]      = combined["temp_c"].rolling(window=6, min_periods=1).mean().fillna(0.0).astype("float64")
        combined["pm25_acc_12h"]      = combined["pm2_5"].rolling(window=12, min_periods=1).sum().fillna(0.0).astype("float64")

        n_new = len(group)
        result_part = combined.tail(n_new).reset_index(drop=True)

        all_rows = combined[HISTORY_COLS].copy()
        STATION_HISTORY[station_id] = all_rows.tail(HISTORY_SIZE).reset_index(drop=True)

        result_parts.append(result_part)

    if result_parts:
        return pd.concat(result_parts, ignore_index=True)
    else:
        return pd.DataFrame()

def process_batch(batch_df, batch_id):
    global GLOBAL_ML_MODELS, IS_FIRST_WRITE
    if batch_df.isEmpty():
        return

    batch_pdf = batch_df.toPandas()
    batch_pdf = apply_l3_with_driver_state(batch_pdf)

    if batch_pdf.empty:
        return

    spark = SparkSession.getActiveSession()

    for col in ["pressure_delta_3h", "wind_shear_U", "wind_shear_V", "temp_mean_6h", "pm25_acc_12h"]:
        if col in batch_pdf.columns:
            batch_pdf[col] = batch_pdf[col].astype(float)

    if "timestamp" in batch_pdf.columns:
        batch_pdf["timestamp"] = pd.to_datetime(batch_pdf["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    l3_df = spark.createDataFrame(batch_pdf)
    l3_df = l3_df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))

    # Định dạng các cột số học
    all_numeric_cols = ["us_aqi", "pm2_5", "humidity", "wind_speed", "wind_dir",
                        "precipitation", "cloud_cover", "shortwave_radiation", "soil_temperature",
                        "pressure_delta_3h", "wind_shear_U", "wind_shear_V", "temp_mean_6h", "pm25_acc_12h",
                        "hour_sin", "hour_cos", "day_sin", "day_cos"]
    
    for col_name in all_numeric_cols:
        if col_name in l3_df.columns:
            l3_df = l3_df.withColumn(col_name, F.col(col_name).cast("double"))
    
    l3_df.cache()

    print(f"[Batch {batch_id}] Ghi dữ liệu quan trắc vào MongoDB...")
    obs_cols = [c for c in config.OBSERVATION_COLS if c in l3_df.columns]
    
    # GHI ĐÈ (OVERWRITE) cho OBSERVATIONS
    current_mode = "append"
    if config.SPARK_LOAD_MODE == "overwrite":
        if IS_FIRST_WRITE:
            current_mode = "overwrite"  
            IS_FIRST_WRITE = False      
        else:
            current_mode = "append"     
    else:
        current_mode = "append"

    print(f"[Batch {batch_id}] Ghi dữ liệu quan trắc vào MongoDB (Mode: {current_mode})...")
    obs_cols = [c for c in config.OBSERVATION_COLS if c in l3_df.columns]
    
    # GHI OBSERVATIONS
    l3_df.select(*obs_cols).write \
        .format("mongodb") \
        .mode(current_mode) \
        .option("spark.mongodb.write.connection.uri", config.MONGO_STREAM_OBS_URI) \
        .save()

    if GLOBAL_ML_MODELS is None or len(GLOBAL_ML_MODELS) < 36:
        print(f"[Batch {batch_id}] Đang tải mô hình...")
        GLOBAL_ML_MODELS = load_all_forecast_models(config.MLFLOW_TRACKING_URI, GLOBAL_ML_MODELS)

    print(f"[Batch {batch_id}] Chạy dự đoán...")
    alert_df = apply_ml_predictions(l3_df, GLOBAL_ML_MODELS)

    if alert_df is not None and not alert_df.isEmpty():
        print(f"[Batch {batch_id}] Đang ghi {alert_df.count()} bản ghi dự đoán vào MongoDB (Mode: {current_mode})...")
        
        # GHI ALERTS
        alert_df.write \
            .format("mongodb") \
            .mode(current_mode) \
            .option("spark.mongodb.write.connection.uri", config.MONGO_STREAM_ALERTS_URI) \
            .save()

    l3_df.unpersist()

def main():
    os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "0"

    spark = SparkSession.builder \
        .appName("Weather_Streaming_Predictive_Pipeline") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("[*] Khởi động Pipeline Streaming...")
    raw_stream = read_kafka_source(spark)

    l0_stream = parse_split_and_join(raw_stream)
    l1_l2_stream = apply_l1_l2_transforms(l0_stream)

    query = l1_l2_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", config.CHECKPOINT_DIR) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()