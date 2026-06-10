import os
import sys
if "ARROW_PRE_0_15_IPC_FORMAT" in os.environ:
    del os.environ["ARROW_PRE_0_15_IPC_FORMAT"]

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-java-options -Djava.security.manager=allow pyspark-shell"

# ==============================================================================
# IMPORT
# ==============================================================================
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampNTZType
from processing.configs import config
from processing.schemas.schemas import get_l0_unified_schema
from processing.utils.utils_features import apply_l1_l2_transforms
from processing.ml.inference import load_all_forecast_models, apply_ml_predictions, load_single_model

# ==============================================================================
# GLOBAL STATE: lưu lịch sử 12 dòng gần nhất của từng station, trên Driver
# KEY: station_id (str)
# VALUE: pd.DataFrame với các cột cần thiết để tính L3
# ==============================================================================
STATION_HISTORY = {}   # dict[str, pd.DataFrame]
GLOBAL_ML_MODELS = None

# Các cột cần giữ trong history để tính L3
HISTORY_COLS = ["timestamp", "pressure", "wind_U", "wind_V", "temp_c", "pm2_5"]
HISTORY_SIZE = 12

# ==============================================================================
# KAFKA SOURCE
# ==============================================================================
def read_kafka_source(spark: SparkSession):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_SERVERS) \
        .option("subscribe", config.KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

def parse_and_rename_l0(kafka_df):
    schema = get_l0_unified_schema()
    parsed_df = kafka_df.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    return parsed_df.select(
        F.lower(F.trim(F.col("city"))).alias("station_id"),
        F.col("event_time").cast(TimestampType()).alias("timestamp"),
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
        F.col("pm25").alias("pm2_5"),
        F.col("aqi").alias("us_aqi")
    ).withColumn("hour", F.hour(F.col("timestamp"))) \
     .withColumn("day_of_year", F.dayofyear(F.col("timestamp")))

# ==============================================================================
# L3: tính stateful trên Driver bằng Python dict
# Gọi trong foreachBatch — chạy trên Driver, không serialize state qua JVM
# ==============================================================================
def apply_l3_with_driver_state(batch_pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Nhận một pandas DataFrame của một micro-batch,
    tính các cột L3 bằng cách kết hợp với lịch sử trên Driver.
    Trả về pandas DataFrame đã có đầy đủ cột L3.
    """
    global STATION_HISTORY

    result_parts = []

    for station_id, group in batch_pdf.groupby("station_id"):
        group = group.sort_values("timestamp").reset_index(drop=True)

        # Lấy lịch sử cũ
        history = STATION_HISTORY.get(station_id, pd.DataFrame(columns=HISTORY_COLS))

        # Gộp lịch sử + dữ liệu mới
        if not history.empty:
            # Chỉ lấy các cột cần thiết từ history để concat
            hist_subset = history[HISTORY_COLS].copy()
            for col in group.columns:
                if col not in hist_subset.columns:
                    hist_subset[col] = None
            combined = pd.concat([hist_subset, group], ignore_index=True)
        else:
            combined = group.copy()

        combined = combined.sort_values("timestamp").reset_index(drop=True)

        # Fill missing sensor values
        for col in ["pressure", "wind_U", "wind_V", "temp_c", "pm2_5"]:
            if col in combined.columns:
                combined[col] = combined[col].ffill().bfill()

        # Tính L3
        combined["pressure_delta_3h"] = combined["pressure"].diff(3).fillna(0.0).astype("float64")
        combined["wind_shear_U"]      = combined["wind_U"].diff(1).fillna(0.0).astype("float64")
        combined["wind_shear_V"]      = combined["wind_V"].diff(1).fillna(0.0).astype("float64")
        combined["temp_mean_6h"]      = combined["temp_c"].rolling(window=6, min_periods=1).mean().fillna(0.0).astype("float64")
        combined["pm25_acc_12h"]      = combined["pm2_5"].rolling(window=12, min_periods=1).sum().fillna(0.0).astype("float64")

        # Số dòng mới (= số dòng của group ban đầu)
        n_new = len(group)
        result_part = combined.tail(n_new).reset_index(drop=True)

        # Cập nhật lịch sử: giữ 12 dòng mới nhất
        all_rows = combined[HISTORY_COLS].copy()
        STATION_HISTORY[station_id] = all_rows.tail(HISTORY_SIZE).reset_index(drop=True)

        result_parts.append(result_part)

    if result_parts:
        return pd.concat(result_parts, ignore_index=True)
    else:
        return pd.DataFrame()

# ==============================================================================
# FOREACHBATCH: toàn bộ pipeline xử lý một micro-batch
# ==============================================================================
def process_batch(batch_df, batch_id):
    global GLOBAL_ML_MODELS

    if batch_df.isEmpty():
        return

    # --- Bước 1: Kéo batch xuống Driver dưới dạng pandas ---
    batch_pdf = batch_df.toPandas()

    # --- Bước 2: Tính L3 trên Driver (stateful, không serialize) ---
    batch_pdf = apply_l3_with_driver_state(batch_pdf)

    if batch_pdf.empty:
        return

    # --- Bước 3: Đẩy lại thành Spark DataFrame để ghi MongoDB ---
    spark = SparkSession.getActiveSession()

    # Đảm bảo kiểu dữ liệu đúng trước khi tạo lại Spark DF
    for col in ["pressure_delta_3h", "wind_shear_U", "wind_shear_V", "temp_mean_6h", "pm25_acc_12h"]:
        if col in batch_pdf.columns:
            batch_pdf[col] = batch_pdf[col].astype(float)

    # Chuyển timestamp về dạng string để tránh lỗi timezone khi createDataFrame
    if "timestamp" in batch_pdf.columns:
        batch_pdf["timestamp"] = pd.to_datetime(batch_pdf["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    l3_df = spark.createDataFrame(batch_pdf)
    l3_df = l3_df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))

    for col_name in ["us_aqi", "pm2_5", "humidity", "wind_speed", "wind_dir",
                    "precipitation", "cloud_cover", "shortwave_radiation", "soil_temperature"]:
        if col_name in l3_df.columns:
            l3_df = l3_df.withColumn(col_name, F.col(col_name).cast("double"))
        l3_df.cache()

    # --- Bước 4: Ghi observations vào MongoDB ---
    print(f"[Batch {batch_id}] Ghi dữ liệu quan trắc vào MongoDB...")
    obs_cols = [c for c in config.OBSERVATION_COLS if c in l3_df.columns]
    #overwrite, append
    l3_df.select(*obs_cols).write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.write.connection.uri", config.MONGO_STREAM_OBS_URI) \
        .save()

    if GLOBAL_ML_MODELS is None or len(GLOBAL_ML_MODELS) < 36:
        print(f"[Batch {batch_id}] Đang tải toàn bộ 36 mô hình...")
        GLOBAL_ML_MODELS = load_all_forecast_models(config.MLFLOW_TRACKING_URI, GLOBAL_ML_MODELS)

    # --- Bước 6: Dự đoán ---
    print(f"[Batch {batch_id}] Chạy dự đoán...")
    alert_df = apply_ml_predictions(l3_df, GLOBAL_ML_MODELS)

    # --- Bước 7: Ghi kết quả dự đoán vào MongoDB ---
    if alert_df is not None and not alert_df.isEmpty():
        print(f"[Batch {batch_id}] Ghi {alert_df.count()} bản ghi vào MongoDB...")
        alert_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("spark.mongodb.write.connection.uri", config.MONGO_STREAM_ALERTS_URI) \
            .save()
        
    # --- Bước 7: Ghi kết quả dự đoán vào MongoDB ---
    if alert_df is not None and not alert_df.isEmpty():
        print(f"[Batch {batch_id}] Đang ghi {alert_df.count()} bản ghi vào MongoDB...")
        #overwrite, append
        alert_df.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", config.MONGO_STREAM_ALERTS_URI) \
            .save()

    l3_df.unpersist()

# ==============================================================================
# MAIN
# ==============================================================================
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

    l0_stream = parse_and_rename_l0(raw_stream)
    l1_l2_stream = apply_l1_l2_transforms(l0_stream)

    # L3 được tính trong foreachBatch trên Driver
    query = l1_l2_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(once=True) \
        .option("checkpointLocation", config.CHECKPOINT_DIR) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()