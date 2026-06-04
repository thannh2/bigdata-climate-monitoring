import os
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# 1. CẤU HÌNH MONGODB
# ==========================================
MONGO_USER = os.getenv("MONGO_SPARK_USER", "spark_user")
MONGO_PASS = os.getenv("MONGO_SPARK_PASS", "password")
MONGO_HOST = os.getenv("MONGO_HOST", "host.docker.internal")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB = os.getenv("MONGO_DB_NAME", "climate_db")

# Khởi tạo sẵn các URI
MONGO_BATCH_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.climate_observations?authSource=admin"
MONGO_STREAM_OBS_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.realtime_observations?authSource=admin"
MONGO_STREAM_ALERTS_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.realtime_alerts?authSource=admin"

# ==========================================
# 2. CẤU HÌNH KAFKA & HDFS
# ==========================================
KAFKA_SERVERS = "ingestion-kafka-1:29092"
KAFKA_TOPIC = "weather.raw.stream,air_quality.raw.stream"

INPUT_HDFS = "hdfs://namenode:8020/user/data_engineer/raw_jsonl/*.jsonl"
OUTPUT_HDFS = "hdfs://namenode:8020/user/data_engineer/feature_store/feature/"
SPARK_LOAD_MODE = os.getenv("SPARK_LOAD_MODE", "append")
CHECKPOINT_DIR = "/tmp/checkpoints/weather_stream"

# ==========================================
# 3. DANH SÁCH CỘT TIÊU CHUẨN
# ==========================================
SERVING_COLS = [
    "station_id", "timestamp", "latitude", "longitude",
    "temp_c", "humidity", "pressure", "wind_speed", "wind_dir",
    "precipitation", "cloud_cover", "shortwave_radiation", "soil_temperature",
    "pm2_5", "us_aqi",
    "is_stagnant_air", "cooling_degree_days", "pm25_acc_12h"
]

OBSERVATION_COLS = [
    "station_id", "timestamp", "latitude", "longitude",
    "temp_c", "humidity", "pressure", "wind_speed", "wind_dir",
    "precipitation", "cloud_cover", "shortwave_radiation", "soil_temperature",
    "pm2_5", "us_aqi",
    "is_stagnant_air", "cooling_degree_days", "pm25_acc_12h",
    "pressure_delta_3h", "wind_shear_U", "wind_shear_V", "temp_mean_6h" # Các cột L3
]

STREAMING_ALERT_COLS = [
    "station_id", 
    "timestamp", 
    "target_feature", # VD: 'pm25', 'temp_c'
    "forecast_horizon", # VD: 'h1', 'h2', 'h3'
    "predicted_value" # Giá trị mô hình dự đoán ra
]

# ==========================================
# 4. CẤU HÌNH MLFLOW
# ==========================================
# Khai báo địa chỉ của máy chủ MLflow Tracking
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")