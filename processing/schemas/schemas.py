from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def get_kafka_weather_schema():
    """Schema Lớp 0 (Bronze) cho dữ liệu thời tiết thô từ Kafka"""
    return StructType([
        StructField("station_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure_hpa", DoubleType(), True),
        StructField("wind_speed_mps", DoubleType(), True),
        StructField("wind_direction_deg", DoubleType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("cloud_cover_pct", DoubleType(), True),
        StructField("shortwave_radiation_wm2", DoubleType(), True),
        StructField("pm25", DoubleType(), True)
    ])