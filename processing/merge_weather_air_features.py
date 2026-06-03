from __future__ import annotations

import argparse
import math
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


DEFAULT_INPUT = "exports/*.jsonl"
DEFAULT_OUTPUT = "Data/extracted features/features"
DEFAULT_START_DATE = "2021-04-16"
DEFAULT_END_DATE = "2026-04-15"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge exported weather and air-quality JSONL records into an ML feature table."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="Input JSONL path or glob. Supports local paths and hdfs:// paths.",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output parquet dataset path. Supports local paths and hdfs:// paths.",
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Inclusive start date for features, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        default=DEFAULT_END_DATE,
        help="Inclusive end date for features, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master override, for example local[*] or spark://spark-master:7077.",
    )
    return parser.parse_args()


def create_spark_session(master: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName("Merge_Weather_Air_Features")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
    )
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def _resolve_local_path(path: str) -> str:
    if "://" in path:
        return path
    return str((Path.cwd() / path).resolve())


def _col_or_null(df: DataFrame, column_name: str):
    if column_name in df.columns:
        return F.col(column_name)
    return F.lit(None)


def load_exported_records(spark: SparkSession, input_path: str) -> DataFrame:
    raw = spark.read.json(input_path)
    if "value" not in raw.columns:
        raise ValueError("Input JSONL must contain a top-level 'value' object")

    selected = raw.select(
        _col_or_null(raw, "kafka_topic").alias("kafka_topic"),
        "value.*",
    )
    if "event_time" not in selected.columns or "city" not in selected.columns:
        raise ValueError("Input records must contain value.event_time and value.city")

    return selected.withColumn("timestamp", F.to_timestamp("event_time")).withColumn(
        "city_key", F.lower(F.trim(F.col("city")))
    )


def build_base_feature_frame(records: DataFrame, start_date: str, end_date: str) -> DataFrame:
    weather = (
        records.filter(
            (F.col("kafka_topic") == F.lit("weather.raw.batch"))
            | F.col("temperature_c").isNotNull()
            | F.col("pressure_hpa").isNotNull()
        )
        .select(
            "city_key",
            "timestamp",
            F.col("station_id").alias("weather_station_id"),
            "region",
            "city",
            "latitude",
            "longitude",
            F.col("temperature_c").alias("temp_c"),
            "humidity",
            F.col("pressure_hpa").alias("pressure"),
            F.col("wind_speed_mps").alias("wind_speed"),
            _col_or_null(records, "wind_direction_deg").alias("wind_dir"),
            _col_or_null(records, "precipitation_mm").alias("precipitation"),
            _col_or_null(records, "cloud_cover_pct").alias("cloud_cover"),
            _col_or_null(records, "shortwave_radiation_wm2").alias("shortwave_radiation"),
            _col_or_null(records, "soil_temperature_0_to_7cm_c").alias("soil_temperature"),
        )
        .dropDuplicates(["city_key", "timestamp"])
    )

    air = (
        records.filter(
            (F.col("kafka_topic") == F.lit("air_quality.raw.batch"))
            | F.col("pm25").isNotNull()
            | F.col("aqi").isNotNull()
        )
        .select(
            "city_key",
            "timestamp",
            F.col("station_id").alias("air_station_id"),
            F.col("pm25").alias("pm2_5"),
            F.col("aqi").alias("us_aqi"),
            "co",
            "no2",
            "so2",
            "o3",
        )
        .dropDuplicates(["city_key", "timestamp"])
    )

    joined = weather.join(air, on=["city_key", "timestamp"], how="inner")
    joined = joined.filter(
        (F.to_date("timestamp") >= F.lit(start_date))
        & (F.to_date("timestamp") <= F.lit(end_date))
    )

    return (
        joined.withColumn("station_id", F.coalesce(F.col("weather_station_id"), F.col("air_station_id")))
        .drop("city_key", "weather_station_id", "air_station_id")
        .withColumn("elevation", F.lit(0.0))
        .withColumn("wind_dir", F.coalesce(F.col("wind_dir"), F.lit(0.0)))
        .withColumn("precipitation", F.coalesce(F.col("precipitation"), F.lit(0.0)))
        .withColumn("cloud_cover", F.coalesce(F.col("cloud_cover"), F.lit(0.0)))
        .withColumn("shortwave_radiation", F.coalesce(F.col("shortwave_radiation"), F.lit(0.0)))
        .withColumn("soil_temperature", F.coalesce(F.col("soil_temperature"), F.col("temp_c")))
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("year", F.year("timestamp"))
    )


def add_engineered_features(df: DataFrame) -> DataFrame:
    lat_rad = F.col("latitude") * math.pi / 180
    lon_rad = F.col("longitude") * math.pi / 180
    wind_dir_rad = F.col("wind_dir") * math.pi / 180

    df = (
        df.withColumn("coord_X", F.cos(lat_rad) * F.cos(lon_rad))
        .withColumn("coord_Y", F.cos(lat_rad) * F.sin(lon_rad))
        .withColumn("coord_Z", F.sin(lat_rad))
        .withColumn("wind_U", -F.col("wind_speed") * F.sin(wind_dir_rad))
        .withColumn("wind_V", -F.col("wind_speed") * F.cos(wind_dir_rad))
        .withColumn("air_density", (F.col("pressure") * 100) / (287.05 * (F.col("temp_c") + 273.15)))
    )

    b, c = 17.625, 243.04
    humidity_safe = F.when(F.col("humidity") <= 0, F.lit(0.000001)).otherwise(F.col("humidity"))
    gamma = F.log(humidity_safe / 100.0) + (b * F.col("temp_c")) / (c + F.col("temp_c"))

    df = (
        df.withColumn("dew_point", (c * gamma) / (b - gamma))
        .withColumn("hour_sin", F.sin(2 * math.pi * F.col("hour") / 24))
        .withColumn("hour_cos", F.cos(2 * math.pi * F.col("hour") / 24))
    )

    e_vapor = 6.112 * F.exp((17.67 * F.col("dew_point")) / (F.col("dew_point") + 243.5))
    mixing_ratio = 0.622 * e_vapor / (F.col("pressure") - e_vapor)
    theta = (F.col("temp_c") + 273.15) * F.pow(1000.0 / F.col("pressure"), 0.286)

    return (
        df.withColumn("theta_e", theta * F.exp((2675.0 * mixing_ratio) / (F.col("dew_point") + 273.15)))
        .withColumn("is_stagnant_air", F.when((F.col("wind_speed") < 1.0) & (F.col("pressure") > 1010), 1).otherwise(0))
        .withColumn("cooling_degree_days", F.when(F.col("temp_c") > 24, F.col("temp_c") - 24).otherwise(0))
    )


def add_time_series_features(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("station_id").orderBy("timestamp")
    window_6h = Window.partitionBy("station_id").orderBy("timestamp").rowsBetween(-5, 0)
    window_12h = Window.partitionBy("station_id").orderBy("timestamp").rowsBetween(-11, 0)

    return (
        df.withColumn("pressure_delta_3h", F.col("pressure") - F.lag("pressure", 3).over(window_spec))
        .withColumn("wind_shear_U", F.col("wind_U") - F.lag("wind_U", 1).over(window_spec))
        .withColumn("wind_shear_V", F.col("wind_V") - F.lag("wind_V", 1).over(window_spec))
        .withColumn("temp_mean_6h", F.avg("temp_c").over(window_6h))
        .withColumn("pm25_acc_12h", F.sum("pm2_5").over(window_12h))
    )


def add_targets(df: DataFrame, target_hours: list[int]) -> DataFrame:
    window_spec = Window.partitionBy("station_id").orderBy("timestamp")

    for hours in target_hours:
        df = df.withColumn(f"target_temp_{hours}h", F.lead("temp_c", hours).over(window_spec))
        df = df.withColumn(f"target_pm25_{hours}h", F.lead("pm2_5", hours).over(window_spec))
        df = df.withColumn(f"target_cloud_cover_{hours}h", F.lead("cloud_cover", hours).over(window_spec))
        df = df.withColumn(f"target_precipitation_{hours}h", F.lead("precipitation", hours).over(window_spec))
        df = df.withColumn(f"target_wind_speed_{hours}h", F.lead("wind_speed", hours).over(window_spec))
        df = df.withColumn(f"target_pressure_{hours}h", F.lead("pressure", hours).over(window_spec))

    return df


def main() -> None:
    args = parse_args()
    input_path = _resolve_local_path(args.input)
    output_path = _resolve_local_path(args.output)

    spark = create_spark_session(args.master)
    try:
        print(f"Reading exported JSONL from: {input_path}")
        records = load_exported_records(spark, input_path)

        print("Merging weather and air-quality records")
        features = build_base_feature_frame(records, args.start_date, args.end_date)

        print("Adding engineered features and targets")
        features = add_engineered_features(features)
        features = add_time_series_features(features)
        features = add_targets(features, target_hours=[1, 2, 3, 4, 5, 6])

        print(f"Writing feature table to: {output_path}")
        features.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)

        print(f"Done. Rows: {features.count()}, columns: {len(features.columns)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
