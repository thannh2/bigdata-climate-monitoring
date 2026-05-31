from __future__ import annotations

<<<<<<< Updated upstream
from pyspark.sql import DataFrame, SparkSession


def stream_score_job(spark: SparkSession, source_df: DataFrame) -> DataFrame:
    """Skeleton for structured-streaming scoring.

    Ban co the mo rong ham nay de:
    - doc tu Kafka/Delta source streaming,
    - load model tu MLflow Registry,
    - score tren micro-batch,
    - ghi ket qua vao lake va MongoDB.
    """
    return source_df
=======
import argparse
import math
import os
from pathlib import Path

import mlflow
import mlflow.spark
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from air_quality_ml.features.l4_targets import infer_target_col_from_model_name, target_metadata
from air_quality_ml.inference.postprocess_alerts import add_alert_level, add_binary_alert
from air_quality_ml.inference.writer_mongodb import write_predictions_to_mongo
from air_quality_ml.settings import load_base_settings, resolve_path
from air_quality_ml.training.thresholding import with_probability_score
from air_quality_ml.utils.logger import get_logger, log_event
from air_quality_ml.utils.parquet_io import write_dataset_safe
from air_quality_ml.utils.spark import create_spark_session


STREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("source", StringType()),
        StructField("region", StringType()),
        StructField("city", StringType()),
        StructField("station_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("elevation", DoubleType()),
        StructField("event_time", StringType()),
        StructField("ingestion_time", StringType()),
        StructField("temperature_c", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("pressure_hpa", DoubleType()),
        StructField("wind_speed_mps", DoubleType()),
        StructField("wind_dir", DoubleType()),
        StructField("precipitation", DoubleType()),
        StructField("cloud_cover", DoubleType()),
        StructField("shortwave_radiation", DoubleType()),
        StructField("soil_temperature", DoubleType()),
        StructField("aqi", DoubleType()),
        StructField("pm25", DoubleType()),
        StructField("pm10", DoubleType()),
        StructField("co", DoubleType()),
        StructField("no2", DoubleType()),
        StructField("so2", DoubleType()),
        StructField("o3", DoubleType()),
        StructField("data_version", StringType()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Structured-streaming scoring from Kafka with an MLflow Spark model")
    parser.add_argument("--base-config", required=True, help="Path to base.yaml")
    parser.add_argument("--model-uri", required=True, help="MLflow model URI, ex: models:/aq_pm25_forecast_h1/latest")
    parser.add_argument("--horizon", required=True, type=int, help="Prediction horizon in hours")
    parser.add_argument("--target-col", default=None, help="L4 target column this model predicts")
    parser.add_argument("--target-name", default=None, help="Human-readable L4 target name")
    parser.add_argument("--prediction-unit", default=None, help="Prediction unit, e.g. ug_m3 or celsius")
    parser.add_argument("--output-kind", default=None, help="regression/classification/binary/probability")
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--topics", default="weather.raw.stream,air_quality.raw.stream")
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--checkpoint-path", default=None)
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    parser.add_argument("--processing-time", default="30 seconds")
    parser.add_argument("--once", action="store_true", help="Process currently available Kafka records and exit")
    parser.add_argument("--alert-threshold", type=float, default=0.5)
    parser.add_argument("--mongo-uri", default=None)
    parser.add_argument("--mongo-db", default="air_quality")
    parser.add_argument("--mongo-collection", default="realtime_predictions")
    return parser.parse_args()


def _model_name_from_uri(model_uri: str) -> str:
    if model_uri.startswith("models:/"):
        return model_uri.removeprefix("models:/").split("/")[0]
    if "/" in model_uri:
        return model_uri.split("/")[1]
    return model_uri


def _infer_target_col(model_uri: str, horizon: int) -> str:
    model_name = _model_name_from_uri(model_uri)
    return infer_target_col_from_model_name(model_name, horizon)


def _prediction_metadata(args: argparse.Namespace) -> dict[str, str]:
    target_col = args.target_col or _infer_target_col(args.model_uri, int(args.horizon))
    metadata = target_metadata(target_col, fallback_task=args.output_kind)
    if args.target_name:
        metadata["target_name"] = args.target_name
    if args.prediction_unit:
        metadata["prediction_unit"] = args.prediction_unit
    if args.output_kind:
        metadata["target_family"] = args.output_kind
    return metadata


def _latest_per_city(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("city_key").orderBy(F.col("timestamp").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def build_feature_frame(events: DataFrame) -> DataFrame:
    parsed = (
        events.select(
            F.col("topic").cast("string").alias("kafka_topic"),
            F.from_json(F.col("value").cast("string"), STREAM_SCHEMA).alias("value"),
        )
        .select("kafka_topic", "value.*")
        .withColumn("timestamp", F.to_timestamp("event_time"))
        .withColumn("city_key", F.lower(F.trim(F.col("city"))))
    )

    weather = (
        parsed.filter(F.col("kafka_topic") == F.lit("weather.raw.stream"))
        .select(
            "city_key",
            F.col("timestamp").alias("weather_timestamp"),
            F.col("station_id").alias("weather_station_id"),
            "region",
            "city",
            "latitude",
            "longitude",
            "elevation",
            F.col("temperature_c").alias("temp_c"),
            "humidity",
            F.col("pressure_hpa").alias("pressure"),
            F.col("wind_speed_mps").alias("wind_speed"),
            "wind_dir",
            "precipitation",
            "cloud_cover",
            "shortwave_radiation",
            "soil_temperature",
        )
        .dropna(subset=["city_key"])
    )

    air = (
        parsed.filter(F.col("kafka_topic") == F.lit("air_quality.raw.stream"))
        .select(
            "city_key",
            F.col("timestamp").alias("air_timestamp"),
            F.col("station_id").alias("air_station_id"),
            F.col("pm25").alias("pm2_5"),
            F.col("aqi").alias("us_aqi"),
            "co",
            "no2",
            "so2",
            "o3",
        )
        .dropna(subset=["city_key"])
    )

    weather_latest = _latest_per_city(weather.withColumn("timestamp", F.col("weather_timestamp"))).drop("timestamp")
    air_latest = _latest_per_city(air.withColumn("timestamp", F.col("air_timestamp"))).drop("timestamp")
    joined = weather_latest.join(air_latest, on="city_key", how="inner")
    features = (
        joined.withColumn("timestamp", F.greatest(F.col("weather_timestamp"), F.col("air_timestamp")))
        .withColumn("station_id", F.coalesce(F.col("weather_station_id"), F.col("air_station_id")))
        .drop("city_key", "weather_station_id", "air_station_id", "weather_timestamp", "air_timestamp")
        .withColumn("elevation", F.coalesce(F.col("elevation"), F.lit(0.0)))
        .withColumn("wind_dir", F.coalesce(F.col("wind_dir"), F.lit(0.0)))
        .withColumn("precipitation", F.coalesce(F.col("precipitation"), F.lit(0.0)))
        .withColumn("cloud_cover", F.coalesce(F.col("cloud_cover"), F.lit(0.0)))
        .withColumn("shortwave_radiation", F.coalesce(F.col("shortwave_radiation"), F.lit(0.0)))
        .withColumn("soil_temperature", F.coalesce(F.col("soil_temperature"), F.col("temp_c")))
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("day_of_year", F.dayofyear("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("year", F.year("timestamp"))
    )

    return add_realtime_features(features)


def add_realtime_features(df: DataFrame) -> DataFrame:
    lat_rad = F.col("latitude") * math.pi / 180
    lon_rad = F.col("longitude") * math.pi / 180
    wind_dir_rad = F.col("wind_dir") * math.pi / 180
    humidity_safe = F.when(F.col("humidity") <= 0, F.lit(0.000001)).otherwise(F.col("humidity"))
    b, c = 17.625, 243.04
    gamma = F.log(humidity_safe / 100.0) + (b * F.col("temp_c")) / (c + F.col("temp_c"))

    df = (
        df.withColumn("coord_X", F.cos(lat_rad) * F.cos(lon_rad))
        .withColumn("coord_Y", F.cos(lat_rad) * F.sin(lon_rad))
        .withColumn("coord_Z", F.sin(lat_rad))
        .withColumn("wind_U", -F.col("wind_speed") * F.sin(wind_dir_rad))
        .withColumn("wind_V", -F.col("wind_speed") * F.cos(wind_dir_rad))
        .withColumn("air_density", (F.col("pressure") * 100) / (287.05 * (F.col("temp_c") + 273.15)))
        .withColumn("dew_point", (c * gamma) / (b - gamma))
        .withColumn("hour_sin", F.sin(2 * math.pi * F.col("hour") / 24))
        .withColumn("hour_cos", F.cos(2 * math.pi * F.col("hour") / 24))
        .withColumn("day_sin", F.sin(2 * math.pi * F.col("day_of_year") / 366))
    )

    e_vapor = 6.112 * F.exp((17.67 * F.col("dew_point")) / (F.col("dew_point") + 243.5))
    mixing_ratio = 0.622 * e_vapor / (F.col("pressure") - e_vapor)
    theta = (F.col("temp_c") + 273.15) * F.pow(1000.0 / F.col("pressure"), 0.286)

    return (
        df.withColumn("theta_e", theta * F.exp((2675.0 * mixing_ratio) / (F.col("dew_point") + 273.15)))
        .withColumn("is_stagnant_air", F.when((F.col("wind_speed") < 1.0) & (F.col("pressure") > 1010), 1).otherwise(0))
        .withColumn("cooling_degree_days", F.when(F.col("temp_c") > 24, F.col("temp_c") - 24).otherwise(0))
        .withColumn("pressure_delta_3h", F.lit(0.0))
        .withColumn("wind_shear_U", F.lit(0.0))
        .withColumn("wind_shear_V", F.lit(0.0))
        .withColumn("temp_mean_6h", F.col("temp_c"))
        .withColumn("pm25_acc_12h", F.col("pm2_5"))
    )


def main() -> None:
    args = parse_args()
    logger = get_logger("air_quality_ml.stream_score")

    base_config_path = Path(args.base_config).resolve()
    settings = load_base_settings(base_config_path)
    output_path_cfg = args.output_path or str(Path(settings.data.gold_predictions_path).parent / "predictions_stream")
    output_path = str(resolve_path(base_config_path.parent, output_path_cfg))
    checkpoint_path = args.checkpoint_path or f"{output_path}_checkpoint"

    spark = create_spark_session(settings)
    try:
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI") or settings.mlflow.tracking_uri)
        source_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topics)
            .option("startingOffsets", args.starting_offsets)
            .load()
            .select("topic", "key", "value", "timestamp")
        )

        model = mlflow.spark.load_model(args.model_uri)
        metadata = _prediction_metadata(args)

        def score_microbatch(batch_df: DataFrame, batch_id: int) -> None:
            if batch_df.rdd.isEmpty():
                return

            features_df = build_feature_frame(batch_df)
            if features_df.rdd.isEmpty():
                log_event(logger, "stream_score_skipped_batch", batch_id=batch_id, reason="no_joined_weather_air_rows")
                return

            pred_df = model.transform(features_df)
            pred_df = (
                pred_df.withColumn("horizon", F.lit(int(args.horizon)))
                .withColumn("target_name", F.lit(metadata["target_name"]))
                .withColumn("target_col", F.lit(metadata["target_col"]))
                .withColumn("target_family", F.lit(metadata["target_family"]))
                .withColumn("prediction_unit", F.lit(metadata["prediction_unit"]))
                .withColumn("prediction_time", F.current_timestamp())
                .withColumn("prediction_date", F.to_date("prediction_time"))
                .withColumn("model_name", F.lit(_model_name_from_uri(args.model_uri)))
                .withColumn("model_version", F.lit(args.model_uri))
                .withColumn("batch_id", F.lit(str(batch_id)))
            )

            if "probability" in pred_df.columns:
                pred_df = with_probability_score(pred_df, probability_col="probability", score_col="pred_prob")
                pred_df = add_binary_alert(pred_df, score_col="pred_prob", threshold=float(args.alert_threshold), output_col="pred_alert")
                pred_df = add_alert_level(pred_df, score_col="pred_prob", output_col="alert_level")

            write_dataset_safe(
                pred_df,
                output_path,
                dataset_format=settings.storage.predictions_format,
                mode="append",
                partition_cols=["prediction_date", "horizon"],
            )

            if args.mongo_uri:
                mongo_cols = [
                    "station_id",
                    "region",
                    "city",
                    "timestamp",
                    "prediction_time",
                    "horizon",
                    "target_name",
                    "target_col",
                    "target_family",
                    "prediction_unit",
                    "model_name",
                    "model_version",
                    "batch_id",
                    "prediction",
                    "pred_prob",
                    "pred_alert",
                    "alert_level",
                ]
                write_predictions_to_mongo(
                    pred_df.select(*[c for c in mongo_cols if c in pred_df.columns]),
                    mongo_uri=args.mongo_uri,
                    database=args.mongo_db,
                    collection=args.mongo_collection,
                )

            log_event(logger, "stream_score_written_batch", batch_id=batch_id, output_path=output_path, rows=pred_df.count())

        writer = (
            source_df.writeStream.foreachBatch(score_microbatch)
            .option("checkpointLocation", checkpoint_path)
            .queryName(f"stream_score_h{args.horizon}")
        )
        writer = writer.trigger(availableNow=True) if args.once else writer.trigger(processingTime=args.processing_time)
        query = writer.start()
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
>>>>>>> Stashed changes
