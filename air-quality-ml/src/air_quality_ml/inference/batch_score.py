from __future__ import annotations

import argparse
from uuid import uuid4
from pathlib import Path

import mlflow
import mlflow.spark
from pyspark.sql import functions as F

from air_quality_ml.inference.postprocess_alerts import add_alert_level, add_binary_alert
from air_quality_ml.inference.writer_mongodb import write_predictions_to_mongo
from air_quality_ml.settings import load_base_settings, resolve_path
from air_quality_ml.training.thresholding import with_probability_score
from air_quality_ml.utils.logger import get_logger, log_event
from air_quality_ml.utils.parquet_io import read_dataset_safe, write_dataset_safe
from air_quality_ml.utils.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch scoring with model from MLflow registry")
    parser.add_argument("--base-config", required=True, help="Path to base.yaml")
    parser.add_argument("--model-uri", required=True, help="MLflow model URI, ex: models:/aq_pm25_forecast_h1/Production")
    parser.add_argument("--input-path", required=False, default=None, help="Input feature table path")
    parser.add_argument("--output-path", required=False, default=None, help="Output prediction path")
    parser.add_argument("--horizon", required=True, type=int, help="Prediction horizon in hours")
    parser.add_argument("--alert-threshold", required=False, type=float, default=0.5)
    parser.add_argument("--mongo-uri", required=False, default=None)
    parser.add_argument("--mongo-db", required=False, default="air_quality")
    parser.add_argument("--mongo-collection", required=False, default="realtime_predictions")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = get_logger("air_quality_ml.batch_score")

    base_config_path = Path(args.base_config).resolve()
    settings = load_base_settings(base_config_path)

    input_path_cfg = args.input_path or settings.data.curated_dataset_path
    output_path_cfg = args.output_path or settings.data.gold_predictions_path
    input_path = str(resolve_path(base_config_path.parent, input_path_cfg))
    output_path = str(resolve_path(base_config_path.parent, output_path_cfg))

    spark = create_spark_session(settings)
    try:
        mlflow.set_tracking_uri(settings.mlflow.tracking_uri)

        features_df = read_dataset_safe(spark, input_path, dataset_format=settings.storage.curated_format)
        model = mlflow.spark.load_model(args.model_uri)

        batch_id = str(uuid4())
        pred_df = model.transform(features_df)
        pred_df = pred_df.withColumn("horizon", F.lit(int(args.horizon)))
        pred_df = pred_df.withColumn("prediction_time", F.current_timestamp())
        pred_df = pred_df.withColumn("prediction_date", F.to_date("prediction_time"))
        pred_df = pred_df.withColumn("model_name", F.lit(args.model_uri.split("/")[1] if "/" in args.model_uri else args.model_uri))
        pred_df = pred_df.withColumn("model_version", F.lit(args.model_uri))
        pred_df = pred_df.withColumn("batch_id", F.lit(batch_id))

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
        log_event(
            logger,
            "batch_score_written",
            output_path=output_path,
            output_format=settings.storage.predictions_format,
            batch_id=batch_id,
        )

        if args.mongo_uri:
            write_predictions_to_mongo(
                pred_df.select(
                    *[
                        c
                        for c in [
                            "station_id",
                            "region",
                            "city",
                            "timestamp",
                            "prediction_time",
                            "horizon",
                            "model_name",
                            "model_version",
                            "batch_id",
                            "prediction",
                            "pred_prob",
                            "pred_alert",
                            "alert_level",
                        ]
                        if c in pred_df.columns
                    ]
                ),
                mongo_uri=args.mongo_uri,
                database=args.mongo_db,
                collection=args.mongo_collection,
            )
            log_event(logger, "batch_score_mongo_written", db=args.mongo_db, collection=args.mongo_collection, batch_id=batch_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
