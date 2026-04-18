from __future__ import annotations

import argparse
from pathlib import Path

from air_quality_ml.processing.build_features import build_hourly_features
from air_quality_ml.processing.build_targets import add_alert_targets, add_pm25_targets
from air_quality_ml.processing.join_sources import join_weather_air
from air_quality_ml.processing.normalize_air_quality import normalize_air_quality
from air_quality_ml.processing.normalize_weather import normalize_weather
from air_quality_ml.processing.write_gold_tables import write_parquet_table
from air_quality_ml.settings import load_base_settings, resolve_path
from air_quality_ml.utils.logger import get_logger, log_event
from air_quality_ml.utils.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold feature/target tables from silver inputs")
    parser.add_argument("--base-config", required=True, help="Path to base config")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = get_logger("air_quality_ml.processing.pipeline_job")

    base_config_path = Path(args.base_config).resolve()
    settings = load_base_settings(base_config_path)

    weather_path = str(resolve_path(base_config_path.parent, settings.data.silver_weather_path))
    air_path = str(resolve_path(base_config_path.parent, settings.data.silver_air_path))
    features_path = str(resolve_path(base_config_path.parent, settings.data.gold_features_path))
    targets_path = str(resolve_path(base_config_path.parent, settings.data.gold_targets_path))

    spark = create_spark_session(settings)
    try:
        weather_raw = spark.read.parquet(weather_path)
        air_raw = spark.read.parquet(air_path)

        weather = normalize_weather(weather_raw)
        air = normalize_air_quality(air_raw)

        joined = join_weather_air(weather, air)
        features = build_hourly_features(joined)
        targets = add_pm25_targets(features, horizons=settings.features.horizons)
        targets = add_alert_targets(targets, horizons=settings.features.horizons, threshold=settings.features.alert_pm25_threshold)

        write_parquet_table(features, path=features_path, mode="overwrite", partition_cols=["region"])
        write_parquet_table(targets, path=targets_path, mode="overwrite", partition_cols=["region"])

        log_event(logger, "gold_tables_written", features_path=features_path, targets_path=targets_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
