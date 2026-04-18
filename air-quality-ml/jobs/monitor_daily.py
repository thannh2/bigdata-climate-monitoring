from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

from air_quality_ml.monitoring.data_quality import duplicate_rate, ingestion_delay_stats, missing_rate
from air_quality_ml.settings import load_base_settings, resolve_path
from air_quality_ml.utils.logger import get_logger, log_event
from air_quality_ml.utils.spark import create_spark_session


def main() -> None:
    logger = get_logger("air_quality_ml.monitor_daily")
    root = Path(__file__).resolve().parents[1]
    base_cfg_path = root / "configs" / "base.yaml"
    settings = load_base_settings(base_cfg_path)

    features_path = str(resolve_path(base_cfg_path.parent, settings.data.gold_features_path))
    monitoring_path = str(resolve_path(base_cfg_path.parent, settings.data.monitoring_path))

    spark = create_spark_session(settings)
    try:
        df = spark.read.parquet(features_path)

        missing_df = missing_rate(df, ["pm25", "temperature_c", "humidity", "pressure_hpa", "wind_speed_mps", "event_hour", "station_id"])
        duplicate_df = duplicate_rate(df, ["station_id", "event_hour"])
        delay_df = ingestion_delay_stats(df, event_col="event_hour", ingestion_col="ingestion_time")

        snapshot_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        missing_row = missing_df.withColumn("metric_type", F.lit("missing_rate")).withColumn("metric_time", F.lit(snapshot_ts))
        duplicate_row = duplicate_df.withColumn("metric_type", F.lit("duplicate_rate")).withColumn("metric_time", F.lit(snapshot_ts))
        delay_row = delay_df.withColumn("metric_type", F.lit("ingestion_delay")).withColumn("metric_time", F.lit(snapshot_ts))

        output_df = missing_row.unionByName(duplicate_row, allowMissingColumns=True).unionByName(delay_row, allowMissingColumns=True)
        output_df.write.mode("append").parquet(monitoring_path)

        log_event(logger, "monitor_daily_written", path=monitoring_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
