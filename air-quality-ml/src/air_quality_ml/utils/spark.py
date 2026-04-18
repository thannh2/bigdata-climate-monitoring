from __future__ import annotations

from pyspark.sql import SparkSession

from air_quality_ml.settings import BaseSettings


def create_spark_session(settings: BaseSettings) -> SparkSession:
    spark = (
        SparkSession.builder.appName(settings.spark.app_name)
        .master(settings.spark.master)
        .config("spark.sql.session.timeZone", settings.spark.session_timezone)
        .config("spark.sql.shuffle.partitions", str(settings.spark.shuffle_partitions))
        .getOrCreate()
    )
    return spark
