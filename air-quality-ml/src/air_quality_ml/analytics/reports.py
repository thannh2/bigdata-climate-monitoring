from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def run_sql_file(spark: SparkSession, sql_file: str | Path) -> DataFrame:
    query = Path(sql_file).read_text(encoding="utf-8")
    return spark.sql(query)
