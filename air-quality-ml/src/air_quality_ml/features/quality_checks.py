from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clip_numeric(df: DataFrame, col_name: str, lower: float, upper: float) -> DataFrame:
    return df.withColumn(
        col_name,
        F.when(F.col(col_name) < F.lit(lower), F.lit(lower))
        .when(F.col(col_name) > F.lit(upper), F.lit(upper))
        .otherwise(F.col(col_name)),
    )
