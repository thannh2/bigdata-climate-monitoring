from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_binary_alert(df: DataFrame, score_col: str = "pred_prob", threshold: float = 0.5, output_col: str = "pred_alert") -> DataFrame:
    return df.withColumn(output_col, F.when(F.col(score_col) >= F.lit(float(threshold)), F.lit(1)).otherwise(F.lit(0)))


def add_alert_level(
    df: DataFrame,
    score_col: str = "pred_prob",
    output_col: str = "alert_level",
    t1: float = 0.25,
    t2: float = 0.50,
    t3: float = 0.75,
) -> DataFrame:
    return df.withColumn(
        output_col,
        F.when(F.col(score_col) < F.lit(t1), F.lit(0))
        .when(F.col(score_col) < F.lit(t2), F.lit(1))
        .when(F.col(score_col) < F.lit(t3), F.lit(2))
        .otherwise(F.lit(3)),
    )
