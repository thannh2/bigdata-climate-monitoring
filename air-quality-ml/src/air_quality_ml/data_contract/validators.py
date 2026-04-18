from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_required_columns(df: DataFrame, required_cols: Iterable[str]) -> None:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def compute_missing_rate(df: DataFrame, cols: Iterable[str]) -> DataFrame:
    metrics = [
        (F.avg(F.when(F.col(c).isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias(c))
        for c in cols
        if c in df.columns
    ]
    return df.agg(*metrics)


def compute_duplicate_rate(df: DataFrame, key_cols: list[str]) -> DataFrame:
    if not key_cols:
        raise ValueError("key_cols must not be empty")

    total = df.count()
    if total == 0:
        return df.sparkSession.createDataFrame([(0, 0, 0.0)], ["total_rows", "duplicate_rows", "duplicate_rate"])

    dup_rows = (
        df.groupBy(*key_cols)
        .count()
        .filter(F.col("count") > 1)
        .agg(F.sum(F.col("count") - F.lit(1)).alias("duplicate_rows"))
        .collect()[0]["duplicate_rows"]
        or 0
    )
    duplicate_rate = float(dup_rows) / float(total)
    return df.sparkSession.createDataFrame(
        [(int(total), int(dup_rows), float(duplicate_rate))],
        ["total_rows", "duplicate_rows", "duplicate_rate"],
    )


def compute_range_violations(df: DataFrame, range_rules: dict[str, dict[str, float]]) -> DataFrame:
    rows = []
    total = df.count()
    for col_name, rule in range_rules.items():
        if col_name not in df.columns:
            continue

        min_v = rule.get("min")
        max_v = rule.get("max")

        expr = F.lit(False)
        if min_v is not None:
            expr = expr | (F.col(col_name) < F.lit(min_v))
        if max_v is not None:
            expr = expr | (F.col(col_name) > F.lit(max_v))

        invalid_count = df.filter(expr).count()
        invalid_rate = (float(invalid_count) / float(total)) if total else 0.0
        rows.append((col_name, int(invalid_count), float(invalid_rate)))

    return df.sparkSession.createDataFrame(rows, ["column_name", "invalid_count", "invalid_rate"])
