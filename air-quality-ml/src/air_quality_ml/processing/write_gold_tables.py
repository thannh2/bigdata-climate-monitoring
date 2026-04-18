from __future__ import annotations

from pyspark.sql import DataFrame


def write_parquet_table(df: DataFrame, path: str, mode: str = "overwrite", partition_cols: list[str] | None = None) -> None:
    writer = df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
