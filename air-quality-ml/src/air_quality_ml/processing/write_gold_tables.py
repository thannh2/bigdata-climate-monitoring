from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame

from air_quality_ml.utils.parquet_io import write_dataset_safe


def write_parquet_table(
    df: DataFrame, 
    path: str, 
    dataset_format: str = "parquet",
    mode: str = "overwrite", 
    partition_cols: Optional[list[str]] = None
) -> None:
    """
    Write DataFrame to parquet with Windows compatibility
    
    Args:
        df: DataFrame to write
        path: Output path
        mode: Write mode (overwrite, append, etc.)
        partition_cols: Columns to partition by
    """
    write_dataset_safe(df, path, dataset_format=dataset_format, mode=mode, partition_cols=partition_cols)
