"""
Dataset I/O utilities with Windows compatibility.

Backward-compatible names `read_parquet_safe` and `write_parquet_safe`
still exist, but internal callers should prefer `read_dataset_safe`
and `write_dataset_safe` so parquet and delta can use the same code path.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def _normalize_format(dataset_format: str | None) -> str:
    return str(dataset_format or "parquet").strip().lower()


def _read_parquet_with_pandas_workaround(spark: SparkSession, path: str) -> DataFrame:
    import pandas as pd

    path_obj = Path(path)
    if path_obj.is_file():
        df_pandas = pd.read_parquet(path_obj)
    else:
        parquet_files = list(path_obj.rglob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {path_obj}")
        print(f"[INFO] Found {len(parquet_files)} parquet files")
        dfs = [pd.read_parquet(file_path) for file_path in parquet_files]
        df_pandas = pd.concat(dfs, ignore_index=True)

    return spark.createDataFrame(df_pandas)


def read_dataset_safe(
    spark: SparkSession,
    path: str,
    dataset_format: str = "parquet",
    use_pandas_workaround: Optional[bool] = None,
) -> DataFrame:
    dataset_format = _normalize_format(dataset_format)
    if dataset_format == "delta":
        return spark.read.format("delta").load(path)

    if use_pandas_workaround is None:
        use_pandas_workaround = os.name == "nt"

    if use_pandas_workaround:
        print(f"[INFO] Reading parquet with pandas workaround: {path}")
        return _read_parquet_with_pandas_workaround(spark, path)

    return spark.read.parquet(path)


def read_parquet_safe(
    spark: SparkSession,
    path: str,
    use_pandas_workaround: Optional[bool] = None,
) -> DataFrame:
    return read_dataset_safe(
        spark=spark,
        path=path,
        dataset_format="parquet",
        use_pandas_workaround=use_pandas_workaround,
    )


def write_dataset_safe(
    df: DataFrame,
    path: str,
    dataset_format: str = "parquet",
    mode: str = "overwrite",
    partition_cols: Optional[list[str]] = None,
    use_pandas_workaround: Optional[bool] = None,
) -> None:
    dataset_format = _normalize_format(dataset_format)
    if dataset_format == "delta":
        writer = df.write.format("delta").mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
        return

    if use_pandas_workaround is None:
        use_pandas_workaround = os.name == "nt"

    if use_pandas_workaround and partition_cols:
        print("[WARN] Partitioned write on Windows may be slow, using Spark native writer")
        use_pandas_workaround = False

    if use_pandas_workaround:
        print(f"[INFO] Writing parquet with pandas workaround: {path}")
        import pandas as pd

        df_pandas = df.toPandas()
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        if mode == "overwrite" and path_obj.exists():
            import shutil

            if path_obj.is_dir():
                shutil.rmtree(path_obj)
            else:
                path_obj.unlink()

        df_pandas.to_parquet(path, index=False)
        return

    writer = df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)


def write_parquet_safe(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_cols: Optional[list[str]] = None,
    use_pandas_workaround: Optional[bool] = None,
) -> None:
    write_dataset_safe(
        df=df,
        path=path,
        dataset_format="parquet",
        mode=mode,
        partition_cols=partition_cols,
        use_pandas_workaround=use_pandas_workaround,
    )


def get_dataset_version(spark: SparkSession, path: str, dataset_format: str) -> str | None:
    dataset_format = _normalize_format(dataset_format)
    if dataset_format != "delta":
        return None

    try:
        from delta.tables import DeltaTable

        if not DeltaTable.isDeltaTable(spark, path):
            return None

        history = DeltaTable.forPath(spark, path).history(1).collect()
        if not history:
            return None
        return str(history[0]["version"])
    except Exception:
        return None
