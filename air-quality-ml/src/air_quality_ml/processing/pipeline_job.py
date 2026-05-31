from __future__ import annotations

import argparse
import math
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from air_quality_ml.data_contract.validators import validate_feature_contract
from air_quality_ml.features.l4_targets import L4_TARGET_SPECS, iter_l4_target_columns
from air_quality_ml.processing.load_features import (
    add_alert_target_from_pm25,
    load_and_prepare_features,
)
from air_quality_ml.settings import load_base_settings, resolve_path
from air_quality_ml.utils.io import write_json
from air_quality_ml.utils.logger import get_logger, log_event
from air_quality_ml.utils.parquet_io import get_dataset_version, write_dataset_safe
from air_quality_ml.utils.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build curated ML dataset from transformed features")
    parser.add_argument("--base-config", required=True, help="Path to base config")
    return parser.parse_args()


def _partition_columns(columns: list[str]) -> list[str]:
    return [column for column in ["year", "month"] if column in columns]


def _ensure_time_features(df: DataFrame) -> DataFrame:
    if "timestamp" not in df.columns:
        return df
    if "day_of_year" not in df.columns:
        df = df.withColumn("day_of_year", F.dayofyear("timestamp"))
    if "day_sin" not in df.columns:
        df = df.withColumn("day_sin", F.sin(2 * math.pi * F.col("day_of_year") / 366))
    return df


def _add_l4_targets(df: DataFrame) -> DataFrame:
    """Regenerate moi L4 target bang lead(source_col, horizon) de dam bao nhat quan.

    Ghi de ca cac target da duoc tien tinh trong nguon (1h/6h/12h/24h) thay vi
    giu nguyen, tranh provenance hon hop giua gia tri nguon va gia tri lead().
    """
    if "station_id" not in df.columns or "timestamp" not in df.columns:
        return df

    window_spec = Window.partitionBy("station_id").orderBy("timestamp")
    for spec in L4_TARGET_SPECS:
        if spec.source_col not in df.columns:
            continue
        for horizon in spec.horizons:
            target_col = spec.column_for_horizon(horizon)
            df = df.withColumn(target_col, F.lead(spec.source_col, horizon).over(window_spec))

    return df


def _drop_unused_targets(df: DataFrame, allowed_targets: set[str]) -> DataFrame:
    """Loai bo moi cot target_* khong nam trong tap cho phep.

    Cac target 'do' (rain_start, storm_prob, inversion, solar_rad, hvac_load,
    wind_U, wind_V) da co san trong parquet nguon se bi drop o day.
    """
    to_drop = [
        column
        for column in df.columns
        if column.startswith("target_") and column not in allowed_targets
    ]
    if to_drop:
        df = df.drop(*to_drop)
    return df


def main() -> None:
    args = parse_args()
    logger = get_logger("air_quality_ml.processing.pipeline_job")

    base_config_path = Path(args.base_config).resolve()
    settings = load_base_settings(base_config_path)

    features_path = str(resolve_path(base_config_path.parent, settings.data.features_path))
    curated_path = str(resolve_path(base_config_path.parent, settings.data.curated_dataset_path))
    contract_reports_path = resolve_path(base_config_path.parent, settings.data.contract_reports_path)
    feature_store_path = base_config_path.parent / "feature_store.yaml"

    spark = create_spark_session(settings)
    try:
        df = load_and_prepare_features(spark, features_path)
        df = _ensure_time_features(df)
        df = _add_l4_targets(df)

        for h in settings.features.horizons:
            pm25_col = f"target_pm25_{h}h"
            alert_col = f"target_alert_{h}h"
            if pm25_col in df.columns:
                df = add_alert_target_from_pm25(
                    df,
                    pm25_col,
                    alert_col,
                    threshold=settings.features.alert_pm25_threshold,
                )

        allowed_targets = set(iter_l4_target_columns()) | {
            f"target_alert_{h}h" for h in settings.features.horizons
        }
        df = _drop_unused_targets(df, allowed_targets)

        contract_report = {"status": "skipped", "reason": "disabled"}
        if settings.data_contract.enabled:
            contract_report = validate_feature_contract(df, settings, feature_store_path)
            report_name = f"feature_contract_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
            write_json(contract_reports_path / report_name, contract_report)

            if contract_report["status"] != "passed" and settings.data_contract.fail_on_error:
                raise ValueError(f"Data contract validation failed: {contract_report['issues']}")

        partition_cols = _partition_columns(df.columns)
        write_dataset_safe(
            df,
            curated_path,
            dataset_format=settings.storage.curated_format,
            mode="overwrite",
            partition_cols=partition_cols or None,
        )
        dataset_version = get_dataset_version(spark, curated_path, settings.storage.curated_format)

        log_event(
            logger,
            "curated_dataset_materialized",
            path=features_path,
            curated_path=curated_path,
            curated_format=settings.storage.curated_format,
            total_rows=df.count(),
            total_cols=len(df.columns),
            columns=df.columns,
            contract_status=contract_report.get("status"),
            dataset_version=dataset_version,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
