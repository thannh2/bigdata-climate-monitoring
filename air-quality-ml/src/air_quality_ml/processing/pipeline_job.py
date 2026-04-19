from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from air_quality_ml.data_contract.validators import validate_feature_contract
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
