from __future__ import annotations

import argparse
from pathlib import Path

from air_quality_ml.processing.load_features import (
    add_alert_target_from_pm25,
    load_and_prepare_features,
)
from air_quality_ml.settings import load_base_settings, resolve_path
from air_quality_ml.utils.logger import get_logger, log_event
from air_quality_ml.utils.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load and validate transformed features")
    parser.add_argument("--base-config", required=True, help="Path to base config")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = get_logger("air_quality_ml.processing.pipeline_job")

    base_config_path = Path(args.base_config).resolve()
    settings = load_base_settings(base_config_path)

    features_path = str(resolve_path(base_config_path.parent, settings.data.features_path))

    spark = create_spark_session(settings)
    try:
        # Load features đã transform
        df = load_and_prepare_features(spark, features_path)
        
        # Thêm alert targets nếu chưa có
        for h in settings.features.horizons:
            pm25_col = f"target_pm25_{h}h"
            alert_col = f"target_alert_{h}h"
            if pm25_col in df.columns:
                df = add_alert_target_from_pm25(
                    df, pm25_col, alert_col, 
                    threshold=settings.features.alert_pm25_threshold
                )
        
        # Show info
        log_event(
            logger, "features_loaded",
            path=features_path,
            total_rows=df.count(),
            total_cols=len(df.columns),
            columns=df.columns
        )
        
        print("\n" + "="*80)
        print("SCHEMA:")
        print("="*80)
        df.printSchema()
        
        print("\n" + "="*80)
        print("SAMPLE DATA:")
        print("="*80)
        df.show(5, truncate=False)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
