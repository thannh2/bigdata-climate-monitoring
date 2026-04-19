from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F

from air_quality_ml.data_contract.validators import validate_feature_contract
from air_quality_ml.settings import load_base_settings


def _base_settings():
    base_config_path = Path(__file__).resolve().parents[1] / "configs" / "base.yaml"
    settings = load_base_settings(base_config_path)
    settings.data_contract.run_pandera = False
    return settings, base_config_path


def test_validate_feature_contract_passes_for_minimal_frame(spark):
    settings, base_config_path = _base_settings()

    row = {
        "station_id": "Hanoi",
        "timestamp": "2026-01-01 00:00:00",
        "latitude": 21.0,
        "longitude": 105.8,
        "temp_c": 30.0,
        "humidity": 70.0,
        "pressure": 1005.0,
        "wind_speed": 4.0,
        "wind_dir": 180.0,
        "precipitation": 0.0,
        "cloud_cover": 0.0,
        "shortwave_radiation": 200.0,
        "soil_temperature": 28.0,
        "pm2_5": 60.0,
        "us_aqi": 120.0,
        "region": "bac",
        "city": "Hanoi",
        "coord_X": 0.1,
        "coord_Y": 0.2,
        "coord_Z": 0.3,
        "wind_U": 1.0,
        "wind_V": 2.0,
        "air_density": 1.1,
        "dew_point": 22.0,
        "hour_sin": 0.0,
        "hour_cos": 1.0,
        "theta_e": 0.5,
        "is_stagnant_air": 0.0,
        "cooling_degree_days": 0.2,
        "pressure_delta_3h": 0.3,
        "wind_shear_U": 29.0,
        "wind_shear_V": 31.0,
        "temp_mean_6h": 32.0,
        "pm25_acc_12h": 33.0,
        "target_pm25_1h": 61.0,
        "target_pm25_6h": 62.0,
        "target_pm25_12h": 63.0,
        "target_pm25_24h": 64.0,
        "target_alert_1h": 1,
        "target_alert_6h": 1,
        "target_alert_12h": 0,
        "target_alert_24h": 0,
    }
    df = spark.createDataFrame([row]).withColumn("timestamp", F.to_timestamp("timestamp"))

    report = validate_feature_contract(df, settings, base_config_path.parent / "feature_store.yaml")
    assert report["status"] == "passed"


def test_validate_feature_contract_fails_on_missing_required_column(spark):
    settings, base_config_path = _base_settings()

    df = spark.createDataFrame([{"station_id": "Hanoi"}])
    report = validate_feature_contract(df, settings, base_config_path.parent / "feature_store.yaml")

    assert report["status"] == "failed"
    assert any(issue["check"] == "missing_columns" for issue in report["issues"])
