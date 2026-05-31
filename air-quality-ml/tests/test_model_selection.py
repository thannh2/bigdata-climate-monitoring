from __future__ import annotations

from pyspark.sql import functions as F

from air_quality_ml.features.l4_targets import (
    HORIZONS,
    L4_TARGET_SPECS,
    infer_target_col_from_model_name,
    iter_l4_target_columns,
)
from air_quality_ml.training.model_selection import (
    Candidate,
    default_regression_candidates,
    select_best_regression,
)


def test_horizons_are_expected():
    assert HORIZONS == (1, 2, 3, 4, 5, 6, 9, 12, 15, 18, 21, 24)


def test_only_green_targets_remain():
    names = {spec.target_name for spec in L4_TARGET_SPECS}
    assert names == {"temp", "pm25", "cloud_cover", "precipitation", "wind_speed", "pressure"}
    # tat ca deu regression
    assert all(spec.task == "regression" for spec in L4_TARGET_SPECS)
    # cac target do khong con
    for removed in ("inversion", "solar_rad", "hvac_load", "rain_start", "storm_prob", "wind_U", "wind_V"):
        assert removed not in names


def test_iter_target_columns_count():
    # 6 target x 12 horizon
    assert len(iter_l4_target_columns()) == 6 * 12


def test_infer_target_col_from_model_name():
    assert infer_target_col_from_model_name("aq_pm25_h6", 6) == "target_pm25_6h"
    assert infer_target_col_from_model_name("aq_cloud_cover_h12", 12) == "target_cloud_cover_12h"
    assert infer_target_col_from_model_name("aq_alert_classifier_h24", 24) == "target_alert_24h"


def _toy_regression_df(spark):
    rows = []
    for i in range(400):
        ts = f"2026-01-{(i % 27) + 1:02d} {i % 24:02d}:00:00"
        x = float(i % 50)
        rows.append(
            {
                "station_id": "Hanoi",
                "timestamp": ts,
                "region": "bac",
                "city": "Hanoi",
                "temp_c": x,
                "humidity": 50.0 + (i % 10),
                "target_pm25_1h": 2.0 * x + 3.0,  # quan he tuyen tinh -> de hoc
            }
        )
    return spark.createDataFrame(rows).withColumn("timestamp", F.to_timestamp("timestamp"))


def test_select_best_regression_picks_a_model(spark):
    df = _toy_regression_df(spark)
    train = df.filter(F.col("timestamp") <= F.to_timestamp(F.lit("2026-01-18 23:00:00")))
    val = df.filter(
        (F.col("timestamp") > F.to_timestamp(F.lit("2026-01-18 23:00:00")))
        & (F.col("timestamp") <= F.to_timestamp(F.lit("2026-01-23 23:00:00")))
    )
    test = df.filter(F.col("timestamp") > F.to_timestamp(F.lit("2026-01-23 23:00:00")))

    candidates = [Candidate("linear", {}), Candidate("gbt", {"maxIter": 20, "maxDepth": 4})]
    result = select_best_regression(
        train_df=train,
        val_df=val,
        test_df=test,
        numeric_features=["temp_c", "humidity"],
        categorical_features=["region", "city"],
        label_col="target_pm25_1h",
        prediction_col="prediction",
        candidates=candidates,
        selection_metric="mae",
    )

    assert result.best is not None
    assert len(result.candidates) == 2
    # baseline linear duoc ghi nhan
    assert result.baseline_metrics is not None
    # MAE cua model tot nhat la huu han va khong am
    assert result.best.val_metrics["mae"] >= 0.0


def test_default_candidates_have_baseline():
    reg = default_regression_candidates()
    assert any(c.model_type in {"linear", "lr"} for c in reg)
