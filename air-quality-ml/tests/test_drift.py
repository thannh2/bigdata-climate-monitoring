from __future__ import annotations

import pandas as pd

from air_quality_ml.monitoring.drift import calculate_psi, compute_feature_drift


def test_calculate_psi_non_negative():
    expected = [1, 2, 3, 4, 5]
    actual = [1, 2, 3, 4, 10]
    psi = calculate_psi(expected=expected, actual=actual)
    assert psi >= 0.0


def test_compute_feature_drift_has_status():
    train_df = pd.DataFrame({"pm2_5": [10, 20, 30, 40], "humidity": [50, 55, 60, 65]})
    serving_df = pd.DataFrame({"pm2_5": [20, 30, 40, 60], "humidity": [50, 56, 62, 68]})

    out = compute_feature_drift(train_df, serving_df, features=["pm2_5", "humidity"])
    assert set(out.columns) >= {"feature_name", "psi", "status"}
    assert len(out) == 2
