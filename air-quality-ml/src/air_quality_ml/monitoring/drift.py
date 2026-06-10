from __future__ import annotations

import numpy as np
import pandas as pd


def calculate_psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    expected = np.asarray(expected, dtype=float)
    actual = np.asarray(actual, dtype=float)

    if expected.size == 0 or actual.size == 0:
        return 0.0

    breakpoints = np.quantile(expected, np.linspace(0, 1, bins + 1))
    breakpoints[0] = -np.inf
    breakpoints[-1] = np.inf

    expected_hist, _ = np.histogram(expected, bins=breakpoints)
    actual_hist, _ = np.histogram(actual, bins=breakpoints)

    expected_pct = expected_hist / np.maximum(expected_hist.sum(), 1)
    actual_pct = actual_hist / np.maximum(actual_hist.sum(), 1)

    epsilon = 1e-9
    expected_pct = np.clip(expected_pct, epsilon, None)
    actual_pct = np.clip(actual_pct, epsilon, None)

    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    return float(psi)


def psi_status(psi: float) -> str:
    if psi < 0.10:
        return "stable"
    if psi < 0.25:
        return "warning"
    return "critical"


def compute_feature_drift(train_df: pd.DataFrame, serving_df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    rows = []
    for feature in features:
        if feature not in train_df.columns or feature not in serving_df.columns:
            continue

        train_vals = train_df[feature].dropna().to_numpy()
        serving_vals = serving_df[feature].dropna().to_numpy()
        psi = calculate_psi(train_vals, serving_vals)
        rows.append(
            {
                "feature_name": feature,
                "psi": psi,
                "mean_train": float(np.nanmean(train_vals)) if train_vals.size else 0.0,
                "mean_serving": float(np.nanmean(serving_vals)) if serving_vals.size else 0.0,
                "std_train": float(np.nanstd(train_vals)) if train_vals.size else 0.0,
                "std_serving": float(np.nanstd(serving_vals)) if serving_vals.size else 0.0,
                "status": psi_status(psi),
            }
        )

    return pd.DataFrame(rows)
