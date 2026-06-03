from __future__ import annotations


def regression_drift_status(current_mae: float, baseline_mae: float) -> str:
    if baseline_mae <= 0:
        return "unknown"

    ratio = (current_mae - baseline_mae) / baseline_mae
    if ratio > 0.20:
        return "critical"
    if ratio > 0.10:
        return "warning"
    return "stable"
