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


def classification_drift_status(current_recall: float, baseline_recall: float, current_auprc: float, baseline_auprc: float) -> str:
    recall_drop_pp = (baseline_recall - current_recall) * 100.0
    auprc_drop_ratio = ((baseline_auprc - current_auprc) / baseline_auprc) if baseline_auprc > 0 else 0.0

    if recall_drop_pp > 10 or auprc_drop_ratio > 0.20:
        return "critical"
    if recall_drop_pp > 5 or auprc_drop_ratio > 0.10:
        return "warning"
    return "stable"
