from __future__ import annotations

from typing import Optional

from air_quality_ml.settings import BaseSettings


def should_promote_regression(
    val_metrics: dict[str, float],
    settings: BaseSettings,
    baseline_metrics: Optional[dict[str, float]] = None,
) -> bool:
    """Promote khi:
    - mae hop le (> 0)
    - khong te hon o vung o nhiem cao: mae_high <= mae * max_mae_high_pollution_ratio
    - neu co baseline (vd LinearRegression): mae <= baseline_mae * regression_mae_improve_ratio
    """
    mae = float(val_metrics.get("mae", 999999.0))
    mae_high = float(val_metrics.get("mae_high_pollution", 999999.0))

    if mae <= 0:
        return False

    if mae_high > (mae * settings.promotion.max_mae_high_pollution_ratio):
        return False

    if baseline_metrics:
        baseline_mae = float(baseline_metrics.get("mae", 0.0))
        if baseline_mae > 0 and mae > (baseline_mae * settings.promotion.regression_mae_improve_ratio):
            return False

    return True


def should_promote_classification(
    val_metrics: dict[str, float],
    settings: BaseSettings,
    baseline_metrics: Optional[dict[str, float]] = None,
) -> bool:
    """Promote khi:
    - recall >= classification_min_recall
    - false_negative_rate <= classification_max_fnr
    - neu co baseline: auprc >= baseline_auprc * classification_min_auprc_ratio
    """
    recall = float(val_metrics.get("recall", 0.0))
    fnr = float(val_metrics.get("false_negative_rate", 1.0))

    if recall < settings.promotion.classification_min_recall:
        return False
    if fnr > settings.promotion.classification_max_fnr:
        return False

    if baseline_metrics:
        baseline_auprc = float(baseline_metrics.get("auprc", 0.0))
        auprc = float(val_metrics.get("auprc", 0.0))
        if baseline_auprc > 0 and auprc < (baseline_auprc * settings.promotion.classification_min_auprc_ratio):
            return False

    return True
