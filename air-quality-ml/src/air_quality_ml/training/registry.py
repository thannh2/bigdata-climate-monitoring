from __future__ import annotations

from air_quality_ml.settings import BaseSettings


def should_promote_regression(val_metrics: dict[str, float], settings: BaseSettings) -> bool:
    mae = float(val_metrics.get("mae", 999999.0))
    mae_high = float(val_metrics.get("mae_high_pollution", 999999.0))

    if mae <= 0:
        return False

    return mae_high <= (mae * settings.promotion.max_mae_high_pollution_ratio)


def should_promote_classification(val_metrics: dict[str, float], settings: BaseSettings) -> bool:
    recall = float(val_metrics.get("recall", 0.0))
    fnr = float(val_metrics.get("false_negative_rate", 1.0))

    return (
        recall >= settings.promotion.classification_min_recall
        and fnr <= settings.promotion.classification_max_fnr
    )
