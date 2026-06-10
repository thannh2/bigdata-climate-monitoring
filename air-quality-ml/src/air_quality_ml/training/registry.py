from __future__ import annotations

from air_quality_ml.settings import BaseSettings


def should_promote_regression(val_metrics: dict[str, float], settings: BaseSettings) -> bool:
    mae = float(val_metrics.get("mae", 999999.0))
    mae_high = float(val_metrics.get("mae_high_pollution", 999999.0))

    if mae <= 0:
        return False

    return mae_high <= (mae * settings.promotion.max_mae_high_pollution_ratio)
