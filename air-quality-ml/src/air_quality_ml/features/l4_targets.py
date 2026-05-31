from __future__ import annotations

from dataclasses import dataclass


# Cac moc thoi gian du bao (gio) ap dung cho moi L4 target.
HORIZONS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 9, 12, 15, 18, 21, 24)


@dataclass(frozen=True)
class L4TargetSpec:
    target_name: str
    source_col: str
    task: str
    unit: str
    horizons: tuple[int, ...]
    column_prefix: str

    def column_for_horizon(self, horizon: int) -> str:
        return f"{self.column_prefix}_{horizon}h"


# Chi giu lai cac target regression dang dung (green). Tat ca sinh bang lead(source_col, h).
L4_TARGET_SPECS: tuple[L4TargetSpec, ...] = (
    L4TargetSpec("temp", "temp_c", "regression", "celsius", HORIZONS, "target_temp"),
    L4TargetSpec("pm25", "pm2_5", "regression", "ug_m3", HORIZONS, "target_pm25"),
    L4TargetSpec("cloud_cover", "cloud_cover", "regression", "percent", HORIZONS, "target_cloud_cover"),
    L4TargetSpec("precipitation", "precipitation", "regression", "mm", HORIZONS, "target_precipitation"),
    L4TargetSpec("wind_speed", "wind_speed", "regression", "m_s", HORIZONS, "target_wind_speed"),
    L4TargetSpec("pressure", "pressure", "regression", "hpa", HORIZONS, "target_pressure"),
)


def iter_l4_target_columns() -> list[str]:
    return [spec.column_for_horizon(h) for spec in L4_TARGET_SPECS for h in spec.horizons]


def infer_target_col_from_model_name(model_name: str, horizon: int) -> str:
    """Suy ra target column tu ten model + horizon.

    Quy uoc dat ten (generator): green = aq_{target_name}_h{h}, alert = aq_alert_classifier_h{h}.
    """
    name = model_name.lower()
    if "alert" in name:
        return f"target_alert_{horizon}h"
    for spec in L4_TARGET_SPECS:
        if spec.target_name in name:
            return spec.column_for_horizon(horizon)
    return f"target_unknown_{horizon}h"


def find_l4_target(target_col: str) -> tuple[L4TargetSpec | None, int | None]:
    for spec in L4_TARGET_SPECS:
        prefix = f"{spec.column_prefix}_"
        if not target_col.startswith(prefix) or not target_col.endswith("h"):
            continue
        value = target_col.removeprefix(prefix).removesuffix("h")
        if value.isdigit():
            horizon = int(value)
            if horizon in spec.horizons:
                return spec, horizon
    return None, None


def target_metadata(target_col: str, fallback_task: str | None = None) -> dict[str, str]:
    spec, horizon = find_l4_target(target_col)
    if spec is None or horizon is None:
        return {
            "target_name": target_col.removeprefix("target_"),
            "target_col": target_col,
            "target_family": "unknown",
            "prediction_unit": "unknown",
        }
    return {
        "target_name": spec.target_name,
        "target_col": target_col,
        "target_family": fallback_task or spec.task,
        "prediction_unit": spec.unit,
    }
