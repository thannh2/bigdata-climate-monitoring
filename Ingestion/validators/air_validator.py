from __future__ import annotations

from math import isfinite
from typing import Any

from utils.quality_rules import evaluate_record_expectations, get_expectation_suite


def validate_air_record(record: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    for field in ("latitude", "longitude"):
        value = record.get(field)
        if value is None:
            errors.append(f"{field} is required")
        elif not isinstance(value, (int, float)) or not isfinite(float(value)):
            errors.append(f"{field} must be numeric")

    numeric_fields = ("aqi", "pm25", "pm10", "co", "no2", "so2", "o3")
    for field in numeric_fields:
        value = record.get(field)
        if value is None:
            continue
        if not isinstance(value, (int, float)) or not isfinite(float(value)):
            errors.append(f"{field} must be numeric")

    errors.extend(evaluate_record_expectations(record, get_expectation_suite("air_quality")))
    return errors
