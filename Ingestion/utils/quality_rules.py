from __future__ import annotations

from math import isfinite
from typing import Any


WEATHER_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "event_time"},
        "description": "event_time must be present",
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "city"},
        "description": "city must be present",
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "source"},
        "description": "source must be present",
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "temperature_c", "min_value": -20, "max_value": 60},
        "description": "temperature_c should stay in a reasonable range",
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "humidity", "min_value": 0, "max_value": 100},
        "description": "humidity should be between 0 and 100",
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "wind_speed_mps", "min_value": 0},
        "description": "wind_speed_mps should be non-negative",
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "pressure_hpa", "min_value": 0, "strict_min": True},
        "description": "pressure_hpa should be positive",
    },
]


AIR_QUALITY_EXPECTATIONS: list[dict[str, Any]] = [
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "event_time"},
        "description": "event_time must be present",
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "city"},
        "description": "city must be present",
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "source"},
        "description": "source must be present",
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "aqi", "min_value": 0},
        "description": "aqi should be non-negative",
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "pm25", "min_value": 0},
        "description": "pm25 should be non-negative",
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "pm10", "min_value": 0},
        "description": "pm10 should be non-negative",
    },
]


def get_expectation_suite(entity: str) -> list[dict[str, Any]]:
    if entity == "weather":
        return WEATHER_EXPECTATIONS
    if entity == "air_quality":
        return AIR_QUALITY_EXPECTATIONS
    raise ValueError(f"Unsupported entity: {entity}")


def evaluate_record_expectations(record: dict[str, Any], expectations: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    for expectation in expectations:
        expectation_type = expectation["expectation_type"]
        kwargs = expectation["kwargs"]
        column = kwargs["column"]
        value = record.get(column)

        if expectation_type == "expect_column_values_to_not_be_null":
            if value is None or value == "":
                errors.append(f"{column} is required")
            continue

        if expectation_type == "expect_column_values_to_be_between":
            if value is None:
                continue
            if not isinstance(value, (int, float)) or not isfinite(float(value)):
                errors.append(f"{column} must be numeric")
                continue

            number = float(value)
            min_value = kwargs.get("min_value")
            max_value = kwargs.get("max_value")
            strict_min = kwargs.get("strict_min", False)
            strict_max = kwargs.get("strict_max", False)

            if min_value is not None:
                if strict_min and number <= float(min_value):
                    errors.append(f"{column} must be > {min_value}")
                    continue
                if not strict_min and number < float(min_value):
                    errors.append(f"{column} must be >= {min_value}")
                    continue

            if max_value is not None:
                if strict_max and number >= float(max_value):
                    errors.append(f"{column} must be < {max_value}")
                    continue
                if not strict_max and number > float(max_value):
                    errors.append(f"{column} must be <= {max_value}")

    return errors


def build_expectation_suite_document(entity: str) -> dict[str, Any]:
    return {
        "suite_name": f"{entity}_input_quality_suite",
        "expectations": get_expectation_suite(entity),
        "meta": {
            "owner": "ingestion",
            "purpose": "GE-aligned input quality rules for normalized ingestion records",
        },
    }
