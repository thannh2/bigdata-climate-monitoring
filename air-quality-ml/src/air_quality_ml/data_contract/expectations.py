from __future__ import annotations

from pyspark.sql import DataFrame

from air_quality_ml.data_contract.validators import (
    assert_required_columns,
    compute_duplicate_rate,
    compute_missing_rate,
    compute_range_violations,
)


def run_data_contract_checks(
    df: DataFrame,
    required_columns: list[str],
    range_rules: dict[str, dict[str, float]],
    duplicate_key: list[str],
) -> dict[str, DataFrame]:
    assert_required_columns(df, required_columns)
    return {
        "missing_rate": compute_missing_rate(df, required_columns),
        "duplicate_rate": compute_duplicate_rate(df, duplicate_key),
        "range_violations": compute_range_violations(df, range_rules),
    }
