from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from air_quality_ml.data_contract.schemas import FeatureTableContract, build_feature_table_contract
from air_quality_ml.settings import BaseSettings


def load_feature_store_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _spark_family(data_type: str) -> str:
    normalized = data_type.lower()
    if normalized in {"string"}:
        return "string"
    if normalized in {"timestamp", "timestamp_ntz"}:
        return "timestamp"
    if normalized in {"int", "integer", "bigint", "smallint", "tinyint", "long", "short"}:
        return "integer"
    if normalized in {"float", "double", "decimal", "bigdecimal"} or normalized.startswith("decimal"):
        return "numeric"
    if normalized in {"boolean"}:
        return "boolean"
    return normalized


def _expected_vs_actual_matches(expected_family: str, actual_type: str) -> bool:
    actual_family = _spark_family(actual_type)
    if expected_family == "numeric":
        return actual_family in {"numeric", "integer"}
    return actual_family == expected_family


def _missing_columns(df: DataFrame, contract: FeatureTableContract) -> list[str]:
    existing = set(df.columns)
    return [col.name for col in contract.required_columns if col.name not in existing]


def _type_issues(df: DataFrame, contract: FeatureTableContract) -> list[dict[str, str]]:
    type_map = dict(df.dtypes)
    issues: list[dict[str, str]] = []
    for column in contract.required_columns:
        actual_type = type_map.get(column.name)
        if actual_type is None:
            continue
        if not _expected_vs_actual_matches(column.expected_family, actual_type):
            issues.append(
                {
                    "column": column.name,
                    "expected_family": column.expected_family,
                    "actual_type": actual_type,
                }
            )
    return issues


def _null_violations(df: DataFrame, contract: FeatureTableContract) -> dict[str, float]:
    non_nullable = [col.name for col in contract.required_columns if not col.nullable and col.name in df.columns]
    if not non_nullable:
        return {}

    row = (
        df.select(
            *[
                F.avg(F.when(F.col(column).isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias(column)
                for column in non_nullable
            ]
        )
        .collect()[0]
    )
    return {column: float(row[column] or 0.0) for column in non_nullable if float(row[column] or 0.0) > 0.0}


def _duplicate_rows(df: DataFrame, duplicate_key_columns: list[str]) -> int:
    if not duplicate_key_columns or any(column not in df.columns for column in duplicate_key_columns):
        return 0

    row = (
        df.groupBy(*duplicate_key_columns)
        .count()
        .filter(F.col("count") > 1)
        .agg(F.sum(F.col("count") - F.lit(1)).alias("duplicate_rows"))
        .collect()[0]
    )
    return int(row["duplicate_rows"] or 0)


def _invalid_regions(df: DataFrame, allowed_regions: list[str]) -> int:
    if "region" not in df.columns or not allowed_regions:
        return 0
    return int(df.filter(~F.col("region").isin(*allowed_regions)).count())


def _pandera_validate_sample(df: DataFrame, sample_rows: int) -> dict[str, Any]:
    try:
        import pandera as pa
    except ImportError:
        return {"status": "skipped", "reason": "pandera_not_installed"}

    required = [column for column in ["station_id", "timestamp", "latitude", "longitude", "temp_c", "humidity", "pressure", "wind_speed", "pm2_5", "us_aqi"] if column in df.columns]
    if not required:
        return {"status": "skipped", "reason": "no_columns_for_pandera"}

    pdf = df.select(*required).limit(int(sample_rows)).toPandas()
    if pdf.empty:
        return {"status": "skipped", "reason": "empty_sample"}

    schema_columns = {}
    if "station_id" in pdf.columns:
        schema_columns["station_id"] = pa.Column(str, nullable=False)
    if "timestamp" in pdf.columns:
        schema_columns["timestamp"] = pa.Column(pa.DateTime, nullable=False)
    for column in ["latitude", "longitude", "temp_c", "humidity", "pressure", "wind_speed", "pm2_5", "us_aqi"]:
        if column in pdf.columns:
            schema_columns[column] = pa.Column(float, nullable=True)

    schema = pa.DataFrameSchema(schema_columns, coerce=True)

    try:
        schema.validate(pdf, lazy=True)
        return {"status": "passed", "sample_rows": len(pdf)}
    except Exception as exc:  # pragma: no cover - depends on optional package
        return {"status": "failed", "sample_rows": len(pdf), "error": str(exc)}


def validate_feature_contract(
    df: DataFrame,
    settings: BaseSettings,
    feature_store_path: str | Path,
) -> dict[str, Any]:
    feature_store_cfg = load_feature_store_config(feature_store_path)
    contract = build_feature_table_contract(settings, feature_store_cfg)

    missing_columns = _missing_columns(df, contract)
    type_issues = _type_issues(df, contract)
    null_violations = _null_violations(df, contract)
    duplicate_rows = _duplicate_rows(df, contract.duplicate_key_columns)
    invalid_regions = _invalid_regions(df, contract.allowed_regions)

    pandera_result = {"status": "skipped", "reason": "disabled"}
    if settings.data_contract.run_pandera:
        pandera_result = _pandera_validate_sample(df, sample_rows=settings.data_contract.pandera_sample_rows)

    issues: list[dict[str, Any]] = []
    if missing_columns:
        issues.append({"check": "missing_columns", "severity": "error", "details": missing_columns})
    if type_issues:
        issues.append({"check": "type_mismatch", "severity": "error", "details": type_issues})
    if null_violations:
        issues.append({"check": "non_nullable_null_rate", "severity": "error", "details": null_violations})
    if duplicate_rows > 0:
        issues.append({"check": "duplicate_keys", "severity": "error", "details": {"duplicate_rows": duplicate_rows}})
    if invalid_regions > 0:
        issues.append({"check": "invalid_regions", "severity": "warning", "details": {"invalid_region_rows": invalid_regions}})
    if pandera_result.get("status") == "failed":
        issues.append({"check": "pandera_sample_validation", "severity": "error", "details": pandera_result})

    status = "passed" if not any(issue["severity"] == "error" for issue in issues) else "failed"
    return {
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "row_count": int(df.count()),
        "contract": contract.to_dict(),
        "pandera": pandera_result,
        "issues": issues,
    }
