from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from air_quality_ml.settings import BaseSettings
from air_quality_ml.features.l4_targets import iter_l4_target_columns


@dataclass
class ColumnContract:
    name: str
    expected_family: str
    nullable: bool = True


@dataclass
class FeatureTableContract:
    required_columns: list[ColumnContract] = field(default_factory=list)
    duplicate_key_columns: list[str] = field(default_factory=list)
    allowed_regions: list[str] = field(default_factory=list)
    target_columns: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "required_columns": [asdict(col) for col in self.required_columns],
            "duplicate_key_columns": list(self.duplicate_key_columns),
            "allowed_regions": list(self.allowed_regions),
            "target_columns": list(self.target_columns),
        }


def build_feature_table_contract(settings: BaseSettings, feature_store_cfg: dict[str, Any]) -> FeatureTableContract:
    numeric_features = list(feature_store_cfg.get("numeric_features", []))
    categorical_features = list(feature_store_cfg.get("categorical_features", []))
    l1_features = list(feature_store_cfg.get("l1_features", []))
    l2_features = list(feature_store_cfg.get("l2_features", []))
    l3_features = list(feature_store_cfg.get("l3_features", []))

    target_columns = iter_l4_target_columns() + [f"target_alert_{h}h" for h in settings.features.horizons]

    required_columns = [
        ColumnContract(name="station_id", expected_family="string", nullable=False),
        ColumnContract(name="timestamp", expected_family="timestamp", nullable=False),
    ]

    for name in settings.data_contract.required_core_columns:
        if name in {"station_id", "timestamp"}:
            continue
        expected_family = "numeric"
        if name in {"region", "city"}:
            expected_family = "string"
        required_columns.append(ColumnContract(name=name, expected_family=expected_family, nullable=True))

    existing_required = {col.name for col in required_columns}
    for name in categorical_features:
        if name not in existing_required:
            required_columns.append(ColumnContract(name=name, expected_family="string", nullable=True))
            existing_required.add(name)

    for name in numeric_features + l1_features + l2_features + l3_features:
        if name not in existing_required:
            required_columns.append(ColumnContract(name=name, expected_family="numeric", nullable=True))
            existing_required.add(name)

    return FeatureTableContract(
        required_columns=required_columns,
        duplicate_key_columns=list(settings.data_contract.duplicate_key_columns),
        allowed_regions=list(settings.data_contract.allowed_regions),
        target_columns=target_columns,
    )
