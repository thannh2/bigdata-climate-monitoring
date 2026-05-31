from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from air_quality_ml.features.l4_targets import HORIZONS, L4_TARGET_SPECS


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate training configs for all L4 targets + alert classifiers")
    parser.add_argument("--output-dir", default=str(ROOT / "configs" / "generated" / "l4"))
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--no-alert",
        action="store_true",
        help="Skip generating alert classifier configs (target_alert_{h}h)",
    )
    parser.add_argument(
        "--full-search",
        action="store_true",
        help="Use the larger model_selection space (more candidates, slower). Default is a reduced 2-candidate space.",
    )
    return parser.parse_args()


def _tree_params(horizon: int) -> dict:
    # Tham so GBT da giam de train nhanh hon (van giu chat luong hop ly).
    return {
        "maxDepth": 5 if horizon <= 6 else 6,
        "maxIter": 50 if horizon <= 6 else 70,
        "stepSize": 0.1,
    }


def _regression_model_selection(params: dict, full_search: bool) -> dict:
    if full_search:
        return {
            "metric": "mae",
            "candidates": [
                {"model_type": "linear", "params": {}},
                {"model_type": "rf", "params": {"numTrees": 120, "maxDepth": 10, "subsamplingRate": 0.85}},
                {"model_type": "rf", "params": {"numTrees": 200, "maxDepth": 12, "subsamplingRate": 0.8}},
                {"model_type": "gbt", "params": {"maxIter": 100, "maxDepth": 5, "stepSize": 0.05, "subsamplingRate": 0.85}},
                {"model_type": "gbt", "params": {"maxIter": 120, "maxDepth": 8, "stepSize": 0.05, "subsamplingRate": 0.85}},
            ],
        }
    # Reduced: 1 baseline (de promotion so sanh) + 1 GBT chinh.
    return {
        "metric": "mae",
        "candidates": [
            {"model_type": "linear", "params": {}},
            {"model_type": "gbt", "params": dict(params)},
        ],
    }


def _classification_model_selection(params: dict, full_search: bool) -> dict:
    if full_search:
        return {
            "metric": "auprc",
            "candidates": [
                {"model_type": "logistic", "params": {"maxIter": 100}},
                {"model_type": "rf", "params": {"numTrees": 150, "maxDepth": 10}},
                {"model_type": "gbt", "params": {"maxIter": 100, "maxDepth": 5, "stepSize": 0.05}},
                {"model_type": "gbt", "params": {"maxIter": 120, "maxDepth": 8, "stepSize": 0.05}},
            ],
        }
    # Reduced: 1 baseline (de promotion so sanh) + 1 GBT chinh.
    return {
        "metric": "auprc",
        "candidates": [
            {"model_type": "logistic", "params": {"maxIter": 100}},
            {"model_type": "gbt", "params": dict(params)},
        ],
    }


def _config_for_target(spec, horizon: int, full_search: bool = False) -> dict:
    target_col = spec.column_for_horizon(horizon)
    params = _tree_params(horizon)
    if spec.task == "regression":
        params["subsamplingRate"] = 0.85

    return {
        "task": spec.task,
        "horizon": horizon,
        "target_col": target_col,
        "target_name": spec.target_name,
        "prediction_unit": spec.unit,
        "output_kind": spec.task,
        "prediction_col": "prediction",
        "model_type": "gbt",
        "model_name": f"aq_{spec.target_name}_h{horizon}",
        "experiment_name": f"air-quality/l4/{spec.target_name}/h{horizon}",
        "training": {
            "label_col": target_col,
            "max_rows": None,
            "dropna_label": True,
        },
        "params": params,
        "model_selection": _regression_model_selection(params, full_search),
        "register_if_pass": True,
    }


def _config_for_alert(horizon: int, full_search: bool = False) -> dict:
    target_col = f"target_alert_{horizon}h"
    params = _tree_params(horizon)
    return {
        "task": "classification",
        "horizon": horizon,
        "target_col": target_col,
        "target_name": "alert",
        "prediction_unit": "binary",
        "output_kind": "classification",
        "prediction_col": "prediction",
        "probability_col": "probability",
        "model_type": "gbt",
        "model_name": f"aq_alert_classifier_h{horizon}",
        "experiment_name": f"air-quality/alert-classifier/h{horizon}",
        "training": {
            "label_col": target_col,
            "max_rows": None,
            "dropna_label": True,
        },
        "params": params,
        "threshold_tuning": {
            "enabled": True,
            "min_precision": 0.35,
            "min_recall": 0.80,
            "default_threshold": 0.50,
        },
        "model_selection": _classification_model_selection(params, full_search),
        "register_if_pass": True,
    }


def _write_config(path: Path, cfg: dict, overwrite: bool) -> bool:
    if path.exists() and not overwrite:
        return False
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False, allow_unicode=True)
    return True


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    skipped = 0

    for spec in L4_TARGET_SPECS:
        for horizon in spec.horizons:
            path = output_dir / f"{spec.target_name}_h{horizon}.yaml"
            if _write_config(path, _config_for_target(spec, horizon, args.full_search), args.overwrite):
                written += 1
            else:
                skipped += 1

    if not args.no_alert:
        for horizon in HORIZONS:
            path = output_dir / f"alert_h{horizon}.yaml"
            if _write_config(path, _config_for_alert(horizon, args.full_search), args.overwrite):
                written += 1
            else:
                skipped += 1

    print(f"Generated L4 configs: written={written}, skipped={skipped}, output_dir={output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
