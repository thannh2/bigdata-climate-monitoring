from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_DIR = ROOT / "configs" / "generated" / "l4"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch score all trained models derived from generated configs")
    parser.add_argument("--config-dir", default=str(DEFAULT_CONFIG_DIR))
    parser.add_argument("--base-config", default=str(ROOT / "configs" / "base.yaml"))
    parser.add_argument("--stage", default="latest", help="MLflow registry stage/alias, e.g. Production or latest")
    parser.add_argument("--filter", default=None, help="Only score configs whose filename contains this substring")
    parser.add_argument("--alert-threshold", default="0.5")
    parser.add_argument("--input-path", default=None)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--mongo-uri", default=None)
    parser.add_argument("--mongo-db", default="air_quality")
    parser.add_argument("--mongo-collection", default="realtime_predictions")
    parser.add_argument("--stop-on-error", action="store_true")
    return parser.parse_args()


def _iter_configs(config_dir: Path, name_filter: str | None) -> list[Path]:
    configs = sorted(config_dir.glob("*.yaml"))
    if name_filter:
        configs = [c for c in configs if name_filter in c.name]
    return configs


def _score_one(cfg: dict, args: argparse.Namespace) -> int:
    model_name = cfg["model_name"]
    horizon = int(cfg["horizon"])
    model_uri = f"models:/{model_name}/{args.stage}"

    cmd = [
        sys.executable,
        "-m",
        "air_quality_ml.inference.batch_score",
        "--base-config",
        args.base_config,
        "--model-uri",
        model_uri,
        "--horizon",
        str(horizon),
        "--target-col",
        cfg["target_col"],
        "--target-name",
        cfg.get("target_name", ""),
        "--prediction-unit",
        cfg.get("prediction_unit", ""),
        "--output-kind",
        cfg.get("output_kind", cfg.get("task", "")),
        "--alert-threshold",
        str(args.alert_threshold),
    ]
    if args.input_path:
        cmd.extend(["--input-path", args.input_path])
    if args.output_path:
        cmd.extend(["--output-path", args.output_path])
    if args.mongo_uri:
        cmd.extend(
            ["--mongo-uri", args.mongo_uri, "--mongo-db", args.mongo_db, "--mongo-collection", args.mongo_collection]
        )
    return subprocess.call(cmd)


def main() -> int:
    args = parse_args()
    config_dir = Path(args.config_dir)
    if not config_dir.exists():
        print(f"[ERROR] Config dir not found: {config_dir}")
        print("Run: python jobs/generate_l4_training_configs.py")
        return 1

    configs = _iter_configs(config_dir, args.filter)
    if not configs:
        print(f"[WARN] No configs matched in {config_dir} (filter={args.filter!r})")
        return 0

    print(f"[INFO] Scoring {len(configs)} models from {config_dir} (stage={args.stage})")
    failures: list[str] = []
    for index, config_path in enumerate(configs, start=1):
        with config_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        print(f"\n[{index}/{len(configs)}] === {cfg['model_name']} (h{cfg['horizon']}) ===")
        return_code = _score_one(cfg, args)
        if return_code != 0:
            failures.append(config_path.name)
            print(f"[ERROR] Failed scoring: {config_path.name} (exit {return_code})")
            if args.stop_on_error:
                break

    print(f"\n[INFO] Done. success={len(configs) - len(failures)}, failed={len(failures)}")
    if failures:
        print(f"[INFO] Failed: {failures}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
