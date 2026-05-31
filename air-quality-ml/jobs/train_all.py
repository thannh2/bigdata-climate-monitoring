from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_DIR = ROOT / "configs" / "generated" / "l4"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train all generated L4 + alert configs sequentially")
    parser.add_argument("--config-dir", default=str(DEFAULT_CONFIG_DIR))
    parser.add_argument("--base-config", default=str(ROOT / "configs" / "base.yaml"))
    parser.add_argument(
        "--filter",
        default=None,
        help="Only run configs whose filename contains this substring (e.g. 'pm25', 'alert', 'h24')",
    )
    parser.add_argument("--data-path", default=None, help="Optional override for training table path")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop at first failing config")
    return parser.parse_args()


def _iter_configs(config_dir: Path, name_filter: str | None) -> list[Path]:
    configs = sorted(config_dir.glob("*.yaml"))
    if name_filter:
        configs = [c for c in configs if name_filter in c.name]
    return configs


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

    print(f"[INFO] Training {len(configs)} configs from {config_dir}")
    failures: list[str] = []
    for index, config_path in enumerate(configs, start=1):
        print(f"\n[{index}/{len(configs)}] === {config_path.name} ===")
        cmd = [
            sys.executable,
            "-m",
            "air_quality_ml.training.train_job",
            "--base-config",
            args.base_config,
            "--job-config",
            str(config_path),
        ]
        if args.data_path:
            cmd.extend(["--data-path", args.data_path])

        return_code = subprocess.call(cmd)
        if return_code != 0:
            failures.append(config_path.name)
            print(f"[ERROR] Failed: {config_path.name} (exit {return_code})")
            if args.stop_on_error:
                break

    print(f"\n[INFO] Done. success={len(configs) - len(failures)}, failed={len(failures)}")
    if failures:
        print(f"[INFO] Failed configs: {failures}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
