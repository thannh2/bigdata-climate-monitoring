from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    cmd = [
        sys.executable,
        "-m",
        "air_quality_ml.training.train_job",
        "--base-config",
        str(ROOT / "configs" / "base.yaml"),
        "--job-config",
        str(ROOT / "configs" / "training_pm25_h12.yaml"),
    ]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
