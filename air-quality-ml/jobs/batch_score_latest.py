from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    model_uri = os.getenv("MODEL_URI", "models:/aq_pm25_forecast_h1/Production")
    horizon = os.getenv("HORIZON", "1")
    alert_threshold = os.getenv("ALERT_THRESHOLD", "0.5")
    target_col = os.getenv("TARGET_COL")
    target_name = os.getenv("TARGET_NAME")
    prediction_unit = os.getenv("PREDICTION_UNIT")
    output_kind = os.getenv("OUTPUT_KIND")

    cmd = [
        sys.executable,
        "-m",
        "air_quality_ml.inference.batch_score",
        "--base-config",
        str(ROOT / "configs" / "base.yaml"),
        "--model-uri",
        model_uri,
        "--horizon",
        str(horizon),
        "--alert-threshold",
        str(alert_threshold),
    ]

    optional_args = {
        "--target-col": target_col,
        "--target-name": target_name,
        "--prediction-unit": prediction_unit,
        "--output-kind": output_kind,
    }
    for flag, value in optional_args.items():
        if value:
            cmd.extend([flag, value])

    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB", "air_quality")
    if mongo_uri:
        cmd.extend(["--mongo-uri", mongo_uri, "--mongo-db", mongo_db])

    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
