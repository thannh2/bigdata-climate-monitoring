from __future__ import annotations

from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT_DIR.parent
ENV_PATH = ROOT_DIR / "config" / ".env"
CHECKPOINT_DIR = REPO_ROOT / "checkpoint"
EXPORTS_DIR = REPO_ROOT / "exports"


def build_checkpoint_path(filename: str) -> Path:
    return CHECKPOINT_DIR / filename
