from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_checkpoint(checkpoint_path: Path) -> dict[str, Any]:
    if not checkpoint_path.exists():
        return {"locations": {}}
    return json.loads(checkpoint_path.read_text(encoding="utf-8"))


def save_checkpoint(checkpoint_path: Path, payload: dict[str, Any]) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def get_location_checkpoint(checkpoint: dict[str, Any], city: str) -> dict[str, Any]:
    return (checkpoint.get("locations") or {}).get(city, {})


def update_location_checkpoint(
    checkpoint: dict[str, Any],
    city: str,
    event_time: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    locations = checkpoint.setdefault("locations", {})
    locations[city] = {
        "event_time": event_time,
        **(metadata or {}),
    }
    return checkpoint
