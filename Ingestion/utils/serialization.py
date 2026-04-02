from __future__ import annotations

from datetime import datetime
from typing import Any


def serialize_record(record: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif hasattr(value, "value"):
            serialized[key] = value.value
        else:
            serialized[key] = value
    return serialized
