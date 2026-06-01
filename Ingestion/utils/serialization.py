from __future__ import annotations

from datetime import datetime
from typing import Any


def model_to_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


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
