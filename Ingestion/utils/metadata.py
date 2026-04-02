from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def build_batch_id(prefix: str, start_date: str | None = None, end_date: str | None = None) -> str:
    parts = [prefix]
    if start_date:
        parts.append(start_date.replace("-", ""))
    if end_date:
        parts.append(end_date.replace("-", ""))
    parts.append(datetime.now(UTC).strftime("%Y%m%d_%H%M%S"))
    return "_".join(parts)


def build_cycle_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"


def enrich_ingestion_metadata(
    record: dict[str, Any],
    pipeline_name: str,
    batch_id: str,
    source_endpoint: str,
    ingestion_status: str = "success",
) -> dict[str, Any]:
    enriched = dict(record)
    region = enriched.get("region")
    partition_key = f"region_{region}" if region else None
    enriched.update(
        {
            "pipeline_name": pipeline_name,
            "batch_id": batch_id,
            "source_endpoint": source_endpoint,
            "ingestion_status": ingestion_status,
            "partition_key": partition_key,
        }
    )
    return enriched
