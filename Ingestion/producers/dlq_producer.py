from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Producer

from producers.kafka_producer import produce_json_message


def build_dlq_message(
    pipeline_name: str,
    batch_id: str | None,
    entity: str,
    error_type: str,
    error_message: str,
    raw_payload: dict[str, Any] | None = None,
    normalized_payload: dict[str, Any] | None = None,
    source: str | None = None,
    city: str | None = None,
    event_time: str | None = None,
    topic: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "pipeline_name": pipeline_name,
        "batch_id": batch_id,
        "entity": entity,
        "source": source,
        "city": city,
        "event_time": event_time,
        "target_topic": topic,
        "error_type": error_type,
        "error_message": error_message,
        "raw_payload": raw_payload,
        "normalized_payload": normalized_payload,
        "failed_at": datetime.now(UTC).isoformat(),
        "extra": extra or {},
    }


def send_to_dlq(
    producer: Producer,
    dlq_topic: str,
    message: dict[str, Any],
    key: str | None = None,
) -> dict[str, Any]:
    return produce_json_message(producer=producer, topic=dlq_topic, message=message, key=key)
