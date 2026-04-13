from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
import yaml


ROOT_DIR = Path(__file__).resolve().parents[1]
KAFKA_CONFIG_PATH = ROOT_DIR / "config" / "kafka_config.yaml"


def load_kafka_config(config_path: Path | None = None) -> dict[str, Any]:
    path = config_path or KAFKA_CONFIG_PATH
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "bootstrap_servers" not in data:
        raise ValueError(f"bootstrap_servers is missing in {path}")
    override = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if override:
        data["bootstrap_servers"] = [server.strip() for server in override.split(",") if server.strip()]
    return data


def build_producer(config_path: Path | None = None) -> Producer:
    config = load_kafka_config(config_path)
    return Producer(
        {
            "bootstrap.servers": ",".join(config["bootstrap_servers"]),
            "client.id": config.get("client_id", "climate-monitoring-ingestion"),
            "acks": config.get("acks", "all"),
            "compression.type": config.get("compression_type", "gzip"),
            "linger.ms": int(config.get("linger_ms", 0)),
            "request.timeout.ms": int(config.get("request_timeout_ms", 30000)),
        }
    )


def send_json_message(
    topic: str,
    message: dict[str, Any],
    key: str | None = None,
    config_path: Path | None = None,
) -> dict[str, Any]:
    producer = build_producer(config_path)
    try:
        return produce_json_message(producer, topic=topic, message=message, key=key)
    finally:
        producer.flush()


def produce_json_message(
    producer: Producer,
    topic: str,
    message: dict[str, Any],
    key: str | None = None,
) -> dict[str, Any]:
    delivery_result: dict[str, Any] = {}

    def on_delivery(error: Any, kafka_message: Any) -> None:
        if error is not None:
            raise RuntimeError(f"Kafka delivery failed: {error}")
        delivery_result.update(
            {
                "topic": kafka_message.topic(),
                "partition": kafka_message.partition(),
                "offset": kafka_message.offset(),
            }
        )

    producer.produce(
        topic=topic,
        key=key.encode("utf-8") if isinstance(key, str) else key,
        value=json.dumps(message, ensure_ascii=True, default=str).encode("utf-8"),
        on_delivery=on_delivery,
    )
    producer.flush()
    if not delivery_result:
        raise RuntimeError("Kafka delivery callback did not return metadata")
    return delivery_result
