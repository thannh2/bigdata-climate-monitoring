from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer
import yaml

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from producers.kafka_producer import KAFKA_CONFIG_PATH, send_json_message


TOPICS_CONFIG_PATH = ROOT_DIR / "config" / "topics.yaml"


def load_bootstrap_servers(config_path: Path) -> list[str]:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    servers = data.get("bootstrap_servers") or []
    if not servers:
        raise ValueError(f"bootstrap_servers is missing in {config_path}")
    return servers


def load_first_topic(config_path: Path) -> str:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    topics = data.get("topics") or []
    if not topics:
        raise ValueError(f"No topics configured in {config_path}")
    return topics[0]["name"]


def build_test_message(topic: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "event_id": str(uuid.uuid4()),
        "pipeline_name": "kafka_smoke_test",
        "batch_id": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
        "source": "smoke-test",
        "source_endpoint": "/internal/smoke-test",
        "ingestion_status": "success",
        "partition_key": "region_bac",
        "event_time": now,
        "ingestion_time": now,
        "city": "Hanoi",
        "region": "bac",
        "topic": topic,
    }


def consume_single_message(topic: str, bootstrap_servers: list[str], group_id: str) -> dict[str, Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": ",".join(bootstrap_servers),
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    try:
        deadline = datetime.now(timezone.utc).timestamp() + 15
        while datetime.now(timezone.utc).timestamp() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Kafka consume failed: {message.error()}")
            return {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "value": json.loads(message.value().decode("utf-8")),
            }
    finally:
        consumer.close()
    raise TimeoutError(f"No message consumed from {topic}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test Kafka producer/consumer.")
    parser.add_argument("--topic", default=None)
    parser.add_argument("--region-key", default="region_bac")
    args = parser.parse_args()

    topic = args.topic or load_first_topic(TOPICS_CONFIG_PATH)
    bootstrap_servers = load_bootstrap_servers(KAFKA_CONFIG_PATH)
    message = build_test_message(topic)
    message["partition_key"] = args.region_key

    metadata = send_json_message(topic=topic, key=args.region_key, message=message)
    consumed = consume_single_message(
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        group_id=f"smoke-test-{uuid.uuid4()}",
    )

    print("PRODUCED", json.dumps(metadata, ensure_ascii=True))
    print("CONSUMED", json.dumps(consumed, ensure_ascii=True))


if __name__ == "__main__":
    main()
