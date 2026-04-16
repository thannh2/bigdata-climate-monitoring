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

from producers.kafka_producer import KAFKA_CONFIG_PATH


TOPICS_CONFIG_PATH = ROOT_DIR / "config" / "topics.yaml"
DEFAULT_OUTPUT_DIR = ROOT_DIR.parent / "exports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Kafka topic data to a local JSONL file.")
    parser.add_argument(
        "--topics",
        nargs="*",
        default=None,
        help="One or more Kafka topics. If omitted, exports all configured ingestion topics.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output file path. Defaults to exports/kafka_export_<timestamp>.jsonl",
    )
    parser.add_argument("--max-messages", type=int, default=1000)
    parser.add_argument("--poll-timeout-seconds", type=float, default=1.0)
    parser.add_argument("--idle-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--include-kafka-meta", action="store_true")
    parser.add_argument("--group-id", default=None)
    return parser.parse_args()


def load_bootstrap_servers(config_path: Path) -> list[str]:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    servers = data.get("bootstrap_servers") or []
    if not servers:
        raise ValueError(f"bootstrap_servers is missing in {config_path}")
    return servers


def load_default_topics(config_path: Path) -> list[str]:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    topics = data.get("topics") or []
    if not topics:
        raise ValueError(f"No topics configured in {config_path}")
    return [topic["name"] for topic in topics]


def build_output_path(output_arg: str | None) -> Path:
    if output_arg:
        return Path(output_arg).resolve()
    DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return (DEFAULT_OUTPUT_DIR / f"kafka_export_{timestamp}.jsonl").resolve()


def build_consumer(bootstrap_servers: list[str], group_id: str, from_beginning: bool) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": ",".join(bootstrap_servers),
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )


def decode_message(raw_value: bytes | None) -> Any:
    if raw_value is None:
        return None
    text = raw_value.decode("utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def export_messages(
    topics: list[str],
    output_path: Path,
    bootstrap_servers: list[str],
    group_id: str,
    max_messages: int,
    poll_timeout_seconds: float,
    idle_timeout_seconds: float,
    from_beginning: bool,
    include_kafka_meta: bool,
) -> int:
    consumer = build_consumer(bootstrap_servers, group_id, from_beginning)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    exported_count = 0

    consumer.subscribe(topics)
    deadline = datetime.now(timezone.utc).timestamp() + idle_timeout_seconds

    try:
        with output_path.open("w", encoding="utf-8", newline="\n") as handle:
            while exported_count < max_messages:
                message = consumer.poll(poll_timeout_seconds)
                if message is None:
                    if datetime.now(timezone.utc).timestamp() >= deadline:
                        break
                    continue

                deadline = datetime.now(timezone.utc).timestamp() + idle_timeout_seconds

                if message.error():
                    raise RuntimeError(f"Kafka consume failed: {message.error()}")

                decoded = decode_message(message.value())
                if include_kafka_meta:
                    record = {
                        "kafka_topic": message.topic(),
                        "kafka_partition": message.partition(),
                        "kafka_offset": message.offset(),
                        "exported_at": datetime.now(timezone.utc).isoformat(),
                        "value": decoded,
                    }
                else:
                    record = decoded

                handle.write(json.dumps(record, ensure_ascii=False, default=str))
                handle.write("\n")
                exported_count += 1
    finally:
        consumer.close()

    return exported_count


def main() -> None:
    args = parse_args()
    bootstrap_servers = load_bootstrap_servers(KAFKA_CONFIG_PATH)
    topics = args.topics or load_default_topics(TOPICS_CONFIG_PATH)
    output_path = build_output_path(args.output)
    group_id = args.group_id or f"kafka-export-{uuid.uuid4()}"

    exported_count = export_messages(
        topics=topics,
        output_path=output_path,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        max_messages=args.max_messages,
        poll_timeout_seconds=args.poll_timeout_seconds,
        idle_timeout_seconds=args.idle_timeout_seconds,
        from_beginning=args.from_beginning,
        include_kafka_meta=args.include_kafka_meta,
    )

    print(
        json.dumps(
            {
                "output": str(output_path),
                "topics": topics,
                "exported_count": exported_count,
                "group_id": group_id,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
