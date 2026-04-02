from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import Any

import yaml


ROOT_DIR = Path(__file__).resolve().parents[1]
TOPICS_CONFIG_PATH = ROOT_DIR / "config" / "topics.yaml"


def load_topics_config(config_path: Path) -> list[dict[str, Any]]:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    topics = data.get("topics") or []
    if not topics:
        raise ValueError(f"No topics configured in {config_path}")
    return topics


def build_kafka_topics_command(
    bootstrap_server: str,
    topic_name: str,
    partitions: int,
    replication_factor: int,
) -> list[str]:
    return [
        "docker",
        "compose",
        "-f",
        str(ROOT_DIR / "docker-compose.yml"),
        "exec",
        "-T",
        "kafka",
        "kafka-topics",
        "--bootstrap-server",
        bootstrap_server,
        "--create",
        "--if-not-exists",
        "--topic",
        topic_name,
        "--partitions",
        str(partitions),
        "--replication-factor",
        str(replication_factor),
    ]


def create_topics(bootstrap_server: str, config_path: Path) -> None:
    for topic in load_topics_config(config_path):
        command = build_kafka_topics_command(
            bootstrap_server=bootstrap_server,
            topic_name=topic["name"],
            partitions=int(topic.get("partitions", 3)),
            replication_factor=int(topic.get("replication_factor", 1)),
        )
        subprocess.run(command, cwd=ROOT_DIR, check=True)


def list_topics(bootstrap_server: str) -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT_DIR / "docker-compose.yml"),
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            bootstrap_server,
            "--list",
        ],
        cwd=ROOT_DIR,
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Kafka topics for ingestion.")
    parser.add_argument("--bootstrap-server", default="kafka:29092")
    parser.add_argument("--config", default=str(TOPICS_CONFIG_PATH))
    args = parser.parse_args()

    create_topics(args.bootstrap_server, Path(args.config).resolve())
    list_topics(args.bootstrap_server)


if __name__ == "__main__":
    main()
