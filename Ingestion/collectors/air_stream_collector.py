from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from producers.dlq_producer import build_dlq_message, send_to_dlq
from producers.kafka_producer import build_producer, produce_json_message
from utils.checkpoint import (
    get_location_checkpoint,
    load_checkpoint,
    save_checkpoint,
    update_location_checkpoint,
)
from utils.logger import get_logger, log_event
from utils.metadata import build_cycle_id, enrich_ingestion_metadata
from utils.retry import retry_call
from utils.serialization import serialize_record
from validators.air_validator import validate_air_record
from validators.normalized_schema import normalize_air_quality


LOGGER = get_logger("air_stream_collector")
AIR_STREAM_TOPIC = "air_quality.raw.stream"
AIR_DLQ_TOPIC = "air_quality.raw.dlq"
CHECKPOINT_PATH = ROOT_DIR.parent / "checkpoint" / "air_stream_checkpoint.json"
OPEN_METEO_AIR_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
PIPELINE_NAME = "air_stream_ingestion"
LOCATION_CATALOG = [
    {"city": "Hanoi", "latitude": 21.0285, "longitude": 105.8542},
    {"city": "Hai Phong", "latitude": 20.8449, "longitude": 106.6881},
    {"city": "Da Nang", "latitude": 16.0544, "longitude": 108.2022},
    {"city": "Hue", "latitude": 16.4637, "longitude": 107.5909},
    {"city": "HCMC", "latitude": 10.7769, "longitude": 106.6964},
    {"city": "Can Tho", "latitude": 10.0379, "longitude": 105.7869},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream ingest latest air quality data to Kafka.")
    parser.add_argument("--topic", default=AIR_STREAM_TOPIC)
    parser.add_argument("--dlq-topic", default=AIR_DLQ_TOPIC)
    parser.add_argument("--poll-seconds", type=float, default=300)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--locations", nargs="*", default=[location["city"] for location in LOCATION_CATALOG])
    parser.add_argument("--checkpoint-file", default=str(CHECKPOINT_PATH))
    parser.add_argument("--run-once", action="store_true")
    return parser.parse_args()


def filter_locations(selected_locations: list[str]) -> list[dict[str, Any]]:
    selected = {name.strip().lower() for name in selected_locations}
    return [location for location in LOCATION_CATALOG if location["city"].lower() in selected]


def fetch_air_current(location: dict[str, Any]) -> dict[str, Any]:
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "current": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,us_aqi",
        "timezone": "Asia/Bangkok",
    }

    def make_request() -> dict[str, Any]:
        response = requests.get(OPEN_METEO_AIR_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    payload = retry_call(make_request, retries=3, base_delay_seconds=1.5, retryable_exceptions=(requests.RequestException,))
    payload["city"] = location["city"]
    return payload


def main() -> None:
    args = parse_args()
    locations = filter_locations(args.locations)
    if not locations:
        raise ValueError("No matching locations were selected")

    checkpoint_path = Path(args.checkpoint_file)
    producer = build_producer()

    log_event(
        LOGGER,
        "air_stream_started",
        topic=args.topic,
        locations=[location["city"] for location in locations],
        checkpoint_file=str(checkpoint_path),
        started_at=datetime.now(UTC).isoformat(),
    )

    try:
        while True:
            checkpoint = load_checkpoint(checkpoint_path)
            produced_count = 0
            skipped_count = 0
            failed_count = 0
            cycle_id = build_cycle_id("air_stream")

            for location in locations:
                try:
                    payload = fetch_air_current(location)
                    normalized = normalize_air_quality(payload, source="open-meteo").dict()
                    serialized = serialize_record(normalized)
                    validation_errors = validate_air_record(normalized)
                    if validation_errors:
                        failed_count += 1
                        log_event(
                            LOGGER,
                            "air_stream_validation_failed",
                            city=location["city"],
                            errors=validation_errors,
                        )
                        _send_dlq_event(
                            producer,
                            dlq_topic=args.dlq_topic,
                            error_type="validation_failed",
                            error_message="; ".join(validation_errors),
                            raw_payload=payload,
                            normalized_payload=serialized,
                            city=location["city"],
                            event_time=serialized.get("event_time"),
                            topic=args.topic,
                        )
                        continue

                    event_time = serialized["event_time"]
                    if not _is_new_record(checkpoint, location["city"], event_time):
                        skipped_count += 1
                        log_event(
                            LOGGER,
                            "air_stream_skipped_duplicate",
                            city=location["city"],
                            event_time=event_time,
                        )
                        _send_dlq_event(
                            producer,
                            dlq_topic=args.dlq_topic,
                            error_type="duplicate_record",
                            error_message="Duplicate air stream record skipped",
                            raw_payload=payload,
                            normalized_payload=serialized,
                            city=location["city"],
                            event_time=event_time,
                            topic=args.topic,
                        )
                        continue

                    metadata = produce_json_message(
                        producer,
                        topic=args.topic,
                        key=f"region_{serialized['region']}",
                        message=enrich_ingestion_metadata(
                            serialized,
                            pipeline_name=PIPELINE_NAME,
                            batch_id=cycle_id,
                            source_endpoint=OPEN_METEO_AIR_URL,
                        ),
                    )
                    update_location_checkpoint(
                        checkpoint,
                        city=location["city"],
                        event_time=event_time,
                        metadata={"topic": args.topic, "offset": metadata["offset"]},
                    )
                    save_checkpoint(checkpoint_path, checkpoint)
                    produced_count += 1
                    log_event(
                        LOGGER,
                        "air_stream_record_produced",
                        city=serialized["city"],
                        event_time=event_time,
                        partition=metadata["partition"],
                        offset=metadata["offset"],
                    )
                except Exception as exc:
                    failed_count += 1
                    log_event(LOGGER, "air_stream_record_failed", city=location["city"], error=str(exc))
                    _send_dlq_event(
                        producer,
                        dlq_topic=args.dlq_topic,
                        error_type="record_failed",
                        error_message=str(exc),
                        raw_payload={"location": location},
                        city=location["city"],
                        topic=args.topic,
                    )
                time.sleep(args.sleep_seconds)

            log_event(
                LOGGER,
                "air_stream_cycle_completed",
                topic=args.topic,
                batch_id=cycle_id,
                produced_count=produced_count,
                skipped_count=skipped_count,
                failed_count=failed_count,
                finished_at=datetime.now(UTC).isoformat(),
            )

            if args.run_once:
                break
            time.sleep(args.poll_seconds)
    finally:
        producer.flush()


def _is_new_record(checkpoint: dict[str, Any], city: str, event_time: str) -> bool:
    saved = get_location_checkpoint(checkpoint, city).get("event_time")
    if not saved:
        return True
    return _parse_iso_datetime(event_time) > _parse_iso_datetime(saved)


def _parse_iso_datetime(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _send_dlq_event(
    producer: Any,
    dlq_topic: str,
    error_type: str,
    error_message: str,
    raw_payload: dict[str, Any] | None,
    city: str | None,
    topic: str,
    normalized_payload: dict[str, Any] | None = None,
    event_time: str | None = None,
) -> None:
    try:
        send_to_dlq(
            producer=producer,
            dlq_topic=dlq_topic,
            key=f"city_{city.lower().replace(' ', '_')}" if city else None,
            message=build_dlq_message(
                pipeline_name="air_stream_ingestion",
                batch_id=build_cycle_id("air_stream_dlq"),
                entity="air_quality",
                source="open-meteo",
                city=city,
                event_time=event_time,
                topic=topic,
                error_type=error_type,
                error_message=error_message,
                raw_payload=raw_payload,
                normalized_payload=normalized_payload,
            ),
        )
    except Exception as exc:
        log_event(LOGGER, "air_stream_dlq_failed", city=city, error=str(exc), original_error=error_message)


if __name__ == "__main__":
    main()
