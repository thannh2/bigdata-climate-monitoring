from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

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
from utils.env import load_env_file
from utils.logger import get_logger, log_event
from utils.locations import IQAIR_LOCATIONS, display_location, filter_locations, location_names
from utils.metadata import build_cycle_id, enrich_ingestion_metadata
from utils.retry import retry_call
from utils.runtime_config import ENV_PATH, build_checkpoint_path
from utils.serialization import serialize_record
from validators.air_validator import validate_air_record
from validators.normalized_schema import normalize_air_quality


LOGGER = get_logger("iqair_air_stream_collector")
AIR_STREAM_TOPIC = "air_quality.raw.stream"
AIR_DLQ_TOPIC = "air_quality.raw.dlq"
CHECKPOINT_PATH = build_checkpoint_path("air_stream_iqair_checkpoint.json")
IQAIR_CITY_URL = "https://api.airvisual.com/v2/city"
PIPELINE_NAME = "air_stream_ingestion_iqair"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream ingest latest air quality data from IQAir to Kafka.")
    parser.add_argument("--topic", default=AIR_STREAM_TOPIC)
    parser.add_argument("--dlq-topic", default=AIR_DLQ_TOPIC)
    parser.add_argument("--poll-seconds", type=float, default=300)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument(
        "--locations",
        nargs="*",
        default=location_names(IQAIR_LOCATIONS),
    )
    parser.add_argument("--checkpoint-file", default=str(CHECKPOINT_PATH))
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--run-once", action="store_true")
    return parser.parse_args()


def resolve_api_key(cli_api_key: str | None) -> str:
    load_env_file(ENV_PATH)
    api_key = cli_api_key or os.getenv("IQAIR_API_KEY")
    if not api_key:
        raise ValueError("IQAIR_API_KEY is required. Put it in Ingestion/config/.env or pass --api-key.")
    return api_key


def fetch_iqair_current(location: dict[str, Any], api_key: str) -> dict[str, Any]:
    params = {
        "city": location["city"],
        "state": location["state"],
        "country": location["country"],
        "key": api_key,
    }

    def make_request() -> dict[str, Any]:
        response = requests.get(IQAIR_CITY_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        status = payload.get("status")
        if status != "success":
            raise RuntimeError(f"IQAir returned status={status}: {payload.get('data')}")
        return payload

    payload = retry_call(make_request, retries=3, base_delay_seconds=1.5, retryable_exceptions=(requests.RequestException, RuntimeError))
    data = payload.get("data") or {}
    coordinates = ((data.get("location") or {}).get("coordinates") or [None, None])
    longitude = coordinates[0] if len(coordinates) >= 1 else None
    latitude = coordinates[1] if len(coordinates) >= 2 else None
    current = data.get("current") or {}
    station_id = f"iqair_{display_location(location).lower().replace(' ', '_')}"
    return {
        "current": current,
        "city": display_location(location),
        "latitude": latitude,
        "longitude": longitude,
        "station_id": station_id,
        "data": data,
    }


def main() -> None:
    args = parse_args()
    locations = filter_locations(IQAIR_LOCATIONS, args.locations)
    if not locations:
        raise ValueError("No matching locations were selected")

    api_key = resolve_api_key(args.api_key)
    checkpoint_path = Path(args.checkpoint_file)
    producer = build_producer()

    log_event(
        LOGGER,
        "iqair_air_stream_started",
        topic=args.topic,
        locations=[display_location(location) for location in locations],
        checkpoint_file=str(checkpoint_path),
        started_at=datetime.now(UTC).isoformat(),
    )

    try:
        while True:
            checkpoint = load_checkpoint(checkpoint_path)
            produced_count = 0
            skipped_count = 0
            failed_count = 0
            cycle_id = build_cycle_id("air_stream_iqair")

            for location in locations:
                city_name = display_location(location)
                try:
                    payload = fetch_iqair_current(location, api_key)
                    normalized = normalize_air_quality(payload, source="iqair").dict()
                    serialized = serialize_record(normalized)
                    validation_errors = validate_air_record(normalized)
                    if validation_errors:
                        failed_count += 1
                        log_event(
                            LOGGER,
                            "iqair_air_validation_failed",
                            city=city_name,
                            errors=validation_errors,
                        )
                        _send_dlq_event(
                            producer=producer,
                            dlq_topic=args.dlq_topic,
                            error_type="validation_failed",
                            error_message="; ".join(validation_errors),
                            raw_payload=payload,
                            normalized_payload=serialized,
                            city=city_name,
                            event_time=serialized.get("event_time"),
                            topic=args.topic,
                        )
                        continue

                    event_time = serialized["event_time"]
                    if not _is_new_record(checkpoint, city_name, event_time):
                        skipped_count += 1
                        log_event(
                            LOGGER,
                            "iqair_air_skipped_duplicate",
                            city=city_name,
                            event_time=event_time,
                        )
                        _send_dlq_event(
                            producer=producer,
                            dlq_topic=args.dlq_topic,
                            error_type="duplicate_record",
                            error_message="Duplicate IQAir air record skipped",
                            raw_payload=payload,
                            normalized_payload=serialized,
                            city=city_name,
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
                            source_endpoint=IQAIR_CITY_URL,
                        ),
                    )
                    update_location_checkpoint(
                        checkpoint,
                        city=city_name,
                        event_time=event_time,
                        metadata={"topic": args.topic, "offset": metadata["offset"], "source": "iqair"},
                    )
                    save_checkpoint(checkpoint_path, checkpoint)
                    produced_count += 1
                    log_event(
                        LOGGER,
                        "iqair_air_record_produced",
                        city=serialized["city"],
                        event_time=event_time,
                        partition=metadata["partition"],
                        offset=metadata["offset"],
                    )
                except Exception as exc:
                    failed_count += 1
                    log_event(
                        LOGGER,
                        "iqair_air_record_failed",
                        city=city_name,
                        error=str(exc),
                    )
                    _send_dlq_event(
                        producer=producer,
                        dlq_topic=args.dlq_topic,
                        error_type="record_failed",
                        error_message=str(exc),
                        raw_payload={"location": location},
                        city=city_name,
                        topic=args.topic,
                    )
                time.sleep(args.sleep_seconds)

            log_event(
                LOGGER,
                "iqair_air_cycle_completed",
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
                pipeline_name=PIPELINE_NAME,
                batch_id=build_cycle_id("air_stream_iqair_dlq"),
                entity="air_quality",
                source="iqair",
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
        log_event(
            LOGGER,
            "iqair_air_dlq_failed",
            city=city,
            error=str(exc),
            original_error=error_message,
        )


if __name__ == "__main__":
    main()
