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

from collectors.openweathermap_weather_stream_collector import (
    OPENWEATHERMAP_WEATHER_URL,
    fetch_openweathermap_weather,
    resolve_api_key as resolve_owm_api_key,
)
from producers.dlq_producer import build_dlq_message, send_to_dlq
from producers.kafka_producer import build_producer, produce_json_message
from utils.checkpoint import (
    get_location_checkpoint,
    load_checkpoint,
    save_checkpoint,
    update_location_checkpoint,
)
from utils.logger import get_logger, log_event
from utils.locations import OPEN_METEO_LOCATIONS, filter_locations, location_names
from utils.metadata import build_cycle_id, enrich_ingestion_metadata
from utils.retry import retry_call
from utils.runtime_config import build_checkpoint_path
from utils.serialization import serialize_record
from validators.normalized_schema import normalize_weather
from validators.weather_validator import validate_weather_record


LOGGER = get_logger("weather_stream_collector")
WEATHER_STREAM_TOPIC = "weather.raw.stream"
WEATHER_DLQ_TOPIC = "weather.raw.dlq"
CHECKPOINT_PATH = build_checkpoint_path("weather_stream_checkpoint.json")
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
PIPELINE_NAME = "weather_stream_ingestion"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream ingest latest weather data to Kafka.")
    parser.add_argument("--topic", default=WEATHER_STREAM_TOPIC)
    parser.add_argument("--dlq-topic", default=WEATHER_DLQ_TOPIC)
    parser.add_argument("--poll-seconds", type=float, default=300)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--locations", nargs="*", default=location_names(OPEN_METEO_LOCATIONS))
    parser.add_argument("--checkpoint-file", default=str(CHECKPOINT_PATH))
    parser.add_argument("--run-once", action="store_true")
    return parser.parse_args()


def fetch_weather_current(location: dict[str, Any]) -> dict[str, Any]:
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "current": "temperature_2m,relative_humidity_2m,pressure_msl,wind_speed_10m,weather_code",
        "timezone": "Asia/Bangkok",
    }

    def make_request() -> dict[str, Any]:
        response = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    payload = retry_call(make_request, retries=3, base_delay_seconds=1.5, retryable_exceptions=(requests.RequestException,))
    payload["city"] = location["city"]
    return payload


def _resolve_owm_fallback_api_key() -> str | None:
    try:
        return resolve_owm_api_key(None)
    except Exception:
        return None


def _fetch_primary_or_fallback(
    location: dict[str, Any],
    fallback_api_key: str | None,
) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    errors: list[str] = []

    try:
        payload = fetch_weather_current(location)
        normalized = normalize_weather(payload, source="open-meteo").dict()
        serialized = serialize_record(normalized)
        validation_errors = validate_weather_record(normalized)
        if validation_errors:
            raise ValueError("; ".join(validation_errors))
        return payload, serialized, "open-meteo", OPEN_METEO_FORECAST_URL
    except Exception as exc:
        errors.append(f"open-meteo: {exc}")

    if not fallback_api_key:
        raise RuntimeError(" | ".join(errors))

    try:
        payload = fetch_openweathermap_weather(location, fallback_api_key)
        normalized = normalize_weather(payload, source="openweathermap").dict()
        serialized = serialize_record(normalized)
        validation_errors = validate_weather_record(normalized)
        if validation_errors:
            raise ValueError("; ".join(validation_errors))
        log_event(
            LOGGER,
            "weather_stream_fallback_used",
            city=location["city"],
            fallback_source="openweathermap",
            primary_source="open-meteo",
        )
        return payload, serialized, "openweathermap", OPENWEATHERMAP_WEATHER_URL
    except Exception as exc:
        errors.append(f"openweathermap: {exc}")
        raise RuntimeError(" | ".join(errors))


def main() -> None:
    args = parse_args()
    locations = filter_locations(OPEN_METEO_LOCATIONS, args.locations)
    if not locations:
        raise ValueError("No matching locations were selected")

    checkpoint_path = Path(args.checkpoint_file)
    fallback_api_key = _resolve_owm_fallback_api_key()
    producer = build_producer()

    log_event(
        LOGGER,
        "weather_stream_started",
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
            cycle_id = build_cycle_id("weather_stream")

            for location in locations:
                try:
                    payload, serialized, source_name, source_endpoint = _fetch_primary_or_fallback(
                        location,
                        fallback_api_key=fallback_api_key,
                    )

                    event_time = serialized["event_time"]
                    if not _is_new_record(checkpoint, location["city"], event_time):
                        skipped_count += 1
                        log_event(
                            LOGGER,
                            "weather_stream_skipped_duplicate",
                            city=location["city"],
                            event_time=event_time,
                        )
                        _send_dlq_event(
                            producer,
                            dlq_topic=args.dlq_topic,
                            error_type="duplicate_record",
                            error_message="Duplicate weather stream record skipped",
                            raw_payload=payload,
                            normalized_payload=serialized,
                            city=location["city"],
                            event_time=event_time,
                            topic=args.topic,
                            source=source_name,
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
                            source_endpoint=source_endpoint,
                        ),
                    )
                    update_location_checkpoint(
                        checkpoint,
                        city=location["city"],
                        event_time=event_time,
                        metadata={"topic": args.topic, "offset": metadata["offset"], "source": source_name},
                    )
                    save_checkpoint(checkpoint_path, checkpoint)
                    produced_count += 1
                    log_event(
                        LOGGER,
                        "weather_stream_record_produced",
                        city=serialized["city"],
                        event_time=event_time,
                        source=source_name,
                        partition=metadata["partition"],
                        offset=metadata["offset"],
                    )
                except Exception as exc:
                    failed_count += 1
                    log_event(LOGGER, "weather_stream_record_failed", city=location["city"], error=str(exc))
                    _send_dlq_event(
                        producer,
                        dlq_topic=args.dlq_topic,
                        error_type="record_failed",
                        error_message=str(exc),
                        raw_payload={"location": location},
                        city=location["city"],
                        topic=args.topic,
                        source="open-meteo",
                    )
                time.sleep(args.sleep_seconds)

            log_event(
                LOGGER,
                "weather_stream_cycle_completed",
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
    source: str,
    normalized_payload: dict[str, Any] | None = None,
    event_time: str | None = None,
) -> None:
    try:
        send_to_dlq(
            producer=producer,
            dlq_topic=dlq_topic,
            key=f"city_{city.lower().replace(' ', '_')}" if city else None,
            message=build_dlq_message(
                pipeline_name="weather_stream_ingestion",
                batch_id=build_cycle_id("weather_stream_dlq"),
                entity="weather",
                source=source,
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
        log_event(LOGGER, "weather_stream_dlq_failed", city=city, error=str(exc), original_error=error_message)


if __name__ == "__main__":
    main()
