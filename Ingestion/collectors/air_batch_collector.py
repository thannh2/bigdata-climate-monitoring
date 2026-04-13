from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import requests


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from producers.dlq_producer import build_dlq_message, send_to_dlq
from producers.kafka_producer import build_producer, produce_json_message
from utils.logger import get_logger, log_event
from utils.locations import OPEN_METEO_LOCATIONS, filter_locations, location_names
from utils.metadata import build_batch_id, enrich_ingestion_metadata
from utils.retry import retry_call
from utils.serialization import serialize_record
from validators.air_validator import validate_air_record
from validators.normalized_schema import normalize_air_quality


LOGGER = get_logger("air_batch_collector")
AIR_BATCH_TOPIC = "air_quality.raw.batch"
AIR_DLQ_TOPIC = "air_quality.raw.dlq"
OPEN_METEO_AIR_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
PIPELINE_NAME = "air_batch_ingestion"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch ingest historical air quality data to Kafka.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--topic", default=AIR_BATCH_TOPIC)
    parser.add_argument("--dlq-topic", default=AIR_DLQ_TOPIC)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--locations", nargs="*", default=location_names(OPEN_METEO_LOCATIONS))
    return parser.parse_args()


def fetch_air_history(location: dict[str, Any], start_date: str, end_date: str) -> dict[str, Any]:
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,us_aqi",
        "timezone": "Asia/Bangkok",
    }

    def make_request() -> dict[str, Any]:
        response = requests.get(OPEN_METEO_AIR_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    return retry_call(make_request, retries=3, base_delay_seconds=1.5, retryable_exceptions=(requests.RequestException,))


def iter_hourly_air_records(payload: dict[str, Any], city: str) -> list[dict[str, Any]]:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    records: list[dict[str, Any]] = []
    for index, event_time in enumerate(times):
        records.append(
            {
                "source": "open-meteo",
                "city": city,
                "latitude": payload.get("latitude"),
                "longitude": payload.get("longitude"),
                "current": {
                    "time": event_time,
                    "pm10": _safe_array_value(hourly.get("pm10"), index),
                    "pm2_5": _safe_array_value(hourly.get("pm2_5"), index),
                    "us_aqi": _safe_array_value(hourly.get("us_aqi"), index),
                    "co": _safe_array_value(hourly.get("carbon_monoxide"), index),
                    "no2": _safe_array_value(hourly.get("nitrogen_dioxide"), index),
                    "so2": _safe_array_value(hourly.get("sulphur_dioxide"), index),
                    "o3": _safe_array_value(hourly.get("ozone"), index),
                },
            }
        )
    return records


def _safe_array_value(values: Any, index: int) -> Any:
    if not isinstance(values, list) or index >= len(values):
        return None
    return values[index]


def main() -> None:
    args = parse_args()
    _validate_date_range(args.start_date, args.end_date)
    locations = filter_locations(OPEN_METEO_LOCATIONS, args.locations)
    if not locations:
        raise ValueError("No matching locations were selected")

    producer = build_producer()
    seen_keys: set[tuple[str, str, str, str]] = set()
    success_count = 0
    failed_count = 0
    skipped_duplicates = 0
    batch_id = build_batch_id("air_batch", args.start_date, args.end_date)

    log_event(
        LOGGER,
        "air_batch_started",
        topic=args.topic,
        start_date=args.start_date,
        end_date=args.end_date,
        locations=[location["city"] for location in locations],
        batch_id=batch_id,
        started_at=datetime.now(UTC).isoformat(),
    )

    try:
        for location in locations:
            try:
                payload = fetch_air_history(location, args.start_date, args.end_date)
            except Exception as exc:
                failed_count += 1
                log_event(LOGGER, "air_fetch_failed", city=location["city"], error=str(exc))
                _send_dlq_event(
                    producer,
                    dlq_topic=args.dlq_topic,
                    batch_id=batch_id,
                    error_type="fetch_failed",
                    error_message=str(exc),
                    raw_payload={"location": location, "start_date": args.start_date, "end_date": args.end_date},
                    city=location["city"],
                    topic=args.topic,
                )
                continue

            for raw_record in iter_hourly_air_records(payload, city=location["city"]):
                try:
                    normalized = normalize_air_quality(raw_record, source="open-meteo").dict()
                    dedup_key = (
                        normalized["source"],
                        normalized["city"],
                        normalized["station_id"],
                        normalized["event_time"].isoformat(),
                    )
                    if dedup_key in seen_keys:
                        skipped_duplicates += 1
                        _send_dlq_event(
                            producer,
                            dlq_topic=args.dlq_topic,
                            batch_id=batch_id,
                            error_type="duplicate_record",
                            error_message="Duplicate air-quality record skipped",
                            raw_payload=raw_record,
                            normalized_payload=serialize_record(normalized),
                            city=location["city"],
                            event_time=normalized["event_time"].isoformat(),
                            topic=args.topic,
                            extra={"dedup_key": list(dedup_key)},
                        )
                        continue

                    validation_errors = validate_air_record(normalized)
                    if validation_errors:
                        failed_count += 1
                        log_event(
                            LOGGER,
                            "air_validation_failed",
                            city=location["city"],
                            event_time=normalized.get("event_time"),
                            errors=validation_errors,
                        )
                        _send_dlq_event(
                            producer,
                            dlq_topic=args.dlq_topic,
                            batch_id=batch_id,
                            error_type="validation_failed",
                            error_message="; ".join(validation_errors),
                            raw_payload=raw_record,
                            normalized_payload=serialize_record(normalized),
                            city=location["city"],
                            event_time=normalized["event_time"].isoformat(),
                            topic=args.topic,
                        )
                        continue

                    metadata = produce_json_message(
                        producer,
                        topic=args.topic,
                        key=f"region_{normalized['region']}",
                        message=enrich_ingestion_metadata(
                            serialize_record(normalized),
                            pipeline_name=PIPELINE_NAME,
                            batch_id=batch_id,
                            source_endpoint=OPEN_METEO_AIR_URL,
                        ),
                    )
                    seen_keys.add(dedup_key)
                    success_count += 1
                    log_event(
                        LOGGER,
                        "air_record_produced",
                        city=normalized["city"],
                        event_time=normalized["event_time"],
                        partition=metadata["partition"],
                        offset=metadata["offset"],
                    )
                except Exception as exc:
                    failed_count += 1
                    log_event(LOGGER, "air_record_failed", city=location["city"], error=str(exc))
                    _send_dlq_event(
                        producer,
                        dlq_topic=args.dlq_topic,
                        batch_id=batch_id,
                        error_type="record_failed",
                        error_message=str(exc),
                        raw_payload=raw_record,
                        city=location["city"],
                        topic=args.topic,
                    )
            time.sleep(args.sleep_seconds)
    finally:
        producer.flush()

    log_event(
        LOGGER,
        "air_batch_completed",
        topic=args.topic,
        start_date=args.start_date,
        end_date=args.end_date,
        success_count=success_count,
        failed_count=failed_count,
        skipped_duplicates=skipped_duplicates,
        finished_at=datetime.now(UTC).isoformat(),
    )


def _validate_date_range(start_date_text: str, end_date_text: str) -> None:
    start = date.fromisoformat(start_date_text)
    end = date.fromisoformat(end_date_text)
    if start > end:
        raise ValueError("start_date must be <= end_date")


def _send_dlq_event(
    producer: Any,
    dlq_topic: str,
    batch_id: str,
    error_type: str,
    error_message: str,
    raw_payload: dict[str, Any] | None,
    city: str | None,
    topic: str,
    normalized_payload: dict[str, Any] | None = None,
    event_time: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    try:
        send_to_dlq(
            producer=producer,
            dlq_topic=dlq_topic,
            key=f"city_{city.lower().replace(' ', '_')}" if city else None,
            message=build_dlq_message(
                pipeline_name=PIPELINE_NAME,
                batch_id=batch_id,
                entity="air_quality",
                source="open-meteo",
                city=city,
                event_time=event_time,
                topic=topic,
                error_type=error_type,
                error_message=error_message,
                raw_payload=raw_payload,
                normalized_payload=normalized_payload,
                extra=extra,
            ),
        )
    except Exception as exc:
        log_event(LOGGER, "air_dlq_failed", city=city, error=str(exc), original_error=error_message)


if __name__ == "__main__":
    main()
