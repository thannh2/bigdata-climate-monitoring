# Ingestion Requirements Specification

## Project

Climate monitoring data pipeline using a Lambda-style ingestion architecture.

## Objective

The ingestion layer is responsible for:

- collecting weather and air-quality data in batch and streaming modes
- normalizing raw payloads into shared schemas
- publishing records to Kafka
- supporting downstream Spark, lakehouse, dashboard, and ML consumers

## Use cases

### Real-time weather monitoring

- Mode: streaming
- Priority: high
- Consumers: dashboards, alerts, Spark Streaming

### Historical backfill

- Mode: batch
- Priority: medium
- Consumers: lakehouse, model training, trend analysis

### Air-quality analysis

- Mode: batch and streaming
- Priority: high
- Consumers: AQI dashboard, health alerts, lakehouse

## MVP geography

| Region | City | Latitude | Longitude |
|---|---|---:|---:|
| bac | Hanoi | 21.0285 | 105.8542 |
| bac | Hai Phong | 20.8449 | 106.6881 |
| trung | Da Nang | 16.0544 | 108.2022 |
| trung | Hue | 16.4637 | 107.5909 |
| nam | HCMC | 10.7769 | 106.6964 |
| nam | Can Tho | 10.0379 | 105.7869 |

## Partitioning

- `region_bac`
- `region_trung`
- `region_nam`

Target: one Kafka partition per region for ordering and parallelism.

## Time scope

### Batch

- historical window: last 30 days
- schedule: daily at 02:00 UTC+7

### Streaming

- default frequency: every 5 minutes
- fallback frequency: every 10 to 15 minutes when rate limited
- target latency: under 10 minutes from source response to Kafka

## Official sources

### Weather

- Open-Meteo
- OpenWeatherMap

### Air quality

- AQICN
- IQAir
- Open-Meteo
- OpenWeatherMap air pollution endpoint

## Fallback policy

- Open-Meteo -> OpenWeatherMap
- AQICN -> IQAir
- if both fail, use short-lived cache when available
- raise alert when data freshness exceeds 15 minutes

## Kafka topics

### Weather

- `weather.raw.batch`
- `weather.raw.stream`
- `weather.raw.dlq`

### Air quality

- `air_quality.raw.batch`
- `air_quality.raw.stream`
- `air_quality.raw.dlq`

### Topic defaults

- partitions: 3
- replication factor: 1
- retention: 7 days
- compression: `lz4`

## Required metadata

All messages must include:

- `event_id`
- `source`
- `region`
- `city`
- `station_id`
- `latitude`
- `longitude`
- `event_time`
- `ingestion_time`
- `data_version`

## Batch ingestion requirements

- support date-range backfill
- retry temporary failures up to 3 times
- produce to `*.raw.batch`
- log success, failure, and record counts

## Streaming ingestion requirements

- poll latest source data on schedule
- avoid duplicates using `event_time` or a stable record key
- produce to `*.raw.stream`

## Validation requirements

### Weather

- `temperature_c` in a reasonable operating range
- `humidity` between 0 and 100
- `wind_speed_mps >= 0`
- `pressure_hpa > 0`

### Air quality

- `aqi >= 0`
- `pm25 >= 0`
- `pm10 >= 0`
- pollutant numeric fields parse correctly

## Error handling

- retry source timeouts, network failures, and 5xx responses
- route invalid records to DLQ
- log `error_type`, `error_message`, `raw_payload`, and `failed_at`

## Orchestration

Airflow should support:

- daily batch ingestion
- backfill by date range
- retry policy
- run logging and status tracking

## Acceptance criteria

- batch ingestion can backfill the defined date range
- streaming ingestion refreshes all MVP locations
- partition routing uses region keys consistently
- schema validation runs before publish
- DLQ handles malformed records
- freshness remains within target thresholds

## Deliverables

- requirements specification
- source mapping document
- Kafka topic design
- normalized schemas
- batch collectors
- streaming collectors
- DLQ handling
- logging notes
- Airflow DAGs
- operational README
