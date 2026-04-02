# Climate Monitoring Ingestion

This module implements the ingestion layer for the climate monitoring pipeline.

## Current scope

- collect weather and air-quality data
- normalize source payloads into shared records
- validate records before publish
- publish batch and stream records to Kafka
- route failures and duplicates to DLQ

## Implemented

- Kafka local stack via Docker Compose
- topic creation for raw and DLQ topics
- smoke test for Kafka producer and consumer
- weather and air batch collectors
- weather and air stream collectors with checkpoint-based deduplication
- structured logging, retry, serialization, and metadata helpers
- DLQ publishing for fetch, validation, processing, and duplicate failures
- Airflow DAG scaffolding for batch scheduling and stream trigger cycles
- GE-aligned input quality rules for downstream validation

## Folder layout

```text
Ingestion/
|-- airflow/        # DAG orchestration
|-- collectors/     # Batch and stream collectors
|-- config/         # Kafka and environment configuration
|-- docs/           # Requirements and design notes
|-- great_expectations/
|-- producers/      # Kafka and DLQ producer helpers
|-- samples/        # Minimal normalized examples
|-- scripts/        # Setup, smoke test, source test, quality validation
|-- tests/          # Placeholder for automated tests
|-- utils/          # Shared helpers
|-- validators/     # Normalization and validation logic
|-- docker-compose.yml
`-- requirements.txt
```

## Kafka topics

- `weather.raw.batch`
- `weather.raw.stream`
- `weather.raw.dlq`
- `air_quality.raw.batch`
- `air_quality.raw.stream`
- `air_quality.raw.dlq`

## Main commands

Install dependencies:

```powershell
.\.venv\Scripts\python.exe -m pip install -r Ingestion\requirements.txt
```

The current `requirements.txt` contains only the runtime packages actually used by the ingestion module, so it is safer to install on newer Python versions.

Start Kafka:

```powershell
docker compose -f Ingestion\docker-compose.yml up -d
```

Create topics:

```powershell
.\.venv\Scripts\python.exe Ingestion\scripts\create_kafka_topics.py
```

Run Kafka smoke test:

```powershell
.\.venv\Scripts\python.exe Ingestion\scripts\smoke_test_kafka.py --topic weather.raw.stream
```

Run weather batch ingestion:

```powershell
.\.venv\Scripts\python.exe Ingestion\collectors\weather_batch_collector.py --start-date 2026-04-01 --end-date 2026-04-01 --locations Hanoi
```

Run air batch ingestion:

```powershell
.\.venv\Scripts\python.exe Ingestion\collectors\air_batch_collector.py --start-date 2026-04-01 --end-date 2026-04-01 --locations Hanoi
```

Run one weather stream cycle:

```powershell
.\.venv\Scripts\python.exe Ingestion\collectors\weather_stream_collector.py --run-once --locations Hanoi
```

Run one air stream cycle:

```powershell
.\.venv\Scripts\python.exe Ingestion\collectors\air_stream_collector.py --run-once --locations Hanoi
```

Validate normalized samples against GE-aligned rules:

```powershell
.\.venv\Scripts\python.exe Ingestion\scripts\validate_input_quality.py --entity weather --input Ingestion\samples\weather_normalized_example.json
```

## Validation model

- ingestion validators perform blocking checks before Kafka publish
- Great Expectations is prepared as a downstream quality layer
- shared thresholds live in `Ingestion/utils/quality_rules.py`

## Notes

- Keep real API keys only in `Ingestion/config/.env`.
- Check `Ingestion/docs/README.md` for requirements and design documents.
- Airflow DAG definitions live in `Ingestion/airflow/ingestion_dag.py`.
- The current local environment is Python 3.14, so the dependency set is kept minimal and Great Expectations remains at suite-and-rules level rather than installed locally.
