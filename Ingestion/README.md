# Climate Monitoring Ingestion

This module is the ingestion boundary of the climate monitoring pipeline. It fetches source data, converts it into a shared schema, applies basic quality checks, and publishes records to Kafka.

## Architecture

```text
External APIs
   ->
Collectors (batch + stream)
   ->
Normalize + Validate
   ->
Kafka raw topics / DLQ
   ->
Downstream consumers
```

The main stream pipelines currently use these fallback chains:

- weather: `Open-Meteo -> OpenWeatherMap`
- air quality: `AQICN -> IQAir`

## Delivered scope

- Kafka local stack via Docker Compose
- topic creation for raw and DLQ topics
- historical batch collectors for Open-Meteo weather and air quality
- streaming collectors for Open-Meteo, OpenWeatherMap, AQICN, and IQAir
- source fallback in the main stream collectors
- shared retry, logging, metadata, checkpoint, and serialization helpers
- DLQ routing for fetch, validation, processing, and duplicate failures
- Airflow orchestration for batch and stream trigger flows
- GE-aligned input quality rules for downstream validation
- Kafka export script for building demo datasets from topics

## Source of truth

- runtime entrypoints: `Ingestion/collectors/`, `Ingestion/scripts/`, `Ingestion/airflow/`
- shared configuration and path helpers: `Ingestion/utils/runtime_config.py`
- shared location catalog: `Ingestion/utils/locations.py`
- normalized schema: `Ingestion/validators/normalized_schema.py`
- basic validation rules: `Ingestion/validators/`

## Folder layout

```text
Ingestion/
|-- airflow/        # Airflow DAGs and orchestration notes
|-- collectors/     # Runtime ingestion entrypoints
|-- config/         # Kafka and local environment configuration
|-- great_expectations/
|-- producers/      # Kafka and DLQ producer helpers
|-- samples/        # Minimal normalized sample records
|-- scripts/        # Topic setup, smoke tests, export, and validation helpers
|-- tests/          # Placeholder for automated tests
|-- utils/          # Shared runtime helpers and source catalogs
|-- validators/     # Normalization and basic validation logic
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

Export Kafka data to a local JSONL file:

```powershell
.\.venv\Scripts\python.exe Ingestion\scripts\export_kafka_to_file.py --topics weather.raw.batch air_quality.raw.batch --from-beginning --max-messages 500 --include-kafka-meta
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
.\.venv\Scripts\python.exe Ingestion\collectors\aqicn_air_stream_collector.py --run-once --locations Hanoi
```

## Validation model

- ingestion validators perform blocking checks before Kafka publish
- Great Expectations is prepared as a downstream quality layer
- shared thresholds live in `Ingestion/utils/quality_rules.py`

## Handover notes

- Keep real API keys only in `Ingestion/config/.env`.
- Airflow DAG definitions live in `Ingestion/airflow/ingestion_dag.py`.
- The current local environment is Python 3.14, so the dependency set is intentionally minimal and Great Expectations remains at suite-and-rules level rather than installed locally.
