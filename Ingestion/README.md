# Climate Monitoring Ingestion

This module is the streaming ingestion boundary of the climate monitoring pipeline. It fetches live data from real external APIs, normalizes records into the shared schema, validates them, and publishes accepted records to Kafka. Failed fetch, validation, and processing events go to DLQ topics.

## Architecture

```text
External APIs
   ->
Streaming collectors
   ->
Normalize + Validate
   ->
Kafka raw stream topics / DLQ topics
   ->
Downstream consumers
```

The production stream entrypoints use these fallback chains:

- weather: `Open-Meteo -> OpenWeatherMap`
- air quality: `AQICN -> IQAir`

Duplicate polling results are skipped and logged. They are not sent to DLQ because an unchanged `current` API response is expected in a short polling window.

## Delivered scope

- Kafka local stack via Docker Compose
- topic creation for stream and DLQ topics
- streaming collectors for Open-Meteo, OpenWeatherMap, AQICN, and IQAir
- source fallback in the production stream collectors
- shared retry, logging, metadata, checkpoint, and serialization helpers
- DLQ routing for fetch, validation, and processing failures
- Airflow orchestration for production streaming trigger flows
- GE-aligned input quality rules for downstream validation
- Kafka export script for exporting real topic data when needed

## Source of truth

- production stream entrypoints:
  - `Ingestion/collectors/weather_stream_collector.py`
  - `Ingestion/collectors/aqicn_air_stream_collector.py`
- Airflow DAG: `Ingestion/airflow/ingestion_dag.py`
- shared configuration and path helpers: `Ingestion/utils/runtime_config.py`
- shared location catalog: `Ingestion/utils/locations.py`
- normalized schema: `Ingestion/validators/normalized_schema.py`
- validation rules: `Ingestion/validators/`

## Folder layout

```text
Ingestion/
|-- airflow/        # Airflow DAGs and orchestration notes
|-- collectors/     # Streaming ingestion entrypoints and source clients
|-- config/         # Kafka and local environment configuration
|-- great_expectations/
|-- producers/      # Kafka and DLQ producer helpers
|-- scripts/        # Topic setup, export, and validation helpers
|-- utils/          # Shared runtime helpers and source catalogs
|-- validators/     # Normalization and validation logic
|-- docker-compose.yml
`-- requirements.txt
```

## Kafka topics

- `weather.raw.stream`
- `weather.raw.dlq`
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

Run one weather stream cycle:

```powershell
.\.venv\Scripts\python.exe Ingestion\collectors\weather_stream_collector.py --run-once --locations Hanoi
```

Run one air-quality stream cycle:

```powershell
.\.venv\Scripts\python.exe Ingestion\collectors\aqicn_air_stream_collector.py --run-once --locations Hanoi
```

Export real Kafka data to a local JSONL file:

```powershell
.\.venv\Scripts\python.exe Ingestion\scripts\export_kafka_to_file.py --topics weather.raw.stream air_quality.raw.stream --from-beginning --max-messages 500 --include-kafka-meta
```

## Required API keys

- `OWM_API_KEY` enables OpenWeatherMap weather fallback.
- `AQICN_API_KEY` is required for the production air-quality stream.
- `IQAIR_API_KEY` enables IQAir air-quality fallback.

Put local keys in `Ingestion/config/.env` or pass supported `--api-key` options on source-specific collectors.

## Validation model

- ingestion validators perform blocking checks before Kafka publish
- Great Expectations-compatible rules are prepared as a downstream quality layer
- shared thresholds live in `Ingestion/utils/quality_rules.py`

## Handover notes

- No batch ingestion is part of this handover.
- No fake data, sample payloads, or smoke-test producers are included.
- Keep real API keys only in `Ingestion/config/.env`.
- Airflow DAG definitions live in `Ingestion/airflow/ingestion_dag.py`.
