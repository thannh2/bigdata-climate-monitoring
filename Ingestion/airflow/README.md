# Airflow

This folder contains the Airflow DAGs used to orchestrate ingestion runs.

## DAGs

- `streaming_trigger_dag`: one production streaming polling cycle for weather and air quality

## Main file

- `ingestion_dag.py`

## Local run requirements

- start Kafka first so the Docker network `ingestion_kafka_network` exists
- use the repository root compose file `docker-compose.airflow.yml`
- provide `AQICN_API_KEY` for air-quality ingestion
- provide `OWM_API_KEY` and `IQAIR_API_KEY` to enable weather and air-quality fallback sources

## Local UI

- Airflow UI: `http://localhost:8088`
- default login: `admin / admin`
