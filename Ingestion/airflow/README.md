# Airflow

This folder contains the Airflow DAGs used to orchestrate ingestion runs.

## DAGs

- `historical_ingestion_dag`: batch ingestion with `start_date` and `end_date`
- `streaming_trigger_dag`: one streaming polling cycle across configured sources

## Main file

- `ingestion_dag.py`

## Local run requirements

- start Kafka first so the Docker network `ingestion_kafka_network` exists
- use the repository root compose file `docker-compose.airflow.yml`
- provide `OWM_API_KEY`, `AQICN_API_KEY`, and `IQAIR_API_KEY` if you want all multi-source stream tasks to run successfully

## Local UI

- Airflow UI: `http://localhost:8088`
- default login: `admin / admin`
