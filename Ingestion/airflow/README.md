# Airflow

This folder contains Airflow DAGs for ingestion orchestration.

Current DAGs:

- `historical_ingestion_dag`: daily batch ingestion with configurable `start_date` and `end_date`
- `streaming_trigger_dag`: scheduled trigger for one streaming polling cycle via `--run-once`

Main DAG file:

- `ingestion_dag.py`
