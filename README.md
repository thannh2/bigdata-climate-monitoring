# Big Data Climate Monitoring

This repository currently contains the ingestion module for a climate monitoring pipeline.

## Repository layout

- [Ingestion](Ingestion/README.md): Kafka-based ingestion for weather and air-quality data

## Ingestion status

Implemented in the ingestion module:

- Kafka stack and topic setup
- batch and stream collectors
- normalization and validation
- DLQ handling
- checkpoint-based duplicate prevention
- metadata enrichment
- Airflow DAG scaffolding
- downstream-ready quality rules for Great Expectations

## Entry point

Start with [Ingestion/README.md](Ingestion/README.md).
