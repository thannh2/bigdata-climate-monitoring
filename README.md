# Big Data Climate Monitoring

This repository packages the streaming ingestion layer of a climate monitoring pipeline. The implemented module collects live weather and air-quality data from real external APIs, normalizes records, validates them, and publishes them to Kafka for downstream systems.

## Start here

- Runtime and usage: [Ingestion/README.md](Ingestion/README.md)

## Current delivered scope

- local Kafka stack and stream topic bootstrap
- streaming ingestion for Open-Meteo, OpenWeatherMap, AQICN, and IQAir
- source fallback in the production stream pipelines
- shared normalization, validation, metadata, DLQ, and checkpoint handling
- Airflow orchestration for streaming trigger flows
- Kafka export script for real topic data

## Repository layout

- [Ingestion](Ingestion/README.md): production-facing ingestion module
