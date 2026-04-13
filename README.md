# Big Data Climate Monitoring

This repository currently packages the ingestion layer of a climate monitoring pipeline. The implemented module collects weather and air-quality data from external APIs, normalizes records, validates them, and publishes them to Kafka for downstream systems.

## Start here

- Runtime and usage: [Ingestion/README.md](Ingestion/README.md)

## Current delivered scope

- local Kafka stack and topic bootstrap
- historical batch ingestion for Open-Meteo weather and air quality
- streaming ingestion for Open-Meteo, OpenWeatherMap, AQICN, and IQAir
- source fallback in the main stream pipelines
- shared normalization, validation, metadata, DLQ, and checkpoint handling
- Airflow orchestration for batch and streaming trigger flows
- Kafka export script for producing demo datasets

## Repository layout

- [Ingestion](Ingestion/README.md): production-facing ingestion module
