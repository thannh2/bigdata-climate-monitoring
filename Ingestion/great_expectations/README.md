# Great Expectations Readiness

This folder contains downstream-ready input quality contracts for normalized ingestion records.

## Scope

- ingestion validators perform basic blocking checks before Kafka publish
- Great Expectations should run downstream as formal data quality validation
- both layers should use the same business rules where possible

## Current status

- `weather_input_quality_suite.json`: GE-aligned suite for weather records
- `air_quality_input_quality_suite.json`: GE-aligned suite for air-quality records

These suite documents are intentionally simple and version-neutral. They are safe to review in this repository even when Great Expectations is not installed locally.

## Python compatibility note

The current local environment uses Python 3.14. Great Expectations should be installed in a compatible downstream environment such as Airflow or analytics tooling that runs a supported Python version.
