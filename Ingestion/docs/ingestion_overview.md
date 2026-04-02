# Ingestion Overview

## Summary

The ingestion layer collects weather and air-quality data from supported external APIs, normalizes records, validates them, and publishes them to Kafka for downstream consumers.

## Target architecture

```text
External APIs
   ->
Ingestion Services (batch + streaming)
   ->
Kafka topics
   ->
Spark / Lakehouse / Dashboard / ML
```

## Main topics

- `weather.raw.batch`
- `weather.raw.stream`
- `weather.raw.dlq`
- `air_quality.raw.batch`
- `air_quality.raw.stream`
- `air_quality.raw.dlq`

## Runtime flow

- batch collectors backfill historical weather and air-quality records
- stream collectors poll the latest records and use checkpoints to avoid duplicates
- validators run before publish
- invalid or failed records are routed to DLQ

## Related documents

- [README](README.md)
- [Requirements](requirements_spec.md)
- [API Mapping and Survey](api_mapping_and_survey.md)
- [Normalized Schema Design](normalized_schema_design.md)
- [Input Quality Validation](input_quality_validation.md)
