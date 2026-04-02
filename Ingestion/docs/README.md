# Climate Monitoring Ingestion Documentation

## Goal

This folder contains the current documentation for the ingestion layer.

## Main documents

- [Ingestion Requirements Specification](requirements_spec.md)
- [API Mapping and Survey](api_mapping_and_survey.md)
- [Normalized Schema Design](normalized_schema_design.md)
- [Input Quality Validation](input_quality_validation.md)
- [Ingestion Overview](ingestion_overview.md)

## Reading order

1. Start with [requirements_spec.md](requirements_spec.md)
2. Review [api_mapping_and_survey.md](api_mapping_and_survey.md)
3. Review [normalized_schema_design.md](normalized_schema_design.md)
4. Review [input_quality_validation.md](input_quality_validation.md)
5. Check [ingestion_overview.md](ingestion_overview.md)

## Structure

```text
docs/
|-- README.md
|-- api_mapping_and_survey.md
|-- ingestion_overview.md
|-- input_quality_validation.md
|-- normalized_schema_design.md
|-- requirements_spec.md
`-- schemas/
```

## Status

- Requirements: ready
- API mapping: ready
- Schema design: ready
- Quality rules: ready
- Implementation status: Kafka, collectors, DLQ, metadata, and Airflow scaffolding are in place
