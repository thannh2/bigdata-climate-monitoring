# Input Quality Validation

## Role split

The ingestion layer is responsible for basic blocking validation before records are published to Kafka.

Examples:

- required fields such as `event_time`, `city`, and `source`
- weather operating ranges such as `temperature_c`, `humidity`, `wind_speed_mps`, and `pressure_hpa`
- air-quality ranges such as `aqi`, `pm25`, and `pm10`
- numeric parsing checks for coordinates and pollutant values

Great Expectations should handle formal quality validation downstream.

Examples:

- validating landed Kafka batches after bronze ingestion
- generating auditable validation reports
- tracking quality drift over time
- enforcing non-blocking or warning-level expectations outside the collector path

## Rule alignment

To avoid rule drift, the ingestion validators and downstream Great Expectations suites should follow the same business thresholds.

Current shared rules:

### Weather

- `event_time` is required
- `city` is required
- `source` is required
- `temperature_c` must be between `-20` and `60`
- `humidity` must be between `0` and `100`
- `wind_speed_mps >= 0`
- `pressure_hpa > 0`

### Air quality

- `event_time` is required
- `city` is required
- `source` is required
- `aqi >= 0`
- `pm25 >= 0`
- `pm10 >= 0`

## Repository assets

- `Ingestion/validators/weather_validator.py`
- `Ingestion/validators/air_validator.py`
- `Ingestion/utils/quality_rules.py`
- `Ingestion/great_expectations/weather_input_quality_suite.json`
- `Ingestion/great_expectations/air_quality_input_quality_suite.json`
- `Ingestion/scripts/validate_input_quality.py`

## Practical note

The local development environment currently uses Python 3.14. Great Expectations should be installed and executed in a compatible downstream environment, for example an Airflow image or a separate analytics virtual environment.
