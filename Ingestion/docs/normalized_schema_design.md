# Normalized Schema Design

## Goal

Normalize raw source payloads into shared internal records for Kafka and downstream processing.

Primary entry points:

- `normalize_weather(record_raw)`
- `normalize_air_quality(record_raw)`

Implementation source:

- [normalized_schema.py](../validators/normalized_schema.py)

## Common contract

Every normalized record must include:

- `event_id`
- `source`
- `region`
- `city`
- `station_id`
- `latitude`
- `longitude`
- `event_time`
- `ingestion_time`
- `data_version`

## Weather schema

### Model

- `NormalizedWeatherRecord`

### JSON Schema

- [weather_normalized.schema.json](schemas/weather_normalized.schema.json)

### Fields

| Field | Type | Required | Note |
|---|---|---:|---|
| `event_id` | UUID | Yes | generated automatically |
| `source` | string | Yes | `open-meteo` or `openweathermap` |
| `region` | enum | Yes | `bac`, `trung`, `nam` |
| `city` | string | Yes | internal city name |
| `station_id` | string | Yes | stable logical ID |
| `latitude` | float | Yes | decimal degrees |
| `longitude` | float | Yes | decimal degrees |
| `event_time` | datetime | Yes | normalized to UTC |
| `ingestion_time` | datetime | Yes | current UTC by default |
| `temperature_c` | float | No | Celsius |
| `humidity` | float | No | percent |
| `pressure_hpa` | float | No | hPa |
| `wind_speed_mps` | float | No | m/s |
| `weather_main` | string | No | short label |
| `weather_desc` | string | No | detailed description |
| `data_version` | string | Yes | schema version |

### Mapping summary

#### Open-Meteo

- `current.time` -> `event_time`
- `current.temperature_2m` -> `temperature_c`
- `current.relative_humidity_2m` -> `humidity`
- `current.pressure_msl` -> `pressure_hpa`
- `current.wind_speed_10m` -> `wind_speed_mps`
- `current.weather_code` -> weather text using WMO mapping

#### OpenWeatherMap

- `dt` -> `event_time`
- `main.temp` -> `temperature_c`
- `main.humidity` -> `humidity`
- `main.pressure` -> `pressure_hpa`
- `wind.speed` -> `wind_speed_mps`
- `weather[0].main` -> `weather_main`
- `weather[0].description` -> `weather_desc`

## Air-quality schema

### Model

- `NormalizedAirQualityRecord`

### JSON Schema

- [air_quality_normalized.schema.json](schemas/air_quality_normalized.schema.json)

### Fields

| Field | Type | Required | Note |
|---|---|---:|---|
| `event_id` | UUID | Yes | generated automatically |
| `source` | string | Yes | `aqicn`, `iqair`, `open-meteo`, `openweathermap` |
| `region` | enum | Yes | `bac`, `trung`, `nam` |
| `city` | string | Yes | internal city name |
| `station_id` | string | Yes | stable logical ID |
| `latitude` | float | Yes | decimal degrees |
| `longitude` | float | Yes | decimal degrees |
| `event_time` | datetime | Yes | normalized to UTC |
| `ingestion_time` | datetime | Yes | current UTC by default |
| `aqi` | float | No | AQI index |
| `quality_level` | string | No | derived from AQI |
| `pm25` | float | No | ug/m3 |
| `pm10` | float | No | ug/m3 |
| `co` | float | No | source-specific unit |
| `no2` | float | No | source-specific unit |
| `so2` | float | No | source-specific unit |
| `o3` | float | No | source-specific unit |
| `data_version` | string | Yes | schema version |

### Mapping summary

#### AQICN

- `data.time.iso` -> `event_time`
- `data.aqi` -> `aqi`
- `data.iaqi.*.v` -> pollutant fields

#### IQAir

- `current.pollution.ts` -> `event_time`
- `current.pollution.aqius` or `aqicn` -> `aqi`
- `current.pollution.*` -> pollutant fields

#### Open-Meteo

- `current.time` -> `event_time`
- `current.us_aqi` -> `aqi`
- `current.pm2_5` -> `pm25`
- `current.pm10` -> `pm10`
- `current.carbon_monoxide` -> `co`
- `current.nitrogen_dioxide` -> `no2`
- `current.sulphur_dioxide` -> `so2`
- `current.ozone` -> `o3`

#### OpenWeatherMap

- `list[0].main.aqi` -> `aqi`
- `list[0].components.pm2_5` -> `pm25`
- `list[0].components.pm10` -> `pm10`
- `list[0].components.co` -> `co`
- `list[0].components.no2` -> `no2`
- `list[0].components.so2` -> `so2`
- `list[0].components.o3` -> `o3`

## Validation notes

- `event_time` and `ingestion_time` must be UTC
- `station_id` should be stable for deduplication
- `quality_level` should be derived when the source does not provide it
- JSON Schema is the base contract for future Avro generation

## Deliverables covered

- Pydantic models
- weather and air normalizers
- JSON Schemas
- mapping notes
