# API Mapping and Survey

## Purpose

This document summarizes how external API payloads map into the ingestion schema and records the survey status of the supported sources.

## Official sources

### Weather

- Open-Meteo
- OpenWeatherMap

### Air quality

- AQICN
- IQAir
- Open-Meteo
- OpenWeatherMap air pollution endpoint

## Canonical weather schema

```json
{
  "event_id": "uuid",
  "source": "open-meteo|openweathermap",
  "region": "bac|trung|nam",
  "city": "Hanoi|HCMC|DaNang",
  "station_id": "station_01",
  "latitude": 21.0285,
  "longitude": 105.8542,
  "event_time": "2026-04-02T09:55:00Z",
  "ingestion_time": "2026-04-02T10:00:00Z",
  "temperature_c": 31.2,
  "humidity": 68,
  "pressure_hpa": 1004,
  "wind_speed_mps": 3.4,
  "weather_main": "Clouds",
  "weather_desc": "scattered clouds",
  "data_version": "v1"
}
```

## Canonical air-quality schema

```json
{
  "event_id": "uuid",
  "source": "aqicn|iqair|open-meteo|openweathermap",
  "region": "bac|trung|nam",
  "city": "Hanoi|HCMC|DaNang",
  "station_id": "aq_station_01",
  "latitude": 21.0285,
  "longitude": 105.8542,
  "event_time": "2026-04-02T09:55:00Z",
  "ingestion_time": "2026-04-02T10:00:00Z",
  "aqi": 132,
  "quality_level": "Unhealthy for Sensitive Groups",
  "pm25": 58.4,
  "pm10": 87.2,
  "co": 0.7,
  "no2": 14.1,
  "so2": 5.6,
  "o3": 21.3,
  "data_version": "v1"
}
```

## Field mapping summary

### Open-Meteo

- best fit for weather
- historical weather and air-quality APIs support date ranges
- weather fields map directly from `current` or `hourly` values
- air-quality support includes AQI, PM values, and gas measurements depending on endpoint

### AQICN

- best fit for AQI and pollutant detail
- core fields come from `data.aqi`, `data.time.iso`, and `data.iaqi.*.v`
- some pollutant fields may be missing depending on station coverage

### OpenWeatherMap

- suitable as weather backup
- air pollution requires a separate endpoint
- timestamps arrive as Unix seconds and must be normalized to UTC

### IQAir

- suitable as AQI backup
- requires API key and endpoint confirmation
- useful when AQICN is limited or unavailable

## Source survey status

| Source | Purpose | Auth | Current status | Notes |
|---|---|---|---|---|
| Open-Meteo | weather primary | none | tested | best source for historical batch demo |
| AQICN | air-quality primary | token | tested | demo token is rate limited |
| OpenWeatherMap | weather backup | API key | partially tested | requires valid key for full verification |
| IQAir | air-quality backup | API key | pending | requires dedicated key and endpoint validation |

## Recommended source policy

### Weather

- primary: Open-Meteo
- fallback: OpenWeatherMap

### Air quality

- primary: AQICN
- fallback: IQAir

## Implementation notes

- normalize all timestamps to UTC
- derive `quality_level` from AQI when the source does not provide it
- keep source-specific unit handling explicit in code
- route invalid payloads to DLQ once DLQ handling is implemented
