from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Region(str, Enum):
    bac = "bac"
    trung = "trung"
    nam = "nam"


class NormalizedWeatherRecord(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    source: str
    region: Region
    city: str
    station_id: str
    latitude: float
    longitude: float
    event_time: datetime
    ingestion_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    temperature_c: Optional[float] = None
    humidity: Optional[float] = None
    pressure_hpa: Optional[float] = None
    wind_speed_mps: Optional[float] = None
    weather_main: Optional[str] = None
    weather_desc: Optional[str] = None
    data_version: str = "v1"


class NormalizedAirQualityRecord(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    source: str
    region: Region
    city: str
    station_id: str
    latitude: float
    longitude: float
    event_time: datetime
    ingestion_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    aqi: Optional[float] = None
    quality_level: Optional[str] = None
    pm25: Optional[float] = None
    pm10: Optional[float] = None
    co: Optional[float] = None
    no2: Optional[float] = None
    so2: Optional[float] = None
    o3: Optional[float] = None
    data_version: str = "v1"


REGION_CITY_MAP = {
    "hanoi": (Region.bac, "Hanoi", 21.0285, 105.8542),
    "ha noi": (Region.bac, "Hanoi", 21.0285, 105.8542),
    "hai phong": (Region.bac, "Hai Phong", 20.8449, 106.6881),
    "quang ninh": (Region.bac, "Quang Ninh", 21.0069, 107.0488),
    "da nang": (Region.trung, "Da Nang", 16.0544, 108.2022),
    "hue": (Region.trung, "Hue", 16.4637, 107.5909),
    "nghe an": (Region.trung, "Nghe An", 19.8167, 105.6667),
    "hcmc": (Region.nam, "HCMC", 10.7769, 106.6964),
    "ho chi minh city": (Region.nam, "HCMC", 10.7769, 106.6964),
    "can tho": (Region.nam, "Can Tho", 10.0379, 105.7869),
    "dong nai": (Region.nam, "Dong Nai", 10.8231, 106.6297),
}

WMO_CODE_MAP = {
    0: ("Clear", "Clear sky"),
    1: ("Partly Cloudy", "Mainly clear"),
    2: ("Partly Cloudy", "Partly cloudy"),
    3: ("Overcast", "Overcast"),
    45: ("Fog", "Fog"),
    48: ("Fog", "Depositing rime fog"),
    51: ("Drizzle", "Light drizzle"),
    53: ("Drizzle", "Moderate drizzle"),
    55: ("Drizzle", "Dense drizzle"),
    61: ("Rain", "Slight rain"),
    63: ("Rain", "Moderate rain"),
    65: ("Rain", "Heavy rain"),
    71: ("Snow", "Slight snow fall"),
    73: ("Snow", "Moderate snow fall"),
    75: ("Snow", "Heavy snow fall"),
    80: ("Rain", "Rain showers"),
    81: ("Rain", "Rain showers"),
    82: ("Rain", "Violent rain showers"),
    95: ("Thunderstorm", "Thunderstorm"),
    96: ("Thunderstorm", "Thunderstorm with slight hail"),
    99: ("Thunderstorm", "Thunderstorm with heavy hail"),
}


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    text = str(value).strip()
    if not text:
        raise ValueError("event_time is required")

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _infer_region_city(city: Optional[str], latitude: Optional[float], longitude: Optional[float]):
    if city:
        key = city.strip().lower()
        if key in REGION_CITY_MAP:
            return REGION_CITY_MAP[key]

    if latitude is not None and longitude is not None:
        # fallback heuristic based on configured anchors
        if latitude >= 18:
            return Region.bac, city or "Hanoi", latitude, longitude
        if latitude >= 14:
            return Region.trung, city or "Da Nang", latitude, longitude
        return Region.nam, city or "HCMC", latitude, longitude

    return Region.bac, city or "Hanoi", 21.0285, 105.8542


def _format_weather_from_wmo(code: Optional[int]):
    if code is None:
        return None, None
    return WMO_CODE_MAP.get(int(code), ("Unknown", "Unknown weather condition"))


def _aqi_level(aqi: Optional[float]):
    if aqi is None:
        return None
    if aqi <= 50:
        return "Good"
    if aqi <= 100:
        return "Moderate"
    if aqi <= 150:
        return "Unhealthy for Sensitive Groups"
    if aqi <= 200:
        return "Unhealthy"
    if aqi <= 300:
        return "Very Unhealthy"
    return "Hazardous"


def normalize_weather(record_raw: dict[str, Any], source: Optional[str] = None) -> NormalizedWeatherRecord:
    """Normalize raw weather payloads from Open-Meteo or OpenWeatherMap."""
    source_name = (source or record_raw.get("source") or "open-meteo").lower()

    if "current" in record_raw:
        current = record_raw["current"]
        latitude = record_raw.get("latitude")
        longitude = record_raw.get("longitude")
        city = record_raw.get("city") or record_raw.get("name")
        event_time = current.get("time")
        temp = current.get("temperature_2m")
        humidity = current.get("relative_humidity_2m")
        pressure = current.get("pressure_msl")
        wind_speed = current.get("wind_speed_10m")
        weather_code = current.get("weather_code")
        weather_main, weather_desc = _format_weather_from_wmo(weather_code)
        if wind_speed is not None and source_name == "open-meteo":
            wind_speed = round(float(wind_speed) * 0.27778, 4)
        city = city or record_raw.get("timezone", "Hanoi")
    else:
        main = record_raw.get("main", {})
        wind = record_raw.get("wind", {})
        weather = (record_raw.get("weather") or [{}])[0]
        coord = record_raw.get("coord", {})
        latitude = coord.get("lat") or record_raw.get("lat")
        longitude = coord.get("lon") or record_raw.get("lon")
        city = record_raw.get("name") or record_raw.get("city")
        event_time = record_raw.get("dt")
        temp = main.get("temp")
        humidity = main.get("humidity")
        pressure = main.get("pressure")
        wind_speed = wind.get("speed")
        weather_main = weather.get("main")
        weather_desc = weather.get("description")

    region, city_name, latitude, longitude = _infer_region_city(city, latitude, longitude)
    station_id = record_raw.get("station_id") or f"{source_name}_{city_name.lower().replace(' ', '_')}"

    return NormalizedWeatherRecord(
        source=source_name,
        region=region,
        city=city_name,
        station_id=station_id,
        latitude=float(latitude),
        longitude=float(longitude),
        event_time=_parse_datetime(event_time),
        temperature_c=float(temp) if temp is not None else None,
        humidity=float(humidity) if humidity is not None else None,
        pressure_hpa=float(pressure) if pressure is not None else None,
        wind_speed_mps=float(wind_speed) if wind_speed is not None else None,
        weather_main=weather_main,
        weather_desc=weather_desc,
    )


def normalize_air_quality(record_raw: dict[str, Any], source: Optional[str] = None) -> NormalizedAirQualityRecord:
    """Normalize raw air-quality payloads from AQICN, IQAir, Open-Meteo or OpenWeatherMap."""
    source_name = (source or record_raw.get("source") or "aqicn").lower()

    if "data" in record_raw and source_name == "aqicn":
        data = record_raw["data"]
        city = (data.get("city") or {}).get("name")
        geo = (data.get("city") or {}).get("geo") or [None, None]
        latitude = geo[0]
        longitude = geo[1]
        event_time = (data.get("time") or {}).get("iso") or (data.get("time") or {}).get("s")
        aqi = data.get("aqi")
        pollutants = data.get("iaqi") or {}
        pm25 = (pollutants.get("pm25") or {}).get("v")
        pm10 = (pollutants.get("pm10") or {}).get("v")
        co = (pollutants.get("co") or {}).get("v")
        no2 = (pollutants.get("no2") or {}).get("v")
        so2 = (pollutants.get("so2") or {}).get("v")
        o3 = (pollutants.get("o3") or {}).get("v")
        station_id = str(data.get("idx") or record_raw.get("station_id") or "aqicn_station")
    elif "current" in record_raw and source_name == "iqair":
        current = record_raw["current"]
        pollution = current.get("pollution") or {}
        city = record_raw.get("city") or (record_raw.get("data") or {}).get("city")
        latitude = record_raw.get("latitude")
        longitude = record_raw.get("longitude")
        event_time = pollution.get("ts") or record_raw.get("event_time")
        aqi = pollution.get("aqius") or pollution.get("aqicn")
        pm25 = pollution.get("pm25")
        pm10 = pollution.get("pm10")
        co = pollution.get("co")
        no2 = pollution.get("no2")
        so2 = pollution.get("so2")
        o3 = pollution.get("o3")
        station_id = str((record_raw.get("station") or {}).get("id") or record_raw.get("station_id") or "iqair_station")
    elif "current" in record_raw and source_name == "open-meteo":
        current = record_raw["current"]
        city = record_raw.get("city") or record_raw.get("name")
        latitude = record_raw.get("latitude")
        longitude = record_raw.get("longitude")
        event_time = current.get("time")
        aqi = current.get("us_aqi")
        pm25 = current.get("pm2_5")
        pm10 = current.get("pm10")
        co = current.get("co") or current.get("carbon_monoxide")
        no2 = current.get("no2") or current.get("nitrogen_dioxide")
        so2 = current.get("so2") or current.get("sulphur_dioxide")
        o3 = current.get("o3") or current.get("ozone")
        station_id = record_raw.get("station_id") or f"open-meteo_{(city or 'unknown').lower().replace(' ', '_')}"
    else:
        data = record_raw.get("list", [{}])[0]
        components = data.get("components", {})
        city = record_raw.get("city") or record_raw.get("name")
        latitude = record_raw.get("lat")
        longitude = record_raw.get("lon")
        event_time = data.get("dt") or record_raw.get("dt")
        aqi = (data.get("main") or {}).get("aqi")
        pm25 = components.get("pm2_5")
        pm10 = components.get("pm10")
        co = components.get("co")
        no2 = components.get("no2")
        so2 = components.get("so2")
        o3 = components.get("o3")
        station_id = record_raw.get("station_id") or f"openweathermap_{(city or 'unknown').lower().replace(' ', '_')}"

    region, city_name, latitude, longitude = _infer_region_city(city, latitude, longitude)

    return NormalizedAirQualityRecord(
        source=source_name,
        region=region,
        city=city_name,
        station_id=station_id,
        latitude=float(latitude),
        longitude=float(longitude),
        event_time=_parse_datetime(event_time),
        aqi=float(aqi) if aqi is not None else None,
        quality_level=_aqi_level(float(aqi)) if aqi is not None else None,
        pm25=float(pm25) if pm25 is not None else None,
        pm10=float(pm10) if pm10 is not None else None,
        co=float(co) if co is not None else None,
        no2=float(no2) if no2 is not None else None,
        so2=float(so2) if so2 is not None else None,
        o3=float(o3) if o3 is not None else None,
    )
