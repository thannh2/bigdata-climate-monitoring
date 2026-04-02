from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / "config" / ".env"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test source API connectivity for ingestion.")
    parser.add_argument("--lat", default="10.8231")
    parser.add_argument("--lon", default="106.6297")
    parser.add_argument("--city", default="Hanoi")
    parser.add_argument("--state", default="Ha Noi")
    parser.add_argument("--country", default="Vietnam")
    return parser.parse_args()


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        os.environ[name.strip()] = value.strip().strip('"').strip("'")


def fetch_json(url: str) -> dict[str, Any]:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def main() -> None:
    args = parse_args()
    load_env_file(ENV_PATH)

    owm_key = os.getenv("OWM_API_KEY")
    aqicn_key = os.getenv("AQICN_API_KEY")
    iqair_key = os.getenv("IQAIR_API_KEY")

    print("\n1) Open-Meteo (no API key)")
    open_meteo_url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={args.lat}&longitude={args.lon}"
        "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,pm10,pm2_5,us_aqi"
    )
    open_meteo = fetch_json(open_meteo_url)
    print("Open-Meteo keys:", ", ".join(open_meteo.keys()))
    print("Open-Meteo current keys:", ", ".join((open_meteo.get("current") or {}).keys()))

    print("\n2) OpenWeatherMap (requires API key)")
    if not owm_key:
        print("OWM_API_KEY is missing. Put it in Ingestion/config/.env.")
    else:
        weather_url = (
            "https://api.openweathermap.org/data/2.5/weather"
            f"?lat={args.lat}&lon={args.lon}&appid={owm_key}&units=metric"
        )
        air_url = (
            "https://api.openweathermap.org/data/2.5/air_pollution"
            f"?lat={args.lat}&lon={args.lon}&appid={owm_key}"
        )
        owm_weather = fetch_json(weather_url)
        owm_air = fetch_json(air_url)
        print("OWM weather keys:", ", ".join(owm_weather.keys()))
        print("OWM weather.main keys:", ", ".join((owm_weather.get("main") or {}).keys()))
        print("OWM air keys:", ", ".join(owm_air.keys()))
        first_air = (owm_air.get("list") or [{}])[0]
        print("OWM air.main keys:", ", ".join((first_air.get("main") or {}).keys()))
        print("OWM air.components keys:", ", ".join((first_air.get("components") or {}).keys()))

    print("\n3) IQAir (AirVisual API)")
    if not iqair_key:
        print("IQAIR_API_KEY is missing. Put it in Ingestion/config/.env.")
    else:
        iqair_url = (
            "https://api.airvisual.com/v2/city"
            f"?city={quote(args.city)}&state={quote(args.state)}&country={quote(args.country)}&key={iqair_key}"
        )
        iqair = fetch_json(iqair_url)
        print("IQAir status:", iqair.get("status"))
        if iqair.get("status") == "success":
            data = iqair.get("data") or {}
            current = data.get("current") or {}
            print("IQAir data keys:", ", ".join(data.keys()))
            print("IQAir current keys:", ", ".join(current.keys()))
            print("IQAir pollution keys:", ", ".join((current.get("pollution") or {}).keys()))
        else:
            print("IQAir response:", json.dumps(iqair, ensure_ascii=True))

    print("\n4) AQICN")
    if not aqicn_key:
        print("AQICN_API_KEY is missing. Put it in Ingestion/config/.env.")
    else:
        aqicn_url = f"https://api.waqi.info/feed/{quote(args.city)}/?token={aqicn_key}"
        aqicn = fetch_json(aqicn_url)
        print("AQICN status:", aqicn.get("status"))
        if aqicn.get("status") == "ok":
            data = aqicn.get("data") or {}
            city = (data.get("city") or {}).get("name")
            iaqi = data.get("iaqi") or {}
            print("AQICN city:", city)
            print("AQICN iaqi keys:", ", ".join(iaqi.keys()))
        else:
            print("AQICN response:", json.dumps(aqicn, ensure_ascii=True))

    print("\nDone.")


if __name__ == "__main__":
    main()
