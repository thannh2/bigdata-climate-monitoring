from __future__ import annotations

from typing import Any


OPEN_METEO_LOCATIONS: list[dict[str, Any]] = [
    {"city": "Hanoi", "latitude": 21.0285, "longitude": 105.8542},
    {"city": "Hai Phong", "latitude": 20.8449, "longitude": 106.6881},
    {"city": "Da Nang", "latitude": 16.0544, "longitude": 108.2022},
    {"city": "Hue", "latitude": 16.4637, "longitude": 107.5909},
    {"city": "HCMC", "latitude": 10.7769, "longitude": 106.6964},
    {"city": "Can Tho", "latitude": 10.0379, "longitude": 105.7869},
]

AQICN_LOCATIONS: list[dict[str, Any]] = [
    {"city": "Hanoi", "feed": "hanoi"},
    {"city": "Hai Phong", "feed": "hai-phong"},
    {"city": "Da Nang", "feed": "da-nang"},
    {"city": "Hue", "feed": "hue"},
    {"city": "HCMC", "feed": "ho-chi-minh-city"},
    {"city": "Can Tho", "feed": "can-tho"},
]

IQAIR_LOCATIONS: list[dict[str, Any]] = [
    {"city": "Hanoi", "state": "Ha Noi", "country": "Vietnam"},
    {"city": "Hai Phong", "state": "Hai Phong", "country": "Vietnam"},
    {"city": "Da Nang", "state": "Da Nang", "country": "Vietnam"},
    {"city": "Hue", "state": "Thua Thien Hue", "country": "Vietnam"},
    {"city": "Ho Chi Minh City", "alias": "HCMC", "state": "Ho Chi Minh City", "country": "Vietnam"},
    {"city": "Can Tho", "state": "Can Tho", "country": "Vietnam"},
]


def display_location(location: dict[str, Any]) -> str:
    return str(location.get("alias") or location["city"])


def location_names(catalog: list[dict[str, Any]]) -> list[str]:
    return [display_location(location) for location in catalog]


def filter_locations(catalog: list[dict[str, Any]], selected_locations: list[str]) -> list[dict[str, Any]]:
    selected = {name.strip().lower() for name in selected_locations}
    result: list[dict[str, Any]] = []
    for location in catalog:
        names = {str(location["city"]).lower()}
        alias = location.get("alias")
        if alias:
            names.add(str(alias).lower())
        if names & selected:
            result.append(location)
    return result
