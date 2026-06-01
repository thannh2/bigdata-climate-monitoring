from __future__ import annotations

from typing import Any


OPEN_METEO_LOCATIONS: list[dict[str, Any]] = [
    {"city": "An Giang", "latitude": 10.5216, "longitude": 105.1259},
    {"city": "Ba Ria - Vung Tau", "latitude": 10.4114, "longitude": 107.1362},
    {"city": "Bac Giang", "latitude": 21.2819, "longitude": 106.1970},
    {"city": "Bac Kan", "latitude": 22.1470, "longitude": 105.8348},
    {"city": "Bac Lieu", "latitude": 9.2941, "longitude": 105.7278},
    {"city": "Bac Ninh", "latitude": 21.1861, "longitude": 106.0763},
    {"city": "Ben Tre", "latitude": 10.2434, "longitude": 106.3759},
    {"city": "Binh Dinh", "latitude": 13.7820, "longitude": 109.2197},
    {"city": "Binh Duong", "latitude": 10.9804, "longitude": 106.6519},
    {"city": "Binh Phuoc", "latitude": 11.5349, "longitude": 106.8834},
    {"city": "Binh Thuan", "latitude": 10.9289, "longitude": 108.1021},
    {"city": "Ca Mau", "latitude": 9.1769, "longitude": 105.1524},
    {"city": "Can Tho", "latitude": 10.0379, "longitude": 105.7869},
    {"city": "Cao Bang", "latitude": 22.6666, "longitude": 106.2639},
    {"city": "Da Nang", "latitude": 16.0544, "longitude": 108.2022},
    {"city": "Dak Lak", "latitude": 12.6667, "longitude": 108.0500},
    {"city": "Dak Nong", "latitude": 12.0042, "longitude": 107.6907},
    {"city": "Dien Bien", "latitude": 21.3860, "longitude": 103.0230},
    {"city": "Dong Nai", "latitude": 10.9574, "longitude": 106.8426},
    {"city": "Dong Thap", "latitude": 10.4938, "longitude": 105.6882},
    {"city": "Gia Lai", "latitude": 13.9833, "longitude": 108.0000},
    {"city": "Ha Giang", "latitude": 22.8233, "longitude": 104.9836},
    {"city": "Ha Nam", "latitude": 20.5411, "longitude": 105.9122},
    {"city": "Hanoi", "latitude": 21.0285, "longitude": 105.8542},
    {"city": "Ha Tinh", "latitude": 18.3428, "longitude": 105.9057},
    {"city": "Hai Duong", "latitude": 20.9373, "longitude": 106.3145},
    {"city": "Hai Phong", "latitude": 20.8449, "longitude": 106.6881},
    {"city": "Hau Giang", "latitude": 9.7845, "longitude": 105.4701},
    {"city": "HCMC", "latitude": 10.7769, "longitude": 106.6964, "alias": "Ho Chi Minh City"},
    {"city": "Hoa Binh", "latitude": 20.8133, "longitude": 105.3383},
    {"city": "Hung Yen", "latitude": 20.6464, "longitude": 106.0511},
    {"city": "Khanh Hoa", "latitude": 12.2388, "longitude": 109.1967},
    {"city": "Kien Giang", "latitude": 10.0125, "longitude": 105.0809},
    {"city": "Kon Tum", "latitude": 14.3545, "longitude": 108.0076},
    {"city": "Lai Chau", "latitude": 22.3964, "longitude": 103.4582},
    {"city": "Lam Dong", "latitude": 11.9404, "longitude": 108.4583},
    {"city": "Lang Son", "latitude": 21.8537, "longitude": 106.7610},
    {"city": "Lao Cai", "latitude": 22.4856, "longitude": 103.9707},
    {"city": "Long An", "latitude": 10.5359, "longitude": 106.4137},
    {"city": "Nam Dinh", "latitude": 20.4388, "longitude": 106.1621},
    {"city": "Nghe An", "latitude": 18.6734, "longitude": 105.6923},
    {"city": "Ninh Binh", "latitude": 20.2506, "longitude": 105.9745},
    {"city": "Ninh Thuan", "latitude": 11.5658, "longitude": 108.9886},
    {"city": "Phu Tho", "latitude": 21.3227, "longitude": 105.4010},
    {"city": "Phu Yen", "latitude": 13.0955, "longitude": 109.3209},
    {"city": "Quang Binh", "latitude": 17.4689, "longitude": 106.6223},
    {"city": "Quang Nam", "latitude": 15.5394, "longitude": 108.0191},
    {"city": "Quang Ngai", "latitude": 15.1214, "longitude": 108.8044},
    {"city": "Quang Ninh", "latitude": 21.0064, "longitude": 107.2925},
    {"city": "Quang Tri", "latitude": 16.7500, "longitude": 107.2000},
    {"city": "Soc Trang", "latitude": 9.6025, "longitude": 105.9739},
    {"city": "Son La", "latitude": 21.3256, "longitude": 103.9188},
    {"city": "Tay Ninh", "latitude": 11.3675, "longitude": 106.1193},
    {"city": "Thai Binh", "latitude": 20.4463, "longitude": 106.3365},
    {"city": "Thai Nguyen", "latitude": 21.5942, "longitude": 105.8481},
    {"city": "Thanh Hoa", "latitude": 19.8075, "longitude": 105.7764},
    {"city": "Hue", "latitude": 16.4637, "longitude": 107.5909, "alias": "Thua Thien Hue"},
    {"city": "Tien Giang", "latitude": 10.4493, "longitude": 106.3420},
    {"city": "Tra Vinh", "latitude": 9.9477, "longitude": 106.3423},
    {"city": "Tuyen Quang", "latitude": 21.8236, "longitude": 105.2140},
    {"city": "Vinh Long", "latitude": 10.2537, "longitude": 105.9722},
    {"city": "Vinh Phuc", "latitude": 21.3089, "longitude": 105.6049},
    {"city": "Yen Bai", "latitude": 21.7229, "longitude": 104.9113},
]

AQICN_LOCATIONS: list[dict[str, Any]] = [
    {"city": "Hanoi", "feed": "hanoi"},
    {"city": "Hai Phong", "feed": "hai-phong"},
    {"city": "Da Nang", "feed": "da-nang"},
    {"city": "Hue", "feed": "hue", "alias": "Thua Thien Hue"},
    {"city": "HCMC", "feed": "ho-chi-minh-city", "alias": "Ho Chi Minh City"},
    {"city": "Can Tho", "feed": "can-tho"},
]

IQAIR_LOCATIONS: list[dict[str, Any]] = [
    {"city": "Hanoi", "state": "Ha Noi", "country": "Vietnam"},
    {"city": "Hai Phong", "state": "Hai Phong", "country": "Vietnam"},
    {"city": "Da Nang", "state": "Da Nang", "country": "Vietnam"},
    {"city": "Hue", "alias": "Thua Thien Hue", "state": "Thua Thien Hue", "country": "Vietnam"},
    {"city": "Ho Chi Minh City", "alias": "HCMC", "state": "Ho Chi Minh City", "country": "Vietnam"},
    {"city": "Can Tho", "state": "Can Tho", "country": "Vietnam"},
]


def display_location(location: dict[str, Any]) -> str:
    return str(location.get("city"))


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
