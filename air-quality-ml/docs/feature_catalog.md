# Feature Catalog (Phase 1)

## Core numeric
- temperature_c, humidity, pressure_hpa, wind_speed_mps, precipitation_mm
- pm25, pm10, aqi, co, no2, so2, o3

## Engineered
- hour_sin, hour_cos, dow_sin, dow_cos
- coord_x, coord_y, coord_z
- wind_u, wind_v
- dew_point, is_stagnant_air
- pm25_lag_1h, pm25_lag_3h, pm25_lag_6h
- aqi_lag_1h, aqi_lag_6h
- pressure_delta_3h
- temp_mean_6h, humidity_mean_6h, wind_speed_mean_6h
- pm25_acc_12h

## Targets
- target_pm25_1h, target_pm25_3h, target_pm25_6h
- target_alert_1h, target_alert_3h, target_alert_6h
