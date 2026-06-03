SELECT
    station_id,
    region,
    city,
    COUNT(*) AS total_rows,
    AVG(CASE WHEN pm25 IS NULL THEN 1 ELSE 0 END) AS pm25_missing_rate,
    AVG(CASE WHEN temperature_c IS NULL THEN 1 ELSE 0 END) AS temp_missing_rate,
    AVG(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END) AS humidity_missing_rate,
    AVG(CASE WHEN wind_speed_mps IS NULL THEN 1 ELSE 0 END) AS wind_missing_rate
FROM gold_features_hourly
WHERE event_hour >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY station_id, region, city
ORDER BY pm25_missing_rate DESC;
