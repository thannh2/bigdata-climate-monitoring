SELECT
    station_id,
    region,
    city,
    AVG(pm25) AS avg_pm25_24h,
    MAX(pm25) AS max_pm25_24h,
    AVG(aqi) AS avg_aqi_24h
FROM gold_features_hourly
WHERE event_hour >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY station_id, region, city
ORDER BY avg_pm25_24h DESC
LIMIT 20;
