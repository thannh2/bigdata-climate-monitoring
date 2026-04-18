SELECT
    region,
    date_trunc('hour', event_hour) AS hour_ts,
    AVG(pm25) AS avg_pm25,
    MAX(pm25) AS max_pm25,
    COUNT(*) AS station_count
FROM gold_features_hourly
GROUP BY region, date_trunc('hour', event_hour)
ORDER BY hour_ts DESC, avg_pm25 DESC;
