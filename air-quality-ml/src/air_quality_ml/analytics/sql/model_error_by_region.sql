SELECT
    model_name,
    horizon,
    region,
    AVG(ABS(prediction - actual)) AS mae,
    SQRT(AVG(POWER(prediction - actual, 2))) AS rmse,
    COUNT(*) AS n_samples
FROM gold_prediction_eval
GROUP BY model_name, horizon, region
ORDER BY horizon, mae DESC;
