# Architecture

## Luong tong quat
1. Silver weather + silver air quality duoc normalize.
2. Join theo (`station_id`, `event_hour`) va deduplicate theo `ingestion_time` moi nhat.
3. Tao feature lag/rolling/cyclic trong `processing/build_features.py`.
4. Tao target PM2.5 va alert theo horizon trong `processing/build_targets.py`.
5. Train model theo horizon trong `training/train_job.py`.
6. Log metrics/artifacts/model vao MLflow.
7. Batch score trong `inference/batch_score.py`.
8. Monitoring data quality + drift + performance trong `jobs/monitor_daily.py` va module `monitoring/`.

## Nguyen tac
- Time-based split, khong random split.
- Khong leakage: feature chi dung du lieu <= t.
- Khong hard-code threshold alert, threshold den tu config.
- Prediction log append vao data lake de audit/retrain.
