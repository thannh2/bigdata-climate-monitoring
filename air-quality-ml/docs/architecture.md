# Architecture

## Luong tong quat
1. Input cua khối ML la transformed feature table co san trong `Data/extracted features/features/`.
2. `processing/pipeline_job.py` load parquet tu `Data/`, bo sung `target_alert_*h`, validate data contract va materialize curated ML dataset.
3. Curated dataset duoc ghi ra curated storage (`delta` mac dinh) de phuc vu train, batch score va monitoring.
4. `training/train_job.py` doc curated dataset, time-based split theo `timestamp`, train model MLlib va log vao MLflow.
5. Evaluation snapshots duoc ghi ra `gold_eval_path`.
6. `inference/batch_score.py` doc curated dataset, load model tu MLflow Registry va ghi prediction ra `gold_predictions_path`, co the upsert sang MongoDB.
7. `jobs/monitor_daily.py` doc curated dataset + evaluation snapshots de tao monitoring snapshots.

## Nguyen tac
- Nguon du lieu dau vao van la du lieu co san trong `Data/`.
- Time-based split, khong random split.
- Khong leakage: feature chi dung du lieu <= `timestamp`.
- Data contract chay truoc khi materialize curated dataset.
- Curated dataset la source of truth cho train/score/monitor.
- Prediction log va evaluation snapshot duoc append vao data lake de audit/retrain.
