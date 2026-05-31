# Runbook

## 1) Cai dat

### Cach 1: chay bang Docker
```bash
docker compose -f ../docker-compose.ml.yml build
docker compose -f ../docker-compose.ml.yml run --rm air-quality-ml python jobs/build_gold_features_targets.py
```

### Cach 2: chay local
```bash
cd air-quality-ml
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
pip install -e .
```

## 2) Chuan bi du lieu
- Nguon input van la du lieu da transform san trong `Data/extracted features/features/`.
- Kiem tra `configs/base.yaml`:
  - `data.features_path`: input parquet hien co trong `Data/`
  - `data.curated_dataset_path`: curated dataset cho train/score
  - `data.gold_predictions_path`: ket qua batch scoring
  - `data.gold_eval_path`: evaluation snapshots
  - `data.monitoring_path`: monitoring snapshots

## 3) Materialize curated dataset
```bash
python jobs/build_gold_features_targets.py
```

Job nay se:
- load transformed features tu `Data/`
- bo sung `target_alert_*h` neu can
- chay data contract validation
- ghi curated dataset theo format cau hinh (`delta` mac dinh)

## 4) Train

### Buoc 4.1: Sinh training configs (1 lan, hoac khi doi target/horizon)
```bash
python jobs/generate_l4_training_configs.py --overwrite
```
Sinh config cho 6 L4 regression target (temp, pm25, cloud_cover, precipitation,
wind_speed, pressure) x 12 horizon + alert classifier 12 horizon, vao
`configs/generated/l4/`.

### Buoc 4.2: Train
```bash
# Train tat ca configs
python jobs/train_all.py

# Hoac loc theo ten config
python jobs/train_all.py --filter pm25     # chi PM2.5
python jobs/train_all.py --filter alert    # chi alert classifier
python jobs/train_all.py --filter h24      # chi horizon 24h

# Hoac train 1 config rieng le
python -m air_quality_ml.training.train_job \
  --base-config configs/base.yaml \
  --job-config configs/generated/l4/pm25_h1.yaml
```

## 5) Batch score
```bash
set MODEL_URI=models:/aq_pm25_forecast_h1/Production
set HORIZON=1
python jobs/batch_score_latest.py
```

Mac dinh, batch score doc tu `data.curated_dataset_path`.

## 6) Monitoring
```bash
python jobs/monitor_daily.py
```

Job nay doc:
- curated dataset
- evaluation snapshots neu da co
- `configs/monitoring_thresholds.yaml`

## 7) Luu y production
- Luon chay `build_gold_features_targets.py` truoc train/score neu curated dataset chua duoc refresh.
- Neu dung Delta Lake, dam bao image/runtime da cai `delta-spark`.
- Kiem tra `MLFLOW_TRACKING_URI` truoc khi train.
- Neu ghi MongoDB, set `MONGO_URI`.
