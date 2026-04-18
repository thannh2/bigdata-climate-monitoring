# Runbook

## 1) Cai dat
```bash
cd air-quality-ml
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
pip install -e .
```

## 2) Chuan bi du lieu
- Dat parquet tai duong dan trong `configs/base.yaml`:
  - `data.gold_targets_path` phai chua feature + target theo horizon.

## 3) Train
```bash
python jobs/train_pm25_h1.py
python jobs/train_pm25_h3.py
python jobs/train_pm25_h6.py
python jobs/train_alert_h1.py
python jobs/train_alert_h3.py
python jobs/train_alert_h6.py
```

## 4) Batch score
```bash
set MODEL_URI=models:/aq_pm25_forecast_h1/Production
set HORIZON=1
python jobs/batch_score_latest.py
```

## 5) Monitoring
```bash
python jobs/monitor_daily.py
```

## 6) Luu y production
- Kiem tra split time co du train/val/test.
- Kiem tra `MLFLOW_TRACKING_URI` truoc khi train.
- Neu ghi MongoDB, set `MONGO_URI`.
