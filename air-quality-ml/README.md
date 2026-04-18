# Air Quality ML (Spark MLlib + MLflow)

Module nay tach rieng phan train/inference/monitoring cho bai toan du bao PM2.5 va canh bao o nhiem.

## Muc tieu pha 1
- Regression PM2.5 cho cac horizon: 1h, 3h, 6h.
- Classification canh bao cho cac horizon: 1h, 3h, 6h.
- Time-based split (khong random split).
- Theo doi va quan tri model bang MLflow.

## Cau truc
- `configs/`: cau hinh data contract, feature, training, monitoring.
- `src/air_quality_ml/`: core package.
- `jobs/`: script train/score/monitor theo horizon.
- `tests/`: unit test co ban.
- `docs/`: tai lieu runbook va mo ta kien truc.

## Cai dat
```bash
cd air-quality-ml
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
pip install -e .
```

## Train mau
```bash
python jobs/train_pm25_h1.py
python jobs/train_alert_h1.py
```

## Batch score mau
```bash
python jobs/batch_score_latest.py
```

## Nguyen tac quan trong
- Khong hard-code nguong alert trong code model, doc tu config.
- Feature chi duoc tao tu du lieu <= thoi diem du bao.
- Moi run train phai log du MLflow params/metrics/artifacts.
- Moi prediction phai co `model_version` va `mlflow_run_id`.

## Bien moi truong thuong dung
- `MLFLOW_TRACKING_URI`: URI tracking server.
- `MLFLOW_EXPERIMENT_ROOT`: prefix ten experiment.
- `MONGO_URI`: ket noi MongoDB cho serving.
- `MONGO_DB`: ten DB mongo.
