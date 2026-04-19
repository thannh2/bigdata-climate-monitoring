# Air Quality ML (Spark MLlib + MLflow)

Module training/inference/monitoring cho bài toán dự báo PM2.5 và cảnh báo ô nhiễm.

## ⚡ Thay đổi quan trọng

**Dự án đã được đơn giản hóa** để sử dụng trực tiếp dữ liệu đã transform từ `processing/transform_feature.py`.

- ✅ Không cần chạy pipeline normalize/join/build_features nữa
- ✅ Dữ liệu features đã sẵn sàng tại `Data/extracted features/features/`
- ✅ Chỉ cần load và train

Xem chi tiết: [docs/MIGRATION.md](docs/MIGRATION.md)

## Mục tiêu

- **Regression PM2.5**: Dự báo nồng độ PM2.5 cho horizons: 1h, 6h, 12h, 24h
- **Classification Alert**: Phân loại cảnh báo ô nhiễm cho horizons: 1h, 6h, 12h, 24h
- **Time-based split**: Train/Val/Test theo thời gian
- **MLflow tracking**: Quản lý experiments và model registry

## Cấu trúc

```
air-quality-ml/
├── configs/          # Cấu hình YAML
│   ├── base.yaml                 # Config chung
│   ├── training_pm25_h1.yaml     # Config training PM2.5 1h
│   ├── training_pm25_h6.yaml     # Config training PM2.5 6h
│   ├── alert_h1.yaml             # Config training Alert 1h
│   └── ...
├── src/air_quality_ml/
│   ├── processing/
│   │   └── load_features.py      # Load dữ liệu đã transform
│   ├── features/
│   │   └── feature_catalog.py    # Danh sách features
│   ├── training/
│   │   ├── dataset_loader.py
│   │   ├── splitters.py
│   │   ├── regression.py
│   │   ├── classification.py
│   │   └── train_job.py
│   ├── inference/
│   │   └── batch_score.py
│   ├── monitoring/
│   │   ├── data_quality.py
│   │   ├── drift.py
│   │   └── performance.py
│   └── mlflow_tracking/
│       ├── tracking.py
│       └── registry.py
├── jobs/             # Entry point scripts
│   ├── train_pm25_h1.py
│   ├── train_pm25_h6.py
│   ├── train_alert_h1.py
│   └── batch_score_latest.py
├── tests/            # Unit tests
└── docs/             # Tài liệu
```

## Cài đặt

```bash
cd air-quality-ml
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

## Sử dụng

### 1. Kiểm tra dữ liệu

```bash
python jobs/build_gold_features_targets.py --base-config configs/base.yaml
```

Output sẽ hiển thị schema và sample data.

### 2. Training PM2.5 Forecast

```bash
# Horizon 1h
python jobs/train_pm25_h1.py

# Horizon 6h
python jobs/train_pm25_h6.py

# Horizon 12h
python jobs/train_pm25_h12.py

# Horizon 24h
python jobs/train_pm25_h24.py
```

### 3. Training Alert Classifier

```bash
# Horizon 1h
python jobs/train_alert_h1.py

# Horizon 6h
python jobs/train_alert_h6.py
```

### 4. Batch Scoring

```bash
set MODEL_URI=models:/aq_pm25_forecast_h1/Production
set HORIZON=1
python jobs/batch_score_latest.py
```

### 5. Monitoring

```bash
python jobs/monitor_daily.py
```

## Features có sẵn

Dữ liệu đã transform có đầy đủ features theo 4 lớp:

**L0 (Raw)**: temp_c, humidity, pressure, wind_speed, pm2_5, us_aqi, ...

**L1 (Engineered)**: coord_X/Y/Z, wind_U/V, dew_point, hour_sin/cos, ...

**L2 (Domain)**: theta_e, is_stagnant_air, cooling_degree_days

**L3 (Time-series)**: pressure_delta_3h, wind_shear_U/V, temp_mean_6h, pm25_acc_12h

**L4 (Targets)**: target_pm25_[1,6,12,24]h, target_temp_*, target_inversion_*, ...

## Horizons hỗ trợ

- ✅ 1h
- ✅ 6h  
- ✅ 12h
- ✅ 24h

## Nguyên tắc quan trọng

- ✅ Time-based split (không random)
- ✅ Không leakage: features chỉ dùng dữ liệu ≤ t
- ✅ Threshold alert đọc từ config
- ✅ Mọi run train phải log MLflow
- ✅ Mọi prediction phải có model_version và mlflow_run_id

## Biến môi trường

```bash
# MLflow
set MLFLOW_TRACKING_URI=http://localhost:5000
set MLFLOW_EXPERIMENT_ROOT=air-quality

# MongoDB (cho serving)
set MONGO_URI=mongodb://localhost:27017
set MONGO_DB=air_quality
```

## Xem thêm

- [docs/architecture.md](docs/architecture.md) - Kiến trúc hệ thống
- [docs/feature_catalog.md](docs/feature_catalog.md) - Danh sách features
- [docs/runbook.md](docs/runbook.md) - Hướng dẫn vận hành
- [docs/MIGRATION.md](docs/MIGRATION.md) - Migration guide
