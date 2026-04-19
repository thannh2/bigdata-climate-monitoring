# Quick Start Guide

## 🚀 Bắt đầu nhanh với air-quality-ml

### Bước 1: Cài đặt

```bash
cd air-quality-ml

# Tạo virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -e .
```

### Bước 2: Kiểm tra dữ liệu

```bash
# Test load features
python test_load_features.py
```

Output mong đợi:
```
✅ Loaded successfully!
   - Total rows: 262,800
   - Total columns: 50+
   
AVAILABLE TARGETS:
   - target_pm25_1h: 262,000+ non-null values
   - target_pm25_6h: 261,000+ non-null values
   - target_pm25_12h: 260,000+ non-null values
   - target_pm25_24h: 258,000+ non-null values
```

### Bước 3: Training đầu tiên

```bash
# Train PM2.5 forecast cho horizon 1h
python jobs/train_pm25_h1.py
```

Output mong đợi:
```
[*] Loading features from: ../Data/extracted features/features
[*] Total rows: 262,800
[*] Train/Val/Test split: 180,000 / 50,000 / 32,800
[*] Training GBTRegressor...
[*] Evaluation metrics:
    - RMSE: 12.5
    - MAE: 8.3
    - R2: 0.85
[*] Model registered: aq_pm25_forecast_h1
```

### Bước 4: Xem kết quả trong MLflow

```bash
# Start MLflow UI (nếu chưa chạy)
mlflow ui --port 5000
```

Mở browser: http://localhost:5000

### Bước 5: Training thêm models

```bash
# PM2.5 forecast cho các horizons khác
python jobs/train_pm25_h6.py
python jobs/train_pm25_h12.py
python jobs/train_pm25_h24.py

# Alert classifier
python jobs/train_alert_h1.py
python jobs/train_alert_h6.py
```

## 📊 Hiểu về dữ liệu

### Cấu trúc dữ liệu

```
Data/extracted features/features/
├── year=2021/
│   ├── month=4/
│   ├── month=5/
│   └── ...
├── year=2022/
├── year=2023/
├── year=2024/
├── year=2025/
└── year=2026/
```

### Features có sẵn

**L0 (Raw - 13 features)**:
- Khí tượng: temp_c, humidity, pressure, wind_speed, wind_dir, precipitation, cloud_cover, shortwave_radiation, soil_temperature
- Không gian: latitude, longitude
- Chất lượng không khí: pm2_5, us_aqi

**L1 (Engineered - 9 features)**:
- Không gian: coord_X, coord_Y, coord_Z
- Gió: wind_U, wind_V
- Khí tượng: air_density, dew_point
- Thời gian: hour_sin, hour_cos

**L2 (Domain - 3 features)**:
- theta_e (equivalent potential temperature)
- is_stagnant_air (điều kiện tù đọng)
- cooling_degree_days

**L3 (Time-series - 5 features)**:
- pressure_delta_3h
- wind_shear_U, wind_shear_V
- temp_mean_6h
- pm25_acc_12h

**L4 (Targets - 20+ features)**:
- target_pm25_[1,6,12,24]h
- target_temp_[1,6,12,24]h
- target_inversion_[1,6,12,24]h
- target_solar_rad_[1,6,12,24]h
- target_hvac_load_[1,6,12,24]h
- target_rain_start_[1,6]h
- target_storm_prob_[12,24]h

### Time range

- **Start**: 2021-04-16
- **End**: 2026-04-15
- **Total**: ~5 years
- **Frequency**: Hourly

### Stations

- Hanoi (bac)
- Hai Phong (bac)
- Hue (trung)
- Da Nang (trung)
- HCMC (nam)
- Can Tho (nam)

## 🎯 Training Strategy

### Time-based Split

```yaml
split:
  timestamp_col: timestamp
  train_end: "2024-12-31 23:00:00"    # ~3.7 years
  val_end: "2025-06-30 23:00:00"      # ~6 months
  # test: 2025-07-01 onwards           # ~9 months
```

### Horizons

- **1h**: Short-term forecast (immediate)
- **6h**: Medium-term forecast (half day)
- **12h**: Medium-term forecast (half day)
- **24h**: Long-term forecast (next day)

### Models

**Regression (PM2.5 forecast)**:
- Algorithm: GBTRegressor
- Metrics: RMSE, MAE, R2
- Target: Minimize MAE

**Classification (Alert)**:
- Algorithm: GBTClassifier
- Metrics: AUC-ROC, AUPRC, Recall, Precision
- Target: Maximize Recall (minimize false negatives)

## 🔧 Troubleshooting

### Lỗi: ModuleNotFoundError

```bash
# Đảm bảo đã install package
pip install -e .
```

### Lỗi: FileNotFoundError

```bash
# Kiểm tra path trong configs/base.yaml
# Đảm bảo features_path trỏ đúng:
data:
  features_path: ../Data/extracted features/features
```

### Lỗi: Spark out of memory

```bash
# Tăng driver memory trong configs/base.yaml
spark:
  driver_memory: 8g  # Tăng từ 4g
```

### MLflow không kết nối được

```bash
# Start MLflow tracking server
mlflow server --host 0.0.0.0 --port 5000

# Hoặc dùng local file store
export MLFLOW_TRACKING_URI=file:///path/to/mlruns
```

## 📚 Tài liệu thêm

- [README.md](README.md) - Tổng quan dự án
- [docs/MIGRATION.md](docs/MIGRATION.md) - Chi tiết migration
- [docs/architecture.md](docs/architecture.md) - Kiến trúc
- [docs/feature_catalog.md](docs/feature_catalog.md) - Features
- [docs/runbook.md](docs/runbook.md) - Vận hành

## 🎉 Hoàn thành!

Bạn đã sẵn sàng train models và dự báo PM2.5!

Câu hỏi? Xem [docs/](docs/) hoặc mở issue.
