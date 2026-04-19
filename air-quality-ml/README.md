# Air Quality ML - PM2.5 Forecasting System

Hệ thống Machine Learning dự báo nồng độ PM2.5 và phát cảnh báo ô nhiễm không khí.

## 🎯 Mục đích

Dự báo nồng độ PM2.5 và phát cảnh báo ô nhiễm không khí cho các thành phố lớn tại Việt Nam với 4 horizons: 1h, 6h, 12h, 24h.

## 📁 Cấu trúc thư mục

```
air-quality-ml/
├── configs/                    # Cấu hình YAML
│   ├── base.yaml              # Config chung (paths, spark, mlflow, split)
│   ├── training_pm25_h*.yaml  # Config training PM2.5 regression
│   ├── alert_h*.yaml          # Config training Alert classification
│   ├── feature_store.yaml     # Danh sách features
│   └── monitoring_thresholds.yaml
│
├── jobs/                       # Entry point scripts
│   ├── train_pm25_h1.py       # Train PM2.5 forecast 1h
│   ├── train_pm25_h6.py       # Train PM2.5 forecast 6h
│   ├── train_pm25_h12.py      # Train PM2.5 forecast 12h
│   ├── train_pm25_h24.py      # Train PM2.5 forecast 24h
│   ├── train_alert_h1.py      # Train Alert classifier 1h
│   ├── train_alert_h6.py      # Train Alert classifier 6h
│   ├── train_alert_h12.py     # Train Alert classifier 12h
│   ├── train_alert_h24.py     # Train Alert classifier 24h
│   ├── batch_score_latest.py  # Batch scoring
│   ├── monitor_daily.py       # Daily monitoring
│   └── build_gold_features_targets.py  # Build curated ML dataset
│
├── src/air_quality_ml/
│   ├── processing/            # Data loading
│   │   ├── load_features.py   # Load features từ parquet
│   │   ├── write_gold_tables.py
│   │   └── pipeline_job.py
│   │
│   ├── features/              # Feature catalog
│   │   └── feature_catalog.py # Danh sách features theo layer
│   │
│   ├── training/              # Model training
│   │   ├── dataset_loader.py  # Load & prepare data
│   │   ├── splitters.py       # Time-based split
│   │   ├── regression.py      # PM2.5 regression
│   │   ├── classification.py  # Alert classification
│   │   ├── evaluate_*.py      # Evaluation metrics
│   │   ├── thresholding.py    # Threshold optimization
│   │   ├── registry.py        # Model registry
│   │   └── train_job.py       # Main training orchestrator
│   │
│   ├── inference/             # Model inference
│   │   ├── batch_score.py     # Batch scoring
│   │   ├── stream_score.py    # Stream scoring
│   │   ├── postprocess_alerts.py
│   │   └── writer_mongodb.py
│   │
│   ├── monitoring/            # Monitoring
│   │   ├── data_quality.py    # Data quality checks
│   │   ├── drift.py           # Data drift detection
│   │   └── performance.py     # Model performance
│   │
│   ├── data_contract/         # Data contract validation
│   │   ├── schemas.py
│   │   └── validators.py
│   │
│   ├── mlflow_tracking/       # MLflow integration
│   │   ├── tracking.py        # Experiment tracking
│   │   ├── registry.py        # Model registry
│   │   ├── artifacts.py       # Artifact logging
│   │   └── signatures.py      # Model signatures
│   │
│   ├── utils/                 # Utilities
│   │   ├── spark.py           # Spark session (Windows compatible)
│   │   ├── parquet_io.py      # Parquet I/O (Windows compatible)
│   │   ├── logger.py          # Logging
│   │   ├── io.py              # File I/O
│   │   └── time.py            # Time utilities
│   │
│   └── settings.py            # Settings & config models
│
├── tests/                     # Unit tests
│   ├── conftest.py
│   ├── test_drift.py
│   └── test_splitters.py
│
├── docs/                      # Documentation
│   ├── architecture.md
│   ├── feature_catalog.md
│   ├── runbook.md
│   └── MIGRATION.md
│
├── test_load_features.py      # Quick test script
├── README.md                  # This file
├── QUICKSTART.md             # Quick start guide
├── CHANGELOG.md              # Version history
├── requirements.txt          # Python dependencies
└── pyproject.toml            # Package config
```

## 🚀 Cài đặt

### 1. Tạo virtual environment

```bash
cd air-quality-ml
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

### 2. Cài đặt dependencies

```bash
pip install -r requirements.txt
pip install -e .
```

### 3. Kiểm tra cài đặt

```bash
python test_load_features.py
```

## 📊 Dữ liệu đầu vào

### Nguồn dữ liệu

Dữ liệu features đã được transform sẵn từ `processing/transform_feature.py`:

```
../Data/extracted features/features/
├── year=2021/
├── year=2022/
├── year=2023/
├── year=2024/
├── year=2025/
└── year=2026/
```

**Thông tin:**
- Time range: 2021-04-16 đến 2026-04-15 (~5 năm)
- Frequency: Hourly
- Total rows: ~262,800
- Format: Parquet (partitioned by year)

Pipeline hien tai van lay du lieu dau vao tu `Data/`, sau do materialize sang curated dataset de train/score/monitor.

### Features (50+ features)

**L0 - Raw (13):** temp_c, humidity, pressure, wind_speed, pm2_5, us_aqi, ...  
**L1 - Engineered (9):** coord_X/Y/Z, wind_U/V, hour_sin/cos, ...  
**L2 - Domain (3):** theta_e, is_stagnant_air, cooling_degree_days  
**L3 - Time-series (5+):** pressure_delta_3h, temp_mean_6h, pm25_acc_12h, ...  
**L4 - Targets (20+):** target_pm25_[1,6,12,24]h, target_temp_*, ...

Chi tiết: [docs/feature_catalog.md](docs/feature_catalog.md)

## 🎯 Training

### Time-based Split

```yaml
train: 2021-04-16 → 2024-12-31  (~3.7 years)
val:   2025-01-01 → 2025-06-30  (~6 months)
test:  2025-07-01 → 2026-04-15  (~9 months)
```

### Chạy training

```bash
# Materialize curated dataset truoc khi train
python jobs/build_gold_features_targets.py

# PM2.5 Regression
python jobs/train_pm25_h1.py   # 1 hour ahead
python jobs/train_pm25_h6.py   # 6 hours ahead
python jobs/train_pm25_h12.py  # 12 hours ahead
python jobs/train_pm25_h24.py  # 24 hours ahead

# Alert Classification
python jobs/train_alert_h1.py  # 1 hour ahead
python jobs/train_alert_h6.py  # 6 hours ahead
python jobs/train_alert_h12.py # 12 hours ahead
python jobs/train_alert_h24.py # 24 hours ahead
```

### Xem kết quả trong MLflow

```bash
mlflow ui --port 5000
```

Mở browser: http://localhost:5000

## 🔮 Inference

### Batch Scoring

```bash
set MODEL_URI=models:/aq_pm25_forecast_h1/Production
set HORIZON=1
python jobs/batch_score_latest.py
```

## 📈 Monitoring

```bash
python jobs/monitor_daily.py
```

## 🪟 Windows Support

Code tự động phát hiện Windows và sử dụng pandas workaround để tránh lỗi Hadoop native library.

**Không cần cài đặt gì thêm!** Code sẽ tự động hoạt động trên Windows.

## ⚙️ Configuration

### Base Config (`configs/base.yaml`)

```yaml
data:
  features_path: ../Data/extracted features/features
  curated_dataset_path: ../Data/gold/curated_ml_dataset
  gold_predictions_path: ../Data/gold/predictions
  gold_eval_path: ../Data/gold/prediction_eval
  monitoring_path: ../Data/gold/monitoring

spark:
  app_name: air-quality-ml
  master: local[*]
  shuffle_partitions: 16

mlflow:
  tracking_uri: http://localhost:5000
  experiment_root: air-quality

split:
  timestamp_col: timestamp
  train_end: "2024-12-31 23:00:00"
  val_end: "2025-06-30 23:00:00"

features:
  horizons: [1, 6, 12, 24]
  alert_pm25_threshold: 35.0
```

### Training Config (`configs/training_pm25_h1.yaml`)

```yaml
model:
  type: regression
  algorithm: gbt
  target_col: target_pm25_1h
  max_iter: 100
  max_depth: 5
```

## 🧪 Testing

```bash
# Test load features
python test_load_features.py

# Build curated dataset
python jobs/build_gold_features_targets.py

# Run unit tests
pytest tests/

# Test training
python jobs/train_pm25_h1.py
```

## 📚 Documentation

- [QUICKSTART.md](QUICKSTART.md) - Quick start guide
- [CHANGELOG.md](CHANGELOG.md) - Version history
- [docs/architecture.md](docs/architecture.md) - System architecture
- [docs/feature_catalog.md](docs/feature_catalog.md) - Feature documentation
- [docs/runbook.md](docs/runbook.md) - Operations runbook

## 🐳 Docker

```bash
docker compose -f ../docker-compose.ml.yml build
docker compose -f ../docker-compose.ml.yml run --rm air-quality-ml python jobs/build_gold_features_targets.py
```

## 🐛 Troubleshooting

### Lỗi: ModuleNotFoundError
```bash
pip install -e .
```

### Lỗi: FileNotFoundError
Kiểm tra `features_path` trong `configs/base.yaml` trỏ đúng:
```yaml
data:
  features_path: ../Data/extracted features/features
```

### Lỗi: Spark chạy chậm hoặc thiếu tài nguyên
Điều chỉnh Spark runtime qua:
- `spark.master`
- `spark.shuffle_partitions`
- tham số tài nguyên của container hoặc Spark submit

```yaml
spark:
  master: local[*]
  shuffle_partitions: 32
```

## 📝 Các file/thư mục không cần commit

- `__pycache__/` - Python cache
- `*.pyc` - Compiled Python
- `.venv/` - Virtual environment
- `mlruns/` - MLflow artifacts
- `data/`, `Data/` - Data files
- `*.log` - Log files
- `metastore_db/`, `spark-warehouse/` - Spark metadata

Xem `.gitignore` để biết chi tiết.

## 🎯 Entry Points quan trọng

| Script | Mục đích |
|--------|----------|
| `jobs/train_pm25_h1.py` | Train PM2.5 forecast 1h |
| `jobs/train_alert_h1.py` | Train Alert classifier 1h |
| `jobs/batch_score_latest.py` | Batch scoring |
| `jobs/monitor_daily.py` | Daily monitoring |
| `test_load_features.py` | Test load features |

## 📧 Support

Questions? Check [docs/](docs/) or open an issue.

---

**Version:** 2.1.0  
**Last Updated:** 2026-04-19
