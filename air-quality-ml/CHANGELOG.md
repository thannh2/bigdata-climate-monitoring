# Changelog

## [2.1.0] - 2026-04-19

### 🧹 Codebase Cleanup

**Removed deprecated/unused modules:**
- ❌ `processing/normalize_weather.py` - Data already normalized
- ❌ `processing/normalize_air_quality.py` - Data already normalized
- ❌ `processing/join_sources.py` - Data already joined
- ❌ `processing/build_features.py` - Features already built
- ❌ `processing/build_targets.py` - Targets already in data
- ❌ `analytics/` - Module not used anywhere
- ❌ `data_contract/` - Module not used anywhere
- ❌ `serving/` - Module not used anywhere
- ❌ `features/lag_features.py` - Not used
- ❌ `features/rolling_features.py` - Not used
- ❌ `features/quality_checks.py` - Not used

**Removed horizon 3h (not in data):**
- ❌ `jobs/train_pm25_h3.py`
- ❌ `jobs/train_alert_h3.py`
- ❌ `configs/training_pm25_h3.yaml`
- ❌ `configs/alert_h3.yaml`
- ❌ `configs/data_contract.yaml`
- ❌ `tests/test_build_targets.py`

**Added:**
- ✅ `.gitignore` - Proper gitignore for Python/ML project
- ✅ Updated README.md - Clear, practical, beginner-friendly

**Changed:**
- 🔄 `settings.py` - Default horizons: [1,3,6] → [1,6,12,24]

**Result:**
- 📉 Removed 25+ unused files
- 📉 Removed 3 unused modules (analytics, data_contract, serving)
- ✅ Cleaner codebase
- ✅ Easier to understand
- ✅ Better documentation

## [2.0.0] - 2026-04-19

### 🎯 Major Refactoring: Simplified Pipeline

Đơn giản hóa air-quality-ml để sử dụng trực tiếp dữ liệu đã transform từ `processing/transform_feature.py`.

### ✅ Added

- `src/air_quality_ml/processing/load_features.py` - Module mới để load dữ liệu đã transform
- `docs/MIGRATION.md` - Hướng dẫn migration chi tiết
- `test_load_features.py` - Script test nhanh
- `CHANGELOG.md` - File này

### ❌ Removed (Deprecated)

Các module processing không còn cần thiết:
- `normalize_weather.py` - Dữ liệu đã được normalize
- `normalize_air_quality.py` - Dữ liệu đã được normalize
- `join_sources.py` - Dữ liệu đã được join
- `build_features.py` - Features đã được tạo sẵn
- `build_targets.py` - Targets đã được tạo sẵn

### 🔄 Changed

**Config changes**:
- `configs/base.yaml`:
  - `data.silver_weather_path` → Removed
  - `data.silver_air_path` → Removed
  - `data.gold_features_path` → Removed
  - `data.gold_targets_path` → Removed
  - `data.features_path` → Added (trỏ đến `Data/extracted features/features`)
  - `split.timestamp_col`: `event_hour` → `timestamp`
  - `split.train_end`: Updated to 2024-12-31
  - `split.val_end`: Updated to 2025-06-30
  - `features.horizons`: [1,3,6,12,24] → [1,6,12,24]

**Code changes**:
- `settings.py`: Simplified DataPaths model
- `feature_catalog.py`: Updated feature names to match transform_feature.py
- `feature_store.yaml`: Updated feature list
- `dataset_loader.py`: Use `timestamp` instead of `event_hour`
- `pipeline_job.py`: Rewritten to use load_features
- `README.md`: Updated documentation

### 📊 Feature Mapping

| Old Name | New Name |
|----------|----------|
| event_hour | timestamp |
| temperature_c | temp_c |
| pressure_hpa | pressure |
| wind_speed_mps | wind_speed |
| wind_direction_deg | wind_dir |
| precipitation_mm | precipitation |
| cloud_cover_pct | cloud_cover |
| shortwave_radiation_wm2 | shortwave_radiation |
| soil_temperature_0_to_7cm_c | soil_temperature |
| pm25 | pm2_5 |
| aqi | us_aqi |
| coord_x/y/z | coord_X/Y/Z |
| wind_u/v | wind_U/V |

### 🎯 Horizons

Supported horizons: **1h, 6h, 12h, 24h**

Note: Horizon 3h không có trong dữ liệu transform, đã loại bỏ khỏi config.

### 📦 Data Structure

**Input**: `Data/extracted features/features/`
- Partition: `year=*/month=*/`
- Format: Parquet (snappy)
- Features: L0 + L1 + L2 + L3 + L4 (đầy đủ)

**Output**:
- `Data/gold/predictions/` - Prediction results
- `Data/gold/prediction_eval/` - Evaluation metrics
- `Data/gold/monitoring/` - Monitoring data

### 🚀 Benefits

1. **Đơn giản hơn**: Giảm từ 7 modules xuống 1 module
2. **Nhanh hơn**: Không cần chạy lại feature engineering
3. **Nhất quán**: Sử dụng cùng features với processing pipeline
4. **Dễ maintain**: Ít code hơn, ít bug hơn
5. **Rõ ràng hơn**: Pipeline flow đơn giản: Load → Train → Predict

### 📝 Migration Steps

1. ✅ Update `settings.py` - DataPaths model
2. ✅ Update `base.yaml` - data paths và split config
3. ✅ Update `feature_catalog.py` - feature names
4. ✅ Update `feature_store.yaml` - feature list
5. ✅ Create `load_features.py` - new module
6. ✅ Update `pipeline_job.py` - use load_features
7. ✅ Update `dataset_loader.py` - timestamp column
8. ✅ Update `README.md` - documentation
9. ⏳ Test training jobs
10. ⏳ Update inference jobs

### ⚠️ Breaking Changes

- Config file format changed (data paths)
- Feature names changed (see mapping table)
- Timestamp column changed: `event_hour` → `timestamp`
- Horizons changed: removed 3h
- Pipeline flow changed: no more normalize/join/build steps

### 🔜 Next Steps

1. Test training jobs với dữ liệu mới
2. Validate model performance
3. Update inference jobs
4. Update monitoring jobs
5. Update documentation
6. Archive old processing modules

---

## [1.0.0] - 2026-04-01

### Initial Release

- Bronze → Silver → Gold pipeline
- Feature engineering (L0 → L4)
- Training với MLlib
- MLflow integration
- Monitoring modules
