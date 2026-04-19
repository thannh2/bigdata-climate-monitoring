# Migration Guide: Sử dụng dữ liệu đã transform

## Thay đổi chính

### 1. Đơn giản hóa pipeline
**Trước**: Bronze → Silver → Gold (normalize, join, build_features, build_targets)
**Sau**: Sử dụng trực tiếp features đã transform từ `Data/extracted features/features/`

### 2. Loại bỏ các module không cần thiết
- ❌ `normalize_weather.py` - Không cần nữa
- ❌ `normalize_air_quality.py` - Không cần nữa  
- ❌ `join_sources.py` - Không cần nữa
- ❌ `build_features.py` - Không cần nữa
- ❌ `build_targets.py` - Không cần nữa
- ✅ `load_features.py` - Module mới để load dữ liệu đã transform

### 3. Mapping tên cột

| Tên cũ (air-quality-ml) | Tên mới (transform_feature.py) |
|--------------------------|----------------------------------|
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

### 4. Features có sẵn

Dữ liệu đã transform có đầy đủ:

**L0 (Raw)**:
- station_id, timestamp, latitude, longitude
- temp_c, humidity, pressure, wind_speed, wind_dir
- precipitation, cloud_cover, shortwave_radiation, soil_temperature
- pm2_5, us_aqi

**L1 (Engineered)**:
- coord_X, coord_Y, coord_Z
- wind_U, wind_V
- air_density, dew_point
- hour_sin, hour_cos

**L2 (Domain)**:
- theta_e
- is_stagnant_air
- cooling_degree_days

**L3 (Time-series)**:
- pressure_delta_3h
- wind_shear_U, wind_shear_V
- temp_mean_6h
- pm25_acc_12h

**L4 (Targets)**:
- target_pm25_[1,6,12,24]h
- target_temp_[1,6,12,24]h
- target_inversion_[1,6,12,24]h
- target_solar_rad_[1,6,12,24]h
- target_hvac_load_[1,6,12,24]h
- target_rain_start_[1,6]h
- target_storm_prob_[12,24]h

### 5. Cấu hình mới

**base.yaml**:
```yaml
data:
  features_path: ../Data/extracted features/features
  gold_predictions_path: ../Data/gold/predictions
  gold_eval_path: ../Data/gold/prediction_eval
  monitoring_path: ../Data/gold/monitoring

split:
  timestamp_col: timestamp  # Thay vì event_hour
  train_end: "2024-12-31 23:00:00"
  val_end: "2025-06-30 23:00:00"
```

### 6. Cách sử dụng

**Load features**:
```python
from air_quality_ml.processing.load_features import load_and_prepare_features

df = load_and_prepare_features(spark, features_path)
```

**Training**:
```python
# Targets có sẵn: target_pm25_1h, target_pm25_6h, etc.
target_col = "target_pm25_1h"

# Thêm alert target nếu cần
from air_quality_ml.processing.load_features import add_alert_target_from_pm25

df = add_alert_target_from_pm25(
    df, 
    pm25_target_col="target_pm25_1h",
    alert_target_col="target_alert_1h",
    threshold=35.0
)
```

### 7. Horizons hỗ trợ

Dữ liệu transform có targets cho horizons: **1h, 6h, 12h, 24h**

Config mặc định trong `base.yaml`:
```yaml
features:
  horizons: [1, 3, 6, 12, 24]
```

**Lưu ý**: Horizon 3h không có trong dữ liệu transform, cần điều chỉnh:
- Sử dụng: [1, 6, 12, 24]
- Hoặc tạo thêm target_pm25_3h bằng interpolation

### 8. Partition

Dữ liệu partition theo: `year/month`

Không partition theo `region` như thiết kế cũ.

### 9. Chạy pipeline

```bash
cd air-quality-ml

# Kiểm tra dữ liệu
python jobs/build_gold_features_targets.py --base-config configs/base.yaml

# Training PM2.5 forecast
python jobs/train_pm25_h1.py
python jobs/train_pm25_h6.py

# Training Alert classifier
python jobs/train_alert_h1.py
python jobs/train_alert_h6.py
```

### 10. Điều chỉnh cần thiết

1. ✅ Cập nhật `settings.py` - DataPaths
2. ✅ Cập nhật `base.yaml` - data paths và timestamp_col
3. ✅ Cập nhật `feature_catalog.py` - tên features
4. ✅ Cập nhật `feature_store.yaml` - danh sách features
5. ✅ Tạo `load_features.py` - load dữ liệu transform
6. ✅ Cập nhật `pipeline_job.py` - sử dụng load_features
7. ✅ Cập nhật `dataset_loader.py` - timestamp thay vì event_hour
8. ⏳ Cập nhật training configs - horizons [1,6,12,24]
9. ⏳ Test training jobs
10. ⏳ Cập nhật docs

## Lợi ích

- ✅ Đơn giản hơn: Bỏ 5 modules processing phức tạp
- ✅ Nhanh hơn: Không cần chạy lại feature engineering
- ✅ Nhất quán: Sử dụng cùng features với processing pipeline
- ✅ Dễ maintain: Ít code hơn, ít bug hơn
