# Windows Setup

## Lỗi Hadoop trên Windows

Khi chạy Spark trên Windows, bạn có thể gặp lỗi:
```
UnsatisfiedLinkError: NativeIO$Windows.access0
```

## Giải pháp

Code đã được fix tự động cho Windows:

1. **`src/air_quality_ml/utils/spark.py`** - Tự động cấu hình Spark cho Windows
2. **`src/air_quality_ml/processing/load_features.py`** - Tự động dùng pandas để đọc parquet trên Windows

## Chạy test

```bash
cd air-quality-ml
python test_load_features.py
```

Code sẽ tự động phát hiện Windows và sử dụng pandas workaround.
