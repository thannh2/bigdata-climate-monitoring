# Windows Fix - Changelog

## Vấn đề
Lỗi `UnsatisfiedLinkError: NativeIO$Windows.access0` khi chạy Spark trên Windows

## Giải pháp đã áp dụng

### 1. File: `src/air_quality_ml/utils/spark.py`
- Tự động set `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON`
- Disable Hadoop native library
- Cấu hình `RawLocalFileSystem` cho Windows

### 2. File: `src/air_quality_ml/processing/load_features.py`
- Tự động phát hiện Windows (`os.name == 'nt'`)
- Dùng pandas để đọc parquet files thay vì Spark native reader
- Hỗ trợ đọc recursive cho partitioned data (year=2021, year=2022, etc.)

## Kết quả
✅ Code hoạt động trên Windows mà không cần cài đặt Hadoop phức tạp
✅ Tự động chuyển sang pandas workaround trên Windows
✅ Vẫn dùng Spark native reader trên Linux/Mac (hiệu suất tốt hơn)

## Test
```bash
cd air-quality-ml
python test_load_features.py
```
