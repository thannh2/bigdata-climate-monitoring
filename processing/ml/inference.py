import mlflow.spark
import os
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import time

def load_mlflow_model(tracking_uri: str, model_name: str, version: int = 1):
    model_path = f"/workspace/models/{model_name}/version-1/model/sparkml"
    
    # Lấy session đang chạy thay vì khởi tạo mới
    spark = SparkSession.getActiveSession() 
    
    try:
        model = PipelineModel.load(model_path)
        print(f"[OK] Nạp thành công: {model_name}")
        return model
    except Exception as e:
        print(f"[!!!] LỖI NẠP {model_name}: {type(e).__name__} - {e}")
        return None
    
def load_all_forecast_models(tracking_uri: str, existing_models: dict = None):
    """
    Tải toàn bộ bộ sưu tập các model dự đoán cho luồng Streaming.
    Chỉ tải những model chưa có trong existing_models.
    """
    if existing_models is None:
        existing_models = {}
        
    # 1. Định nghĩa các tiền tố của các nhóm mô hình
    target_prefixes = [
        "aq_pm25_forecast",
        "weather_temp_forecast",
        "weather_cloud_cover_forecast",
        "weather_precipitation_forecast",
        "weather_pressure_forecast",
        "weather_wind_speed_forecast"
    ]
    
    # 2. Xác định các khung thời gian dự báo (h1 đến h6)
    forecast_horizons = ["h1", "h2", "h3", "h4", "h5", "h6"]
    
    # 3. Tạo danh sách toàn bộ các tên model cần tải
    all_model_names = [f"{prefix}_{horizon}" for prefix in target_prefixes for horizon in forecast_horizons]
            
    print(f"[*] Bắt đầu kiểm tra và tải các mô hình còn thiếu...")

    # 4. Tải từng mô hình và lưu vào Dictionary
    loaded_count = 0
    
    for name in all_model_names:
        if name not in existing_models:
            model = load_mlflow_model(tracking_uri, name, version=1)
            if model:
                existing_models[name] = model
                loaded_count += 1
                
            time.sleep(1) # Nghỉ 1 giây giữa các lần tải
            
    print(f"[+] Hoàn tất! Đã tải thêm {loaded_count} mô hình. Tổng: {len(existing_models)}/{len(all_model_names)}.")
    
    return existing_models
def apply_ml_predictions(batch_df: DataFrame, ml_models_dict: dict) -> DataFrame:
    if batch_df is None or batch_df.isEmpty():
        return None
    
    # 1. Điền các giá trị mặc định nếu thiếu
    if "region" not in batch_df.columns:
        batch_df = batch_df.withColumn("region", F.lit("Vietnam"))

    if "city" in batch_df.columns:
        batch_df = batch_df.withColumn("city", F.col("city").cast("string"))
    else:
        batch_df = batch_df.withColumn("city", F.col("station_id").cast("string"))

    result_df = batch_df
    count = 0  
    
    junk_cols = [
        "features", "scaledFeatures", 
        "region_idx", "city_idx", 
        "region_ohe", "city_ohe", 
        "rawPrediction", "probability"
    ]

    # 3. Lặp qua 36 models
    for model_name, model in ml_models_dict.items():
        try:
            # Biến đổi trực tiếp
            result_df = model.transform(result_df)
            
            # Kiểm tra xem cột 'prediction' có tồn tại không
            if "prediction" in result_df.columns:
                result_df = result_df.withColumnRenamed("prediction", model_name)
            else:
                original_cols = set(result_df.columns)
                new_cols = [c for c in result_df.columns if c not in original_cols]
                if new_cols:
                    result_df = result_df.withColumnRenamed(new_cols[-1], model_name)
                else:
                    print(f"[!!!] Không tìm thấy kết quả từ {model_name}")
                    result_df = result_df.withColumn(model_name, F.lit(None).cast("double"))

            # Xóa các cột trung gian để chuẩn bị cho model tiếp theo
            for col in junk_cols:
                if col in result_df.columns:
                    result_df = result_df.drop(col)
            
            # Cắt đứt DAG để tránh StackOverflowError
            count += 1
            if count % 6 == 0:
                result_df = result_df.localCheckpoint()
            
        except Exception as e:
            print(f"[!!!] Lỗi dự đoán {model_name}: {e}")
            result_df = result_df.withColumn(model_name, F.lit(None).cast("double"))
            
    return result_df

def load_single_model(tracking_uri: str, model_name: str):
    """Chỉ tải một model cụ thể để debug."""
    return load_mlflow_model(tracking_uri, model_name)
