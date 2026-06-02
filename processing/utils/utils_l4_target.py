from pyspark.sql import functions as F
from pyspark.sql.window import Window

def apply_l4_targets(df):
    """Tính toán Targets phục vụ Huấn luyện AI cho các đặc trưng được chọn (Khối màu xanh)"""
    window_spec = Window.partitionBy("station_id").orderBy("timestamp")
    
    # 1. Cập nhật danh sách khung giờ mục tiêu
    target_hours = [1, 2, 3, 4, 5, 6, 9, 15, 24]
    l4_expressions = []
    
    for n in target_hours:
        # 2. Chỉ giữ lại các đặc trưng được tô màu xanh
        l4_expressions.extend([
            F.lead("temp_c", n).over(window_spec).alias(f"target_temp_{n}h"),
            F.lead("pm2_5", n).over(window_spec).alias(f"target_pm25_{n}h"),
            F.lead("cloud_cover", n).over(window_spec).alias(f"target_cloud_cover_{n}h"),
            F.lead("precipitation", n).over(window_spec).alias(f"target_precipitation_{n}h"),
            F.lead("wind_speed", n).over(window_spec).alias(f"target_wind_speed_{n}h"),
            F.lead("pressure", n).over(window_spec).alias(f"target_pressure_{n}h")
        ])

    df = df.select("*", *l4_expressions)
    
    # 3. Loại bỏ các giá trị Null dựa trên khung giờ xa nhất (24h) của một trường cơ sở
    return df.dropna(subset=[f"target_temp_{target_hours[-1]}h"])