from pyspark.sql import functions as F
from pyspark.sql.window import Window

def apply_l4_targets(df):
    """Tính toán Targets phục vụ Huấn luyện AI"""
    window_spec = Window.partitionBy("station_id").orderBy("timestamp")
    target_hours = list(range(1, 25)) 
    l4_expressions = []
    
    for n in target_hours:
        l4_expressions.extend([
            F.lead("temp_c", n).over(window_spec).alias(f"target_temp_{n}h"),
            F.lead("pm2_5", n).over(window_spec).alias(f"target_pm25_{n}h"),
            F.lead("is_stagnant_air", n).over(window_spec).alias(f"target_inversion_{n}h"),
            F.lead("shortwave_radiation", n).over(window_spec).alias(f"target_solar_rad_{n}h"),
            F.lead("cooling_degree_days", n).over(window_spec).alias(f"target_hvac_load_{n}h"),
            F.lead("cloud_cover", n).over(window_spec).alias(f"target_cloud_cover_{n}h"),
            F.lead("precipitation", n).over(window_spec).alias(f"target_precipitation_{n}h"),
            F.lead("wind_speed", n).over(window_spec).alias(f"target_wind_speed_{n}h"),
            F.lead("wind_U", n).over(window_spec).alias(f"target_wind_U_{n}h"),
            F.lead("wind_V", n).over(window_spec).alias(f"target_wind_V_{n}h"),
            F.lead("pressure", n).over(window_spec).alias(f"target_pressure_{n}h")
        ])
        if n <= 6:
            rain_cond = F.when((F.lead("precipitation", n).over(window_spec) > 0) & (F.col("precipitation") == 0), 1).otherwise(0)
            l4_expressions.append(rain_cond.alias(f"target_rain_start_{n}h"))
        if n >= 9:
            # Kéo các thông số tương lai về
            future_wind = F.lead("wind_speed", n).over(window_spec)
            future_delta_p = F.lead("pressure_delta_3h", n).over(window_spec)
            future_precip = F.lead("precipitation", n).over(window_spec)
            future_pressure = F.lead("pressure", n).over(window_spec)
            
            # Gán nhãn 1 (Có bão) khi thỏa mãn ĐỒNG THỜI 4 yếu tố vật lý
            storm_cond = F.when(
                (future_wind > 15) &              # 1. Gió mạnh (Cấp 7 trở lên)
                (future_delta_p < -3) &           # 2. Sụt áp nhanh (Dấu hiệu bão tới)
                (future_pressure < 1005) &        # 3. Nằm trong vùng áp thấp
                (future_precip > 2.0),            # 4. Có mưa vừa đến mưa to
                1
            ).otherwise(0)
            
            l4_expressions.append(storm_cond.alias(f"target_storm_prob_{n}h"))

    df = df.select("*", *l4_expressions)
    return df.dropna(subset=[f"target_temp_{target_hours[-1]}h"])
