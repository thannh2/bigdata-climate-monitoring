from pyspark.sql import functions as F

def apply_wmo_rules(df):
    """
    Luật Hậu xử lý (Post-processing) dựa trên WMO và EPA.
    Nhận đầu vào là các cột dự báo (pred_*) và xuất ra các mã trạng thái (Int).
    """
    # =====================================================================
    # 1. NHIỆT ĐỘ (Temp_Code) 
    # =====================================================================
    # Phân loại nắng nóng và rét đậm, rét hại
    df = df.withColumn("Temp_Code", 
        F.when(F.col("pred_temp") >= 40.0, 6)      # Nắng nóng ĐB gay gắt
         .when(F.col("pred_temp") >= 37.0, 5)      # Nắng nóng gay gắt
         .when(F.col("pred_temp") >= 35.0, 4)      # Nắng nóng
         .when(F.col("pred_temp") <= 13.0, 3)      # Rét hại
         .when(F.col("pred_temp") <= 15.0, 2)      # Rét đậm
         .otherwise(1)                             # 15.1 -> 34.9: Bình thường
    )

    # =====================================================================
    # 2. LƯỢNG MƯA (Rain_Code)
    # =====================================================================
    # Phân loại cường độ mưa
    df = df.withColumn("Rain_Code",
        F.when(F.col("pred_precipitation") > 50.0, 5)        # Mưa rất to (Cảnh báo lũ/sạt lở)
         .when(F.col("pred_precipitation") >= 7.6, 4)        # Mưa to (Cảnh báo ngập đô thị)
         .when(F.col("pred_precipitation") >= 2.6, 3)        # Mưa vừa
         .when(F.col("pred_precipitation") >= 0.1, 2)        # Mưa nhỏ / Phùn
         .otherwise(1)                                       # Không mưa
    )

    # =====================================================================
    # 3. TỐC ĐỘ GIÓ (Wind_Code)
    # =====================================================================
    # Áp dụng Thang sức gió Beaufort
    df = df.withColumn("Wind_Code",
        F.when(F.col("pred_wind_speed") > 20.7, 5)         # > Cấp 9: Bão mạnh/Siêu bão
         .when(F.col("pred_wind_speed") >= 17.2, 4)        # Cấp 8: Gió bão nhẹ
         .when(F.col("pred_wind_speed") >= 13.9, 3)        # Cấp 7: Gió rất mạnh
         .when(F.col("pred_wind_speed") >= 10.8, 2)        # Cấp 6: Gió mạnh (Biển động)
         .otherwise(1)                                     # < Cấp 6: Bình thường
    )

    # =====================================================================
    # 4. TỶ LỆ MÂY (Cloud_Code)
    # =====================================================================
    # Trạng thái bầu trời (dùng để map icon UI)
    df = df.withColumn("Cloud_Code",
        F.when(F.col("pred_cloud_cover") > 70.0, 4)        # U ám (Khả năng mưa)
         .when(F.col("pred_cloud_cover") >= 41.0, 3)       # Nhiều mây (Râm mát)
         .when(F.col("pred_cloud_cover") >= 11.0, 2)       # Ít mây / Mây rải rác
         .otherwise(1)                                     # Quang mây (Nắng)
    )

    # =====================================================================
    # 5. CHẤT LƯỢNG KHÔNG KHÍ (PM25_Code)
    # =====================================================================
    # Theo chuẩn EPA (Mỹ)
    if "pred_pm25" in df.columns:
        df = df.withColumn("PM25_Code",
            F.when(F.col("pred_pm25") > 150.4, 5)          # Rất xấu/Nguy hại (Tím/Nâu)
             .when(F.col("pred_pm25") >= 55.5, 4)          # Xấu (Đỏ)
             .when(F.col("pred_pm25") >= 35.5, 3)          # Kém (Cam)
             .when(F.col("pred_pm25") >= 12.1, 2)          # Trung bình (Vàng)
             .otherwise(1)                                 # Tốt (Xanh lá)
        )

    # =====================================================================
    # 6. BỨC XẠ MẶT TRỜI & NĂNG LƯỢNG (Solar_Code)
    # =====================================================================
    if "pred_solar_rad" in df.columns:
        df = df.withColumn("Solar_Code",
            F.when(F.col("pred_solar_rad") > 0, 1)         # Ban ngày / Có phát điện
             .otherwise(0)                                 # Ban đêm
        )

    # =====================================================================
    # 7. CẢNH BÁO TỔNG HỢP / XÁC SUẤT (Extreme_Alert)
    # =====================================================================
    # 7A. Dông lốc: Kết hợp lượng mưa và gió mạnh từ L4 liên tục
    df = df.withColumn("Alert_Thunderstorm",
        F.when((F.col("pred_precipitation") > 0) & (F.col("pred_wind_speed") >= 10.8), 1)
         .otherwise(0)
    )

    # 7B. Xử lý ngưỡng xác suất (Probability Threshold) từ các mô hình Phân loại (Classification)
    # Tinh chỉnh độ nhạy: Mức 0.3 thay vì 0.5 để không bỏ sót thiên tai.
    if "pred_storm_prob" in df.columns:
        df = df.withColumn("Alert_Storm_Prob", F.when(F.col("pred_storm_prob") > 0.3, 1).otherwise(0))
        
    if "pred_inversion" in df.columns: # Dự đoán nghịch nhiệt
        df = df.withColumn("Alert_Inversion", F.when(F.col("pred_inversion") > 0.5, 1).otherwise(0))

    return df