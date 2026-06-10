import pandas as pd
from typing import Tuple, Iterator
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql import Row
from pyspark.sql import functions as F

def apply_streaming_l3_stateful(df):
    """Tính toán L3 Stateful bằng applyInPandasWithState cho luồng Streaming."""
    
    df = df.withColumn("pressure_delta_3h", F.lit(0.0).cast("double")) \
           .withColumn("wind_shear_U", F.lit(0.0).cast("double")) \
           .withColumn("wind_shear_V", F.lit(0.0).cast("double")) \
           .withColumn("temp_mean_6h", F.lit(0.0).cast("double")) \
           .withColumn("pm25_acc_12h", F.lit(0.0).cast("double"))
           
    output_schema = df.schema 
    
    # ✅ THAY ĐỔI CHÍNH: Lưu timestamp dưới dạng LONG (epoch ms) thay vì timestamp
    # Tránh hoàn toàn lỗi timetuple khi PySpark serialize state
    state_schema = "timestamp_ms long, pressure double, wind_U double, wind_V double, temp_c double, pm2_5 double"

    def l3_state_update(key: Tuple[str], pdfs: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
        new_pdf = pd.concat(list(pdfs))
        
        if state.exists:
            state_rows = state.get
            state_pdf = pd.DataFrame([row.asDict() for row in state_rows])
            
            # Chuyển timestamp_ms (long) về datetime để dùng trong pandas
            state_pdf["timestamp"] = pd.to_datetime(state_pdf["timestamp_ms"], unit="ms")
            state_pdf = state_pdf.drop(columns=["timestamp_ms"])
            
            for col in new_pdf.columns:
                if col not in state_pdf.columns:
                    state_pdf[col] = None
                    
            combined_pdf = pd.concat([state_pdf, new_pdf], ignore_index=True)
        else:
            combined_pdf = new_pdf
            
        combined_pdf = combined_pdf.sort_values("timestamp").reset_index(drop=True)

        target_cols = ["pressure", "wind_U", "wind_V", "temp_c", "pm2_5"]
        combined_pdf[target_cols] = combined_pdf[target_cols].ffill().bfill()
        
        combined_pdf["pressure_delta_3h"] = combined_pdf["pressure"].diff(3).fillna(0.0)
        combined_pdf["wind_shear_U"] = combined_pdf["wind_U"].diff(1).fillna(0.0)
        combined_pdf["wind_shear_V"] = combined_pdf["wind_V"].diff(1).fillna(0.0)
        combined_pdf["temp_mean_6h"] = combined_pdf["temp_c"].rolling(window=6, min_periods=1).mean().fillna(0.0)
        combined_pdf["pm25_acc_12h"] = combined_pdf["pm2_5"].rolling(window=12, min_periods=1).sum().fillna(0.0)
        
        new_cols = ["pressure_delta_3h", "wind_shear_U", "wind_shear_V", "temp_mean_6h", "pm25_acc_12h"]
        for col in new_cols:
            combined_pdf[col] = combined_pdf[col].astype("float64")
        
        latest_12_rows = combined_pdf.tail(12)
        state_list = []
        for _, row in latest_12_rows[["timestamp", "pressure", "wind_U", "wind_V", "temp_c", "pm2_5"]].iterrows():
            # Ép kiểu rõ ràng về dạng số nguyên thô (long) ngay tại đây
            ts = pd.to_datetime(row["timestamp"])
            ts_ms = int(ts.value // 1_000_000) # Lấy giá trị nanoseconds chia cho 1 triệu
            
            state_list.append((
                ts_ms,
                float(row["pressure"]),
                float(row["wind_U"]),
                float(row["wind_V"]),
                float(row["temp_c"]),
                float(row["pm2_5"])
            ))
        state.update(state_list)
        
        result_pdf = combined_pdf.tail(len(new_pdf)).reset_index(drop=True)
        yield result_pdf

    return df.groupBy("station_id").applyInPandasWithState(
        func=l3_state_update,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="update",
        timeoutConf=GroupStateTimeout.NoTimeout
    )