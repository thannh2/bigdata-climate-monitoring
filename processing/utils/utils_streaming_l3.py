import pandas as pd
from typing import Tuple, Iterator
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType

def apply_streaming_l3_stateful(df):
    """Tính toán L3 Stateful bằng applyInPandasWithState cho luồng Streaming."""
    
    # 1. Định nghĩa Output Schema (Thừa kế schema của L2 và thêm 5 cột L3)
    output_schema = df.schema \
        .add("pressure_delta_3h", "double") \
        .add("wind_shear_U", "double") \
        .add("wind_shear_V", "double") \
        .add("temp_mean_6h", "double") \
        .add("pm25_acc_12h", "double")
    
    # 2. Định nghĩa State Schema (Chỉ lưu các cột cần thiết để tối ưu RAM)
    state_schema = "timestamp timestamp, pressure double, wind_U double, wind_V double, temp_c double, pm2_5 double"

    def l3_state_update(key: Tuple[str], pdfs: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
        # Gộp tất cả các DataFrame trong Micro-batch hiện tại
        new_pdf = pd.concat(list(pdfs))
        
        # Truy xuất trạng thái cũ từ bộ nhớ
        if state.exists:
            state_dict = state.get[0] if isinstance(state.get, tuple) else state.get
            state_pdf = pd.DataFrame(state_dict)
            combined_pdf = pd.concat([state_pdf, new_pdf])
        else:
            combined_pdf = new_pdf
            
        # Sắp xếp theo chuỗi thời gian
        combined_pdf = combined_pdf.sort_values("timestamp").reset_index(drop=True)

        #Áp dụng Forward Fill để lấp đầy các cảm biến bị lệch pha thời gian
        combined_pdf = combined_pdf.ffill()
        
        # Để an toàn hơn, lấp ngược (Backward fill) cho dòng đầu tiên nếu nó bị Null
        combined_pdf = combined_pdf.bfill()
        
        # 3. Biến đổi Pandas (Vectorized)
        combined_pdf["pressure_delta_3h"] = combined_pdf["pressure"] - combined_pdf["pressure"].shift(3)
        combined_pdf["wind_shear_U"] = combined_pdf["wind_U"] - combined_pdf["wind_U"].shift(1)
        combined_pdf["wind_shear_V"] = combined_pdf["wind_V"] - combined_pdf["wind_V"].shift(1)
        combined_pdf["temp_mean_6h"] = combined_pdf["temp_c"].rolling(window=6, min_periods=1).mean()
        combined_pdf["pm25_acc_12h"] = combined_pdf["pm2_5"].rolling(window=12, min_periods=1).sum()
        
        # 4. Cập nhật State (Giữ đúng 12 dòng mới nhất)
        latest_12_rows = combined_pdf.tail(12)[["timestamp", "pressure", "wind_U", "wind_V", "temp_c", "pm2_5"]]
        state.update(latest_12_rows.to_dict(orient="records"))
        
        # 5. Xuất kết quả (Chỉ trả về các dòng tương ứng với dữ liệu mới tới)
        result_pdf = combined_pdf.tail(len(new_pdf))
        yield result_pdf

    # Kích hoạt Stateful Process theo từng trạm
    return df.groupBy("station_id").applyInPandasWithState(
        func=l3_state_update,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="update",
        timeoutConf=GroupStateTimeout.NoTimeout()
    )