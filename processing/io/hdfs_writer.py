from pyspark.sql import DataFrame

def write_to_hdfs(df: DataFrame, output_path: str, load_mode: str = "append", partition_cols: list = None):
    """
    Hàm ghi DataFrame xuống HDFS dưới định dạng Parquet.
    """
    # Mặc định sử dụng chiến lược partition theo thời gian nếu không truyền vào
    if partition_cols is None:
        partition_cols = ["year", "month", "day_of_year"]

    print(f"[*] Đang ghi dữ liệu xuống HDFS (Mode: {load_mode}) tại: {output_path}")
    
    if not df.isEmpty():
        # Dùng coalesce(1) để gom thành 1 file lớn mỗi partition (tránh lỗi small files trên HDFS)
        writer = df.coalesce(1).write.mode(load_mode)
        
        # Áp dụng partition nếu có
        if partition_cols and len(partition_cols) > 0:
            writer = writer.partitionBy(*partition_cols)
            
        writer.parquet(output_path)
        return True
    else:
        print("[-] DataFrame trống, bỏ qua thao tác ghi HDFS.")
        return False