def write_to_mongo(df, uri: str, cols: list, write_mode: str = "append"):
    """
    Hàm lõi để ghi một DataFrame (tĩnh hoặc micro-batch) vào MongoDB.
    Dùng chung cho cả luồng Batch và Streaming.
    """
    # Lọc ra các cột thực sự tồn tại trong DataFrame để tránh lỗi mismatch schema
    available_cols = [col for col in cols if col in df.columns]
    
    if not df.isEmpty():
        df.select(*available_cols).write \
            .format("mongodb") \
            .option("spark.mongodb.write.connection.uri", uri) \
            .mode(write_mode) \
            .save()
        return True
    return False

def make_foreach_batch_function(ml_model, observations_uri, alerts_uri, obs_cols, alert_cols):
    """
    Factory function trả về một hàm xử lý micro-batch dùng riêng cho Streaming (foreachBatch).
    """
    def process_and_write_batch(batch_df, batch_id):
        # Cache dữ liệu để tái sử dụng nhiều lần trong cùng 1 micro-batch
        batch_df.persist()
        
        if not batch_df.isEmpty():
            print(f"[*] Đang xử lý Micro-batch {batch_id} | Kích thước: {batch_df.count()} bản ghi")
            
            # 1. TÁI SỬ DỤNG HÀM LÕI: Ghi luồng giám sát (L3)
            write_to_mongo(batch_df, observations_uri, obs_cols, "append")
            
            # 2. Xử lý Model ML (Inference)
            if ml_model is not None:
                predictions_df = ml_model.transform(batch_df)
                
                # 3. TÁI SỬ DỤNG HÀM LÕI: Ghi luồng cảnh báo dự đoán (L4)
                write_to_mongo(predictions_df, alerts_uri, alert_cols, "append")
            
        # Giải phóng bộ nhớ
        batch_df.unpersist()
        
    return process_and_write_batch