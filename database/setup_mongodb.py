import os
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import OperationFailure

def setup_database():
    print("[*] Đang kết nối tới MongoDB...")
    
    # 1. Tải cấu hình
    load_dotenv()
    ROOT_USER = os.getenv("MONGO_ROOT_USER")
    ROOT_PASS = os.getenv("MONGO_ROOT_PASS")
    PORT = os.getenv("MONGO_PORT", "27017")
    HOST = "localhost"
    
    # Lấy thông tin user
    SPARK_USER = os.getenv("MONGO_SPARK_USER", "spark_user")
    SPARK_PASS = os.getenv("MONGO_SPARK_PASS", "SparkWritePassword2026")
    STREAMLIT_USER = os.getenv("MONGO_STREAMLIT_USER", "streamlit_user")
    STREAMLIT_PASS = os.getenv("MONGO_STREAMLIT_PASS", "StreamlitReadPassword2026")
    
    # 2. Khởi tạo kết nối Root
    MONGO_URI = f"mongodb://{ROOT_USER}:{ROOT_PASS}@{HOST}:{PORT}/?authSource=admin"
    client = MongoClient(MONGO_URI)
    
    db = client["climate_db"]
    admin_db = client["admin"] 
    
    # Dọn dẹp collection cũ để tránh rác hệ thống
    if "historical_weather" in db.list_collection_names():
        db.drop_collection("historical_weather")
        print("[*] Đã xóa collection cũ: historical_weather")

    print("[*] Đang cấu hình Indexes cho hệ thống mới...")
    
    # 1. Collection: Dữ liệu thực tế
    db["climate_observations"].create_index([("station_id", ASCENDING), ("timestamp", DESCENDING)])

    # 2. Collection: Dự đoán thời gian thực (tự xóa sau 24h)
    db["realtime_alerts"].create_index("timestamp", expireAfterSeconds=86400)
    
    # 3. Collection: Quan trắc thời gian thực
    db["realtime_observations"].create_index([("station_id", ASCENDING), ("timestamp", DESCENDING)])

    print("[*] Đang thiết lập Role-Based Access Control...")
    
    # Tạo user cho PySpark (Quyền Ghi)
    try:
        admin_db.command(
            "createUser", SPARK_USER,
            pwd=SPARK_PASS,
            roles=[{"role": "readWrite", "db": "climate_db"}]
        )
        print(f"  + Đã tạo tài khoản Ingestion: {SPARK_USER}")
    except OperationFailure as e:
        if e.code == 51003: 
            print(f"  - Tài khoản {SPARK_USER} đã tồn tại, bỏ qua.")
        else:
            print(f"  - Lỗi khi tạo {SPARK_USER}: {e}")

    # Tạo user cho Streamlit (Quyền Đọc)
    try:
        admin_db.command(
            "createUser", STREAMLIT_USER,
            pwd=STREAMLIT_PASS,
            roles=[{"role": "read", "db": "climate_db"}]
        )
        print(f"  + Đã tạo tài khoản Serving: {STREAMLIT_USER}")
    except OperationFailure as e:
        if e.code == 51003:
            print(f"  - Tài khoản {STREAMLIT_USER} đã tồn tại, bỏ qua.")
        else:
            print(f"  - Lỗi khi tạo {STREAMLIT_USER}: {e}")

    print("[+] Hoàn tất thiết lập MongoDB!")
    client.close()

if __name__ == "__main__":
    setup_database()