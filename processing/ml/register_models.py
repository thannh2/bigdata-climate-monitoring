import os
import sys
# 1. THIẾT LẬP ĐƯỜNG DẪN GỐC
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")

import mlflow

def main():
    print(f"[*] Cấu hình MLflow Tracking URI: {MLFLOW_TRACKING_URI}")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("Climate_Models")

    models_dir = os.path.join(ROOT_DIR, "models")
    print(f"[*] Đang quét thư mục: {models_dir}")
    
    if not os.path.exists(models_dir):
        print(f"[ERROR] KHÔNG TÌM THẤY THƯ MỤC: {models_dir}")
        return

    model_folders = [f for f in os.listdir(models_dir) if os.path.isdir(os.path.join(models_dir, f))]
    
    if len(model_folders) == 0:
        print("[WARNING] Thư mục models đang trống.")
        return
        
    print(f"[*] Bắt đầu đẩy {len(model_folders)} mô hình lên server (Chế độ Direct Copy)...")

    for model_name in model_folders:
        # Trỏ thẳng vào thư mục chứa file MLmodel
        mlflow_model_dir = os.path.join(models_dir, model_name, "version-1", "model")
        
        print(f"  -> Đang xử lý: {model_name}")
        
        if not os.path.exists(os.path.join(mlflow_model_dir, "MLmodel")):
            print(f"     [ERROR] Bỏ qua do không tìm thấy file MLmodel tại: {mlflow_model_dir}")
            continue

        try:
            with mlflow.start_run(run_name=f"Import_{model_name}") as run:
                # 1. Upload toàn bộ file trong thư mục lên MLflow Artifacts
                mlflow.log_artifacts(local_dir=mlflow_model_dir, artifact_path="model")
                
                # 2. Đăng ký Artifact vừa upload thành Model chính thức
                model_uri = f"runs:/{run.info.run_id}/model"
                mlflow.register_model(model_uri=model_uri, name=model_name)
                
            print(f"     [+] Thành công: {model_name}")
        except Exception as e:
            print(f"     [ERROR] Thất bại ở {model_name}. Chi tiết: {e}")

    print("[*] Hoàn tất quá trình đăng ký!")

if __name__ == "__main__":
    main()