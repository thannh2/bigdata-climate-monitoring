# Kế hoạch Cải Thiện `air-quality-ml` Cho Luồng ML End-to-End

**Ngày đánh giá:** 2026-04-19  
**Phạm vi đọc:** `configs/`, `jobs/`, `src/`, `tests/`, `docs/`, `README.md`, `QUICKSTART.md`, `MLLIB_MLFLOW_ASSESSMENT.md`, `CLEANUP_REPORT.md`

## 1. Mục tiêu

Tài liệu này tổng hợp những hạng mục cần cải thiện để `air-quality-ml` thực sự vận hành được như khối ML trong sơ đồ tổng thể:

- nhận feature table từ luồng ingestion/processing,
- train và đăng ký model qua MLflow,
- score batch và tiến tới score streaming,
- ghi kết quả xuống MongoDB hoặc data lake,
- phát dữ liệu ổn định cho dashboard và monitoring.

Phạm vi của tài liệu này **vẫn bám theo hiện trạng dự án**: khối ML lấy dữ liệu đầu vào từ dữ liệu đã có sẵn trong thư mục `Data/`, cụ thể là transformed feature table đang được cấu hình qua `features_path`. Các đề xuất bên dưới không thay đổi nguồn dữ liệu đầu vào này; chúng chỉ bổ sung lớp kiểm soát, curated storage và vận hành quanh nguồn dữ liệu đã có.

Tài liệu này **bổ sung** cho `MLLIB_MLFLOW_ASSESSMENT.md`. Báo cáo cũ tập trung vào chất lượng MLlib/MLflow; tài liệu này tập trung vào **khả năng chạy end-to-end của toàn bộ luồng ML**.

## 2. Kết luận nhanh

`air-quality-ml` có nền tảng tốt ở phần training với Spark MLlib, cấu trúc module rõ ràng, và đã có tích hợp MLflow. Tuy nhiên, nếu đối chiếu với luồng trong sơ đồ thì dự án hiện mới ở mức **partial implementation**:

- training có khung tốt nhưng còn vướng tham chiếu path/cột cũ,
- batch scoring có khung nhưng default input và output contract chưa ổn,
- streaming inference mới là skeleton,
- monitoring đang lệch schema sau refactor,
- tài liệu và test chưa đồng bộ với code hiện tại,
- môi trường chạy Python trong workspace hiện không sẵn sàng để smoke test.

Nếu không xử lý các blocker này trước, luồng từ feature table -> training -> registry -> serving -> dashboard sẽ dễ đứt ở runtime, dù code nhìn qua có vẻ đầy đủ.

## 3. Trạng thái hiện tại theo từng khối

| Khối | Trạng thái | Nhận xét ngắn |
|---|---|---|
| Feature loading | Có nhưng mỏng | Đọc parquet transformed được, nhưng chưa có schema contract/data contract rõ ràng |
| Processing job | Chưa hoàn chỉnh | `build_gold_features_targets.py` chỉ gọi validate/show, chưa materialize output gold |
| Training | Có nhưng đang lệch refactor | `train_job.py` vẫn gọi `gold_targets_path` và dùng `event_hour` ở artifact mẫu |
| MLflow | Khá tốt | Có tracking, signature, registry; promotion logic còn quá đơn giản |
| Batch serving | Có nhưng chưa ổn định | `batch_score.py` vẫn gọi `gold_targets_path`, output schema chưa chuẩn hóa |
| Stream serving | Chưa có | `stream_score.py` mới là skeleton |
| Monitoring | Gãy sau refactor | `monitor_daily.py` vẫn dùng `gold_features_path` và tên cột cũ |
| Visualization contract | Chưa rõ | Chưa có schema chính thức cho real-time view và historical view |
| Testing | Chưa đủ và còn stale | Test vẫn dùng `event_hour`, `pm25`; coverage còn thấp |
| Runtime environment | Đang block | `venv` trỏ tới Python không còn tồn tại, `python`/`pytest` không chạy được trong workspace |

## 4. Các khoảng trống chính cần cải thiện

### 4.1. Đồng bộ schema và path sau refactor

Đây là blocker lớn nhất hiện nay.

### Phát hiện

- `settings.py` chỉ còn `features_path`, `gold_predictions_path`, `gold_eval_path`, `monitoring_path`.
- Nhưng `train_job.py` vẫn đọc `base_settings.data.gold_targets_path`.
- `batch_score.py` vẫn dùng `settings.data.gold_targets_path`.
- `jobs/monitor_daily.py` vẫn dùng `settings.data.gold_features_path`.
- Một số đoạn code, test và docs vẫn dùng schema cũ: `event_hour`, `pm25`, `temperature_c`, `pressure_hpa`, `wind_speed_mps`.

### Tác động

- Training job có thể fail ngay khi resolve config.
- Batch scoring không có default input path hợp lệ.
- Monitoring job gần như chắc chắn fail khi chạy.
- Docs và tests tạo cảm giác an toàn giả vì không phản ánh schema thật.

### Cần cải thiện

- Chốt một **canonical schema** cho `air-quality-ml`, tối thiểu gồm:
  - `station_id`
  - `timestamp`
  - feature columns chuẩn hóa như `temp_c`, `humidity`, `pressure`, `wind_speed`, `pm2_5`, `us_aqi`
  - target columns `target_pm25_*h` và `target_alert_*h`
- Chốt một **canonical path contract**:
  - `features_path`: input transformed features
  - `gold_predictions_path`: output scoring
  - `gold_eval_path`: output evaluation/performance snapshots
  - `monitoring_path`: output monitoring snapshots
- Sửa toàn bộ code, test và docs để chỉ dùng một bộ tên cột và path.
- Bất kỳ tên cột cũ nào còn lại phải bị loại bỏ hoặc đặt ở tầng migration rõ ràng.

### 4.2. Thiếu data contract giữa upstream processing và khối ML

Hiện `load_features.py` đang làm nhiệm vụ đọc dữ liệu khá tốt, nhưng phần contract vẫn còn cảm tính. Với một pipeline Spark, không nên tiếp tục mở rộng bằng các đoạn validation tự viết rời rạc trong từng job; nên chuẩn hóa bằng thư viện chuyên dụng để data contract trở thành một phần chính thức của pipeline.

Lưu ý: ở đây **không đổi nguồn input**. Input vẫn là dữ liệu transformed đang nằm trong `Data/`; phần cần cải thiện là cách kiểm tra và bảo vệ chất lượng của dữ liệu đó trước khi train/score.

### Phát hiện

- `load_features.py` tự suy diễn `region` và `city` từ `station_id`.
- `feature_store.yaml` hiện mới giống tài liệu hơn là cấu hình được thực thi.
- Sau khi xóa module `data_contract/`, code không còn một lớp validate schema/type/range chính thức trước khi train và score.

### Tác động

- Nếu upstream đổi tên station hoặc bổ sung station mới, mapping `region/city` có thể sai hoặc thành `unknown`.
- Không có cơ chế fail-fast khi feature table thiếu cột, sai kiểu dữ liệu, hoặc mất partition.
- Dễ xảy ra silent drift giữa dữ liệu train và dữ liệu serving.

### Cần cải thiện

- Tạo lại một lớp **schema contract** cho transformed feature table:
  - required columns,
  - data types,
  - nullable/non-nullable,
  - allowed categorical values,
  - partition convention `year/month`.
- Thay vì duy trì validation thủ công theo kiểu ad-hoc, tích hợp một framework data validation tương thích với hệ sinh thái Spark/PySpark như:
  - `Great Expectations` cho batch checkpoint, expectation suite và validation report trên feature table, prediction table, monitoring table;
  - `Pandera` cho schema-first validation ở lớp unit test, sampled batch hoặc các bước convert sang pandas/pandas-on-Spark khi cần kiểm tra chặt kiểu dữ liệu.
- Tách rõ 3 loại kiểm tra:
  - schema validation,
  - data quality validation,
  - semantic/business rule validation.
- Tách `station metadata` thành lookup table hoặc file cấu hình riêng thay vì hard-code trong loader.
- Biến `feature_store.yaml` thành cấu hình có kiểm tra thực thi, không chỉ là danh sách tham khảo.
- Lưu version của data contract cùng với kết quả validation để trace được:
  - dataset nào đã pass contract,
  - contract version nào đã được áp dụng,
  - expectation/schema nào đã fail.

### 4.3. Processing job hiện chưa thực sự tạo “gold dataset” cho ML

Tên job và hành vi hiện tại chưa khớp nhau.

Trong bối cảnh hiện tại, “gold dataset” được hiểu là **lớp curated được tạo ra từ dữ liệu đã có sẵn trong `Data/`**, không phải thay thế hay chuyển nguồn ingestion sang nơi khác.

### Phát hiện

- `jobs/build_gold_features_targets.py` chỉ gọi `air_quality_ml.processing.pipeline_job`.
- `pipeline_job.py` hiện:
  - load dữ liệu,
  - thêm alert targets nếu thiếu,
  - log row count,
  - in schema và sample.
- Job này **không ghi ra** một bảng gold/training snapshot cố định.

### Tác động

- Không có mốc dữ liệu huấn luyện được materialize để audit, reproduce hoặc rollback.
- Tên job dễ gây hiểu nhầm cho vận hành vì “build_gold_features_targets” nhưng thực chất mới là “validate and preview”.
- Batch training và batch scoring đang cùng phụ thuộc trực tiếp vào raw transformed features, chưa có lớp curated view trung gian.

### Cần cải thiện

- Hoặc đổi tên job theo đúng chức năng hiện tại.
- Hoặc triển khai đúng ý nghĩa của job:
  - validate schema,
  - tạo alert targets,
  - chuẩn hóa cột metadata,
  - ghi một bảng curated dành cho training/batch scoring,
  - partition theo thời gian/horizon nếu cần.
- Thay vì chỉ ghi parquet + manifest thủ công, chuẩn hóa curated dataset sang **Delta Lake**:
  - nguồn upstream vẫn là parquet/features đã có trong `Data/`,
  - dùng Delta table làm source of truth cho dữ liệu training/serving,
  - tận dụng `time-travel` để reproduce đúng snapshot dữ liệu đã dùng train một model,
  - tận dụng `schema enforcement` để chặn batch ghi sai cấu trúc,
  - tận dụng transaction log để biết chính xác dataset version nào đã được sử dụng.
- Nếu vẫn cần manifest cho mục đích vận hành, manifest chỉ nên là lớp metadata mỏng tham chiếu tới:
  - Delta table path hoặc table name,
  - Delta version/timestamp,
  - input source range,
  - row count sau khi curate.
- Về mặt thiết kế, `train_job.py` và `batch_score.py` nên đọc từ curated Delta table thay vì phụ thuộc trực tiếp vào raw transformed parquet.

### 4.4. Training pipeline tốt nhưng chưa đủ “production-safe”

### Phát hiện

- `train_job.py` có orchestration rõ ràng và MLflow integration tốt.
- Tuy nhiên phần path/schema hiện chưa đồng bộ.
- `should_promote_regression()` chỉ kiểm tra tương quan `mae_high_pollution` với `mae`.
- `should_promote_classification()` mới kiểm tra `recall` và `false_negative_rate`.
- Hàm `try_transition_stage()` có tồn tại nhưng chưa được dùng.
- `thresholding.py` gọi `toPandas()` trên toàn bộ validation set để tune threshold.
- `base.yaml` khai báo `regression_mae_improve_ratio` và `classification_min_auprc_ratio` nhưng code hiện không dùng.

### Tác động

- Promotion logic chưa đủ để bảo vệ production model.
- Các ngưỡng khai báo trong config có thể khiến người vận hành hiểu nhầm rằng đã được áp dụng.
- Việc kéo full validation set về pandas sẽ trở thành bottleneck khi dữ liệu tăng.

### Cần cải thiện

- Bắt buộc so sánh candidate với champion hiện tại trong MLflow registry trước khi promote.
- Dùng đầy đủ các rule đã khai báo trong config hoặc xóa config thừa.
- Gắn alias/stage cho model sau khi pass gating.
- Log thêm:
  - dataset version,
  - feature set version,
  - baseline model version,
  - decision threshold đã áp dụng,
  - promotion decision reason.
- Nếu dữ liệu tiếp tục tăng, threshold tuning cần chuyển sang:
  - Spark aggregation theo grid threshold,
  - hoặc sample có kiểm soát,
  - hoặc batch evaluate theo histogram xác suất.

### 4.5. Hỗ trợ horizon đang không nhất quán

### Phát hiện

- `configs/base.yaml` và tài liệu nhắc đến `1h, 6h, 12h, 24h`.
- Nhưng trong `jobs/` và `configs/` hiện chỉ có wrapper/config train cho `1h` và `6h`.
- Tài liệu cũ từng nhắc `3h`, một phần code/tests/docs vẫn còn dấu vết schema cũ.

### Tác động

- Dễ gây hiểu sai rằng hệ thống đã hỗ trợ đủ 4 horizons ở cấp độ vận hành.
- Khó lập lịch Airflow hoặc CI vì không rõ đâu là matrix train chính thức.

### Cần cải thiện

- Chốt một trong hai hướng:
  - **Hướng A:** chỉ công bố chính thức `1h` và `6h` cho tới khi hoàn tất `12h/24h`.
  - **Hướng B:** bổ sung đầy đủ job/config/test/docs cho `12h/24h`.
- Matrix job phải được sinh từ config hoặc manifest chung, tránh nhân bản thủ công quá nhiều wrapper scripts.

### 4.6. Batch scoring chưa có output contract đủ rõ

### Phát hiện

- `batch_score.py` load model từ MLflow và score được theo hướng thiết kế.
- Nhưng default input path đang trỏ vào field config không còn tồn tại.
- Code vẫn chọn `event_hour` khi ghi Mongo payload.
- `gold_eval_path` đã có trong config nhưng chưa thấy được dùng.

### Tác động

- Khó nối batch score vào serving layer một cách ổn định.
- Prediction output chưa được chuẩn hóa thành schema dùng chung cho MongoDB, historical store và dashboard.
- Chưa có evaluation sink riêng để phục vụ backtesting hoặc compare prediction vs actual.

### Cần cải thiện

- Chuẩn hóa output schema cho prediction:
  - `station_id`
  - `timestamp`
  - `horizon`
  - `prediction`
  - `pred_prob`
  - `pred_alert`
  - `alert_level`
  - `model_name`
  - `model_version`
  - `prediction_time`
  - `batch_id` hoặc `run_id`
- Ghi prediction ra data lake theo partition rõ ràng, ví dụ:
  - `prediction_date`
  - `horizon`
  - `model_version`
- Nếu đã dùng Delta Lake cho curated layer, prediction output và evaluation output cũng nên ưu tiên ghi dưới dạng Delta để giữ:
  - schema consistency,
  - audit trail,
  - khả năng backfill/replay theo version.
- Sử dụng `gold_eval_path` cho bảng performance/backfill evaluation.

### 4.7. Stream serving vẫn chưa được triển khai

### Phát hiện

- `stream_score.py` hiện mới là skeleton và trả lại nguyên `source_df`.
- Trong sơ đồ tổng thể, Kafka -> Spark Streaming -> MongoDB là luồng quan trọng cho real-time view.

### Tác động

- Luồng real-time trong sơ đồ hiện chưa có thực thi ở khối ML.
- Không có checkpoint, retry, DLQ, hoặc chiến lược reload model khi stage/alias thay đổi.

### Cần cải thiện

- Triển khai structured streaming thật sự:
  - đọc từ Kafka hoặc Delta source,
  - parse schema message,
  - load model theo alias/stage từ MLflow,
  - score theo micro-batch,
  - ghi xuống lake và MongoDB,
  - checkpoint để đảm bảo exactly-once ở mức khả dụng của pipeline.
- Bổ sung DLQ hoặc quarantine stream cho record lỗi schema.
- Tách rõ real-time collection và historical collection cho dashboard.

### 4.8. Ghi MongoDB hiện chưa đủ an toàn cho serving

### Phát hiện

- `writer_mongodb.py` dùng `insert_many()` trực tiếp theo partition.
- Chưa có:
  - upsert key,
  - unique index strategy,
  - retry policy,
  - error routing,
  - deduplication logic.

### Tác động

- Chạy lại batch có thể tạo duplicate documents.
- Khó bảo đảm “last prediction wins”.
- Dashboard real-time và historical có thể nhìn thấy dữ liệu trùng hoặc không nhất quán.

### Cần cải thiện

- Thiết kế khóa idempotent, ví dụ theo:
  - `station_id`
  - `timestamp`
  - `horizon`
  - `model_version`
- Chuyển sang `bulk_write` với `UpdateOne(..., upsert=True)`.
- Định nghĩa index cho realtime view và historical view.
- Tách collection contract giữa:
  - latest serving view,
  - historical prediction log,
  - monitoring snapshots.

### 4.9. Monitoring hiện đang lệch schema và chưa được orchestration

### Phát hiện

- `jobs/monitor_daily.py` vẫn dùng:
  - `gold_features_path`,
  - `pm25`,
  - `temperature_c`,
  - `pressure_hpa`,
  - `wind_speed_mps`,
  - `event_hour`.
- `monitoring_thresholds.yaml` tồn tại nhưng chưa được code consume.
- `monitoring/drift.py` và `monitoring/performance.py` có utility hữu ích nhưng chưa được nối thành một pipeline monitoring hoàn chỉnh.

### Tác động

- Monitoring job hiện không đáng tin để chạy production.
- Threshold trong config không tạo ra alert hoặc severity thực tế.
- Chưa có snapshot phục vụ dashboard cho:
  - data quality,
  - feature drift,
  - model performance drift.

### Cần cải thiện

- Sửa monitoring job về schema/path hiện tại.
- Đọc `monitoring_thresholds.yaml` trong runtime thay vì để config chết.
- Tạo 3 nhóm output monitoring riêng:
  - data quality snapshot,
  - feature drift snapshot,
  - performance snapshot.
- Mỗi snapshot cần có:
  - `metric_time`
  - `metric_name`
  - `metric_value`
  - `severity`
  - `model_name`
  - `horizon`
  - `data_window`

### 4.10. Test, docs và môi trường chạy chưa đủ để hỗ trợ vận hành

### Phát hiện

- Tests hiện còn ít và có test dùng schema cũ.
- `docs/runbook.md`, `docs/architecture.md`, `docs/feature_catalog.md` chưa đồng bộ sau refactor.
- `README.md` mô tả phạm vi hỗ trợ rộng hơn khả năng vận hành thực tế.
- Trong workspace hiện tại:
  - `pytest` không có trên PATH,
  - `python` không có trên PATH,
  - `venv\Scripts\python.exe` báo lỗi do interpreter gốc không tồn tại.

### Tác động

- Không có cách smoke test ổn định cho luồng ML.
- Người vận hành và người phát triển dễ hiểu sai khả năng thật của hệ thống.
- Việc bàn giao hoặc CI/CD sẽ bị block.

### Cần cải thiện

- Không nên xem `venv` là cơ chế chính để đóng gói môi trường chạy cho pipeline này. Với luồng dữ liệu lớn dùng Spark/Hadoop, cần coi **Docker** là môi trường chuẩn để chạy local, CI và server.
- Thiết lập một Docker image chuẩn chứa đầy đủ:
  - Python dependencies,
  - Java/Spark runtime,
  - Hadoop native dependencies nếu còn cần,
  - các biến môi trường và path cấu hình như `HADOOP_HOME`, `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON`,
  - các file native dành cho Windows nếu workflow hiện tại vẫn phụ thuộc `hadoop.dll`.
- Chuẩn hóa cách chạy job qua container thay vì phụ thuộc vào interpreter nằm trong `venv` của từng máy.
- Thêm test cho:
  - config loading,
  - feature schema validation,
  - training orchestration,
  - batch scoring,
  - monitoring job,
  - Mongo writer contract.
- Đồng bộ lại toàn bộ docs theo canonical schema mới.
- Nếu cần giữ `venv` cho phát triển nhanh, phải ghi rõ trong tài liệu rằng `venv` chỉ là môi trường phụ trợ; môi trường chuẩn để reproduce production phải là container image có version rõ ràng.

## 5. Backlog ưu tiên triển khai

### 5.1. P0 - Sửa blocker để chạy được end-to-end cơ bản

**Mục tiêu:** làm cho luồng `features_path -> train -> batch score -> monitoring snapshot` chạy được với schema hiện tại.

- Đồng bộ toàn bộ code khỏi `gold_targets_path`, `gold_features_path`, `event_hour`, `pm25`, `temperature_c`, `pressure_hpa`, `wind_speed_mps`.
- Sửa `train_job.py`, `batch_score.py`, `monitor_daily.py`, tests và docs theo schema `timestamp`, `pm2_5`, `temp_c`, `pressure`, `wind_speed`.
- Chốt chính thức phạm vi horizon:
  - bổ sung nốt `12h/24h`.
- `build_gold_features_targets.py` : giữ tên file và biến nó thành job materialize thật
- Thiết lập Docker image chuẩn cho dự án để khóa version runtime và loại bỏ phụ thuộc vào `venv` cục bộ của từng máy.
- Đóng gói trong container các cấu hình hệ thống đang dễ vỡ như `HADOOP_HOME`, Spark runtime, Hadoop native binaries và các biến `PYSPARK_*`.
- Tạo lại môi trường Python chạy được trong container và thêm smoke tests tối thiểu.

**Điều kiện hoàn thành P0**

- Chạy được `train_pm25_h1`.
- Chạy được `batch_score_latest.py` với input mặc định hợp lệ.
- Chạy được `monitor_daily.py`.
- Chạy được cùng một lệnh trên máy dev và môi trường server thông qua Docker image thống nhất.
- Docs không còn nhắc schema/path cũ.

### 5.2. P1 - Hoàn thiện batch ML workflow

**Mục tiêu:** batch training và batch serving có thể vận hành ổn định, audit được, và gắn được vào Airflow.

- Tạo curated training/serving dataset trên Delta Lake từ dữ liệu đầu vào đang có trong `Data/`, dùng Delta version làm dataset version chính.
- Đưa `gold_eval_path` vào sử dụng thật cho backtest/performance snapshots.
- Hoàn thiện model promotion:
  - compare với champion,
  - ghi lại lý do promote/reject,
  - stage/alias transition rõ ràng.
- Chuẩn hóa prediction output schema và partition strategy.
- Đọc và áp dụng `monitoring_thresholds.yaml` trong pipeline monitoring.

**Điều kiện hoàn thành P1**

- Một DAG batch có thể:
  - build curated data,
  - train,
  - register model,
  - score,
  - xuất monitoring/eval artifacts.

### 5.3. P2 - Mở rộng streaming và serving contract

**Mục tiêu:** khớp với nhánh Kafka -> Spark Streaming -> MongoDB -> Dashboard trong sơ đồ.

- Triển khai `stream_score.py` thành structured streaming job thật.
- Dùng model alias/stage để serving không phải hard-code version.
- Dùng Mongo upsert/idempotent write.
- Tách latest serving view và historical serving log.
- Bổ sung checkpoint, DLQ, retry policy, schema validation cho message stream.

**Điều kiện hoàn thành P2**

- Có thể score real-time từ Kafka source và cập nhật MongoDB ổn định.
- Dashboard có thể đọc cả real-time view lẫn historical view theo cùng contract.

### 5.4. P3 - Hoàn thiện MLOps và governance

**Mục tiêu:** giảm rủi ro vận hành, tăng khả năng mở rộng và bàn giao.

- CI cho lint/test/smoke run.
- Tăng coverage cho training, inference, monitoring.
- Theo dõi drift/performance theo model version và horizon.
- Tạo runbook vận hành và rollback rõ ràng.
- Bổ sung cơ chế retrain trigger khi drift/performance vượt ngưỡng.

## 6. Thứ tự triển khai đề xuất

1. Sửa schema/path mismatch và repair runtime environment.
2. Thiết lập Docker image chuẩn cho môi trường chạy.
3. Chốt support matrix cho horizons và cập nhật docs.
4. Materialize curated dataset + prediction/eval outputs, ưu tiên Delta Lake.
5. Hoàn thiện model gating/promotion trong MLflow.
6. Sửa monitoring để phản ánh schema mới và đọc threshold config.
7. Hoàn thiện Mongo serving contract.
8. Triển khai streaming scoring.
9. Sau cùng mới mở rộng thêm tuning, explainability và automation nâng cao.

## 7. Tiêu chí “đủ tốt” để bám theo sơ đồ hiện tại

`air-quality-ml` được xem là đủ tốt để phục vụ luồng ML trong sơ đồ khi đạt được các điều kiện sau:

- Có một input contract rõ ràng từ upstream processing.
- Có curated dataset phục vụ train/score, không phụ thuộc trực tiếp vào dữ liệu thô chưa kiểm soát.
- Có ít nhất một train job và một batch score job chạy thành công bằng config mặc định.
- Có model registry flow với promote rule rõ ràng.
- Có monitoring snapshot dùng được cho dashboard.
- Có Mongo contract idempotent cho serving.
- Có streaming scoring thật nếu dashboard yêu cầu real-time view.
- Docs, tests và runtime environment phản ánh đúng hiện trạng code.

## 8. Kết luận

Ưu tiên đúng lúc này không phải thêm thuật toán mới, mà là **đóng các khoảng trống hạ tầng ML** sau refactor:

- đồng bộ schema/path,
- tiêu chuẩn hóa data contract bằng công cụ chuyên dụng,
- làm cho job chạy thật,
- chuyển curated storage sang Delta Lake,
- chuẩn hóa output contract,
- sửa monitoring,
- container hóa môi trường chạy,
- và khôi phục testability của dự án.

Sau khi hoàn tất P0 và P1, `air-quality-ml` mới đủ nền tảng để nối chắc chắn vào luồng ingestion -> processing -> training -> serving -> visualization trong kiến trúc tổng thể.
