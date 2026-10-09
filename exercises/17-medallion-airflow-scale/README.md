# Kiến trúc Data Lakehouse ETL - Quy mô 1M đến 100M (Dữ liệu BHXH)

Tài liệu này mô tả quy trình thực thi kiến trúc Medallion (Bronze - Silver - Gold) cho dự án Bảo Hiểm Xã Hội (BHXH) và các kỹ thuật xử lý dữ liệu ở quy mô lớn (1M - 10M - 100M dòng).

## 1. Nhận xét & Đánh giá khi Scale từ 1M lên 100M

Khi hệ thống đối mặt với việc mở rộng dữ liệu từ 1 Triệu lên 100 Triệu dòng, chúng ta không thể sử dụng cấu hình mặc định (vốn chạy tốt trên 1M). Dưới đây là những khác biệt và sự điều chỉnh bắt buộc:

1. **Memory Tuning (Tối ưu Bộ nhớ):**
   - **Tại 1M:** Spark có thể chạy mượt mà với tài nguyên cấp phát mặc định (`1g` memory).
   - **Tại 100M:** Các thao tác Join và Aggregate (ở lớp Silver và Gold) đòi hỏi một lượng lớn dữ liệu phải được nạp vào RAM. Nếu không cấu hình rõ ràng thông qua `.config("spark.driver.memory", "8g")` và `spark.executor.memory`, hệ thống chắc chắn sẽ sập với lỗi `Out-of-Memory (OOM)`.
2. **Shuffle Management (Quản lý trộn dữ liệu):**
   - Khi thực hiện `.groupBy("MA_DON_VI")` ở lớp Gold, dữ liệu của cùng một đơn vị phải di chuyển xuyên qua mạng để hội tụ về cùng một Executor (quá trình Shuffle). 
   - **Tại 10M / 100M:** Bắt buộc phải tăng `spark.sql.shuffle.partitions` (ví dụ: `200` thay vì mặc định `200` hoặc nhỏ hơn) để chia nhỏ khối lượng công việc, tránh hiện tượng thắt cổ chai (bottleneck) ở một vài Node cụ thể.
3. **Partitioning (Phân mảnh Ổ đĩa):**
   - **Tại 1M:** Ghi ra 1 file hay 63 file (chia theo `MA_TINH`) không tạo ra khác biệt thời gian rõ rệt.
   - **Tại 100M:** Nếu ghi ra 1 file duy nhất, dung lượng file parquet sẽ rất khổng lồ. Việc `partitionBy("MA_TINH")` ở lớp Silver không chỉ giúp song song hóa luồng Ghi (Write) mà còn kích hoạt cơ chế *Partition Discovery*, loại bỏ hoàn toàn hiện tượng Full-scan khi đọc lại dữ liệu phân tích từng tỉnh sau này.

---

## 2. Luồng quy trình ETL (Medallion Architecture)

### Lớp Bronze (Raw Ingestion)
- **File phụ trách:** `etl_bronze.py`
- **Mục tiêu:** Nạp dữ liệu BHXH thô từ hệ thống sinh tự động (`synthetic_bhxh/output/{scale}`) vào kho lưu trữ Lakehouse.
- **Hoạt động:** Đọc các thư mục `MASTER`, `DETAIL`, `ML_LABELS`, `ML_ANOMALY` và ghi nguyên trạng vào thư mục `bronze`. Đây là điểm neo an toàn giữ lại toàn bộ lịch sử nguyên bản.

### Lớp Silver (Cleansing & Enrichment)
- **File phụ trách:** `etl_silver.py`
- **Mục tiêu:** Cung cấp "Single Source of Truth". Dữ liệu được làm sạch và chuẩn hóa phục vụ Machine Learning và phân tích.
- **Hoạt động:**
  1. **Enrichment:** Thực hiện phép kết nối (Join) `MASTER` với `ML_LABELS` (chứa nhãn trốn đóng) và `DETAIL` với `ML_ANOMALY` (chứa cờ bất thường).
  2. **Write:** Ghi dữ liệu đã làm giàu vào `silver/MASTER_ENRICHED` và `silver/DETAIL_ENRICHED` sử dụng `partitionBy("MA_TINH")`.

### Lớp Gold (Aggregations & Reporting)
- **File phụ trách:** `etl_gold.py`
- **Mục tiêu:** Cung cấp các Data Mart (Bảng tổng hợp) phục vụ trực tiếp cho báo cáo và BI Dashboard.
- **Hoạt động:**
  - `AGG_PERSON`: Tổng hợp lịch sử đóng của từng cá nhân (Tính tổng mức đóng, trung bình lương bình quân, tổng lượt đóng).
  - `AGG_COMPANY`: Thống kê theo doanh nghiệp (Số lượng nhân sự, tổng tiền BHXH đã nộp, số lượt đóng bất thường).

---

## 3. Điều phối tự động với Apache Airflow (Orchestration)

Để đảm bảo các tiến trình Spark xử lý dữ liệu khổng lồ (100M rows) được chạy theo đúng thứ tự tuyến tính và tránh tranh chấp tài nguyên máy tính, hệ thống áp dụng **Apache Airflow**.

### Thiết kế DAG (`medallion_scale_etl_dag`)
Toàn bộ luồng được định nghĩa trong file `dags/etl_scale_dag.py` với cấu trúc nối tiếp:
`run_bronze_layer >> run_silver_layer >> run_gold_layer`

- Tiến trình sau chỉ được kích hoạt khi tiến trình trước đã thành công. 
- Lớp Silver là tiến trình chịu tải nặng nhất do phải thao tác Shuffle và ghi phân mảnh toàn bộ hàng chục triệu dòng.

### Cơ chế Tham số hóa (Parametrization) để Test Hiệu năng
DAG được thiết kế cực kỳ linh hoạt để phục vụ việc so sánh Scale:
Khi người dùng bấm **Trigger DAG w/ config** trên giao diện Airflow, có thể truyền vào biến `{"scale": "1M"}`, `{"scale": "10M"}`, hoặc `{"scale": "100M"}`. 

DAG sẽ tự động định tuyến đường dẫn biến `input_path` để trỏ vào đúng thư mục tệp dữ liệu BHXH tương ứng mà không cần phải sửa bất cứ dòng code nào.

### Quản trị rủi ro & Restartability (Khả năng chạy lại)
- **Tính luỹ đẳng (Idempotency):** Code Spark được thiết lập sử dụng `mode("overwrite")`. Nếu tiến trình Gold bị sập do OOM khi xử lý 100M dòng, bạn chỉ cần điều chỉnh RAM, bấm nút **Clear** trên giao diện Airflow tại task đó để chạy lại. Hệ thống sẽ ghi đè lên thư mục lỗi một cách an toàn mà không bị nhân bản (duplicate) dữ liệu.
