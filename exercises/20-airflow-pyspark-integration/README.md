# Bài 20: Tích hợp Airflow và PySpark (Custom Docker Image)

Trong các bài trước, chúng ta dùng Airflow để điều phối công việc, nhưng worker của Airflow thường không chứa sẵn môi trường Java và PySpark. Điều này dẫn đến việc `BashOperator` khi gọi lệnh `python script_pyspark.py` sẽ gặp lỗi thiếu module hoặc thiếu Java.

Bài này đi sâu vào giải pháp: **Tùy biến (Custom) Docker Image của Airflow** để cài đặt sẵn Java 17 và PySpark. Nhờ đó, Airflow worker có thể trực tiếp chạy mã Spark ở chế độ `local`.

## 1. Nhận xét & Đánh giá khi Scale từ 1M lên 100M với Airflow
Tại sao chúng ta phải khổ sở đóng gói PySpark vào Airflow Worker thay vì chạy bằng lệnh `python` trên máy cá nhân? Đó là vì câu chuyện Scale (Mở rộng):

1. **Khả năng tự phục hồi (Resilience):**
   - **Tại 1M:** Chạy bằng tay (CLI) cực kỳ nhanh, hiếm khi lỗi.
   - **Tại 100M:** Các thao tác ETL có thể mất 30 phút - 1 tiếng và đối diện với nguy cơ Out-of-Memory (OOM) nếu dữ liệu Skew. Nếu bạn đang chạy dở bước Silver sang Gold mà bị sập, Airflow sẽ phát hiện lỗi và khoanh vùng chính xác Task nào bị sập (màu đỏ). Nó có thể tự động Retry (thử lại) hoặc đợi bạn nâng cấp RAM rồi ấn nút **Clear** để chạy tiếp đúng chỗ bị sập, thay vì phải cày lại luồng Bronze từ số 0.
2. **Khả năng Lập lịch Đa luồng (Parallel Orchestration):**
   - Ở mốc 100M, nếu bạn có nhiều quy trình ETL độc lập (ví dụ vừa tạo bảng Báo cáo Công ty, vừa tạo bảng Báo cáo Cá nhân), Airflow Worker có thể cấp phát để 2 tiến trình Spark chạy song song, tận dụng tối đa CPU của Server.
3. **Quản lý Vòng đời Sinh Dữ Liệu:**
   - Image Airflow này đã được cấu hình để "nhìn thấy" trực tiếp thư mục `synthetic_bhxh` khổng lồ của chúng ta. Bạn có thể thay đổi biến `scale` (1M, 10M, 100M) trên giao diện Web, và Airflow sẽ điều hướng Job Spark chọc đúng vào thư mục đó để test hiệu năng cực kỳ tiện lợi.

## 2. Cấu trúc bài thực hành

```text
20-airflow-pyspark-integration/
├── Dockerfile                  # Chứa script cài đặt Java 17 & PySpark lên nền Airflow Image
├── docker-compose.yml          # Triển khai Airflow độc lập với Custom Image (mount synthetic_bhxh)
├── README.md                   
└── dags/
    └── nessie_iceberg_dag.py   # DAG điều phối Data pipeline (từ bài 19)
```

## 3. Hướng dẫn chạy

> **Lưu ý**: Đảm bảo rằng MinIO và Nessie từ file `docker-compose.yml` ở thư mục gốc đang hoạt động, vì các script PySpark cần ghi dữ liệu vào đó.

1. Từ thư mục `exercises/20-airflow-pyspark-integration`, chạy lệnh:
   ```bash
   docker-compose up --build -d
   ```
2. Mở trình duyệt và truy cập Web UI của Airflow tại [http://localhost:8081](http://localhost:8081).
3. Đăng nhập với tài khoản:
   * **Username:** `admin`
   * **Password:** `admin`
4. Bạn có thể Trigger các DAG trong danh sách để theo dõi các job phân tích dữ liệu BHXH tự động chạy.
