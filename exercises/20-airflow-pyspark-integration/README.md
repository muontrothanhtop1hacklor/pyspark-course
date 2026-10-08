# Bài 20: Tích hợp Airflow và PySpark (Custom Docker Image)

Trong các bài trước, chúng ta dùng Airflow để điều phối công việc, nhưng worker của Airflow thường không chứa sẵn môi trường Java và PySpark. Điều này dẫn đến việc `BashOperator` khi gọi lệnh `python script_pyspark.py` sẽ gặp lỗi thiếu module hoặc thiếu Java.

Bài này đi sâu vào giải pháp 1: **Tùy biến (Custom) Docker Image của Airflow** để cài đặt sẵn Java 11 và PySpark. Nhờ đó, Airflow worker có thể trực tiếp chạy mã Spark ở chế độ `local`.

## Cấu trúc bài thực hành

```text
20-airflow-pyspark-integration/
├── Dockerfile                  # Chứa script cài đặt Java 11 & PySpark lên nền Airflow Image
├── docker-compose.yml          # Triển khai Airflow độc lập với Custom Image
├── README.md                   
└── dags/
    └── nessie_iceberg_dag.py   # DAG điều phối Data pipeline (từ bài 19)
```

## Các bước thực hiện

1. **Kiểm tra Dockerfile**: File `Dockerfile` kế thừa từ `apache/airflow:2.10.5` sau đó cài đặt thêm `openjdk-11-jre-headless` bằng quyền root, rồi cài đặt `pyspark==4.0.1` qua pip.
2. **Kiểm tra DAG**: DAG `nessie_iceberg_dag.py` được cấu hình để gọi các script của Bài 19 (`19-iceberg-nessie-catalog`). Khác biệt ở đây là nó sử dụng trình thông dịch Python mặc định trong môi trường Airflow.

## Hướng dẫn chạy

> **Lưu ý**: Đảm bảo rằng MinIO và Nessie từ file `docker-compose.yml` ở thư mục gốc (hoặc bài 19) đang hoạt động, vì các script PySpark cần ghi dữ liệu vào đó.

1. Từ thư mục `exercises/20-airflow-pyspark-integration`, chạy lệnh:
   ```bash
   docker-compose up --build -d
   ```
2. Mở trình duyệt và truy cập Web UI của Airflow tại [http://localhost:8081](http://localhost:8081).
3. Đăng nhập với tài khoản:
   * **Username:** `admin`
   * **Password:** `admin`
4. Tìm DAG có tên `nessie_iceberg_git_for_data` và Trigger nó.

Airflow sẽ tự động kéo các script của bài 19 và chạy thành công thông qua `BashOperator` bên trong Container.
