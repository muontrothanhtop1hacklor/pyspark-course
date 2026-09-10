# PySpark Data Lakehouse exercise

Flow thực hành:

`orders.csv -> Bronze raw -> Silver clean -> Gold aggregate -> MinIO`

## Các layer

- **Bronze**: giữ dữ liệu raw, thêm `source_file` và `load_time`.
- **Silver**: bỏ `order_id` rỗng, bỏ `amount <= 0`, chuẩn hóa `status` uppercase,
  cast `order_id`, `amount` và `order_date`.
- **Gold**: group theo `province`, tính `total_orders`, `total_amount`,
  `success_orders`, `failed_orders`.

Spark ghi mỗi layer thành một thư mục gồm các part-file CSV. Script sau đó tạo
bucket `lakehouse-demo` nếu chưa có và upload theo các prefix:

```text
s3://lakehouse-demo/bronze/orders/
s3://lakehouse-demo/silver/orders/
s3://lakehouse-demo/gold/order_summary/
```

## Chạy

Khởi động MinIO từ thư mục gốc:

```powershell
docker compose up -d
```

Cài dependency và chạy:

```powershell
cd C:\Users\Administrator\minio-nessie\iceburg_test_project
python -m pip install -r .\dev-requirements.txt
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
python .\spark_orders_exercise.py
```

Mặc định script dùng `http://localhost:9000`, user `admin`, password
`password123`. Có thể ghi đè bằng `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY` và
`MINIO_SECRET_KEY`.
