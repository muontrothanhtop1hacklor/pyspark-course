# Orders Data Lakehouse

Bài thực hành mô phỏng flow xử lý dữ liệu đơn hàng:

```text
orders.csv -> Bronze -> Silver -> Gold -> MinIO
```

## Cấu trúc

```text
02-orders-lakehouse/
├── README.md
├── orders.csv
└── spark_orders_lakehouse.py
```

## Các layer

- **Bronze**: đọc dữ liệu raw từ `orders.csv`, thêm `source_file` và `load_time`.
- **Silver**: làm sạch dữ liệu, chuẩn hóa `status`, cast kiểu dữ liệu và loại
  record không hợp lệ.
- **Gold**: tổng hợp theo `province`, gồm `total_orders`, `total_amount`,
  `success_orders` và `failed_orders`.

Output local được ghi vào:

```text
output/lakehouse/bronze/orders/
output/lakehouse/silver/orders/
output/lakehouse/gold/order_summary/
```

Mỗi đường dẫn là một thư mục Spark output chứa các part-file CSV.

## Chạy MinIO

`docker-compose.yml` dùng chung nằm ở root repository:

```powershell
cd C:\Users\Administrator\minio-nessie
docker compose up -d
```

## Chạy bài thực hành

```powershell
cd C:\Users\Administrator\minio-nessie\exercises\02-orders-lakehouse
python -m pip install pyspark boto3
python .\spark_orders_lakehouse.py
```

Trên Windows, nếu Spark yêu cầu Hadoop native helper, cấu hình thêm:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
```

## Upload lên MinIO

Script mặc định kết nối tới:

```text
Endpoint: http://localhost:9000
Access key: admin
Secret key: password123
Bucket: lakehouse-demo
```

Các object được upload theo prefix:

```text
s3://lakehouse-demo/bronze/orders/
s3://lakehouse-demo/silver/orders/
s3://lakehouse-demo/gold/order_summary/
```

Có thể ghi đè cấu hình bằng các biến môi trường:

```powershell
$env:MINIO_ENDPOINT = "http://localhost:9000"
$env:MINIO_ACCESS_KEY = "admin"
$env:MINIO_SECRET_KEY = "password123"
```
