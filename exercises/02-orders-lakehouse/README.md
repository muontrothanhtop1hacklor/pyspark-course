# 02 – Orders Lakehouse (Bronze → Silver → Gold → MinIO)

Bài nâng cao, mô phỏng một flow **Data Lakehouse** đơn giản: đọc `orders.csv`, xử lý qua ba lớp **Bronze → Silver → Gold**, rồi upload output lên **MinIO** bằng `boto3`.

```text
orders.csv
    ↓
Bronze   (raw + metadata)
    ↓
Silver   (làm sạch, chuẩn hóa)
    ↓
Gold     (tổng hợp theo province)
    ↓
MinIO    (upload qua boto3)
```

## 1. Mục tiêu

- Hiểu kiến trúc Lakehouse theo lớp Bronze/Silver/Gold.
- Thực hành làm sạch và chuẩn hóa dữ liệu thực tế (dữ liệu "bẩn": sai định dạng số, sai chính tả, thiếu trường...).
- Tổng hợp dữ liệu theo `province` ở lớp Gold.
- Upload output (các part-files do Spark tạo ra) lên MinIO qua S3-compatible API.

## 2. Cấu trúc

```text
02-orders-lakehouse/
├── README.md                     ← file này
├── orders.csv
└── spark_orders_lakehouse.py
```

`docker-compose.yml` dùng để chạy MinIO **nằm ở root của repository**, không nằm riêng trong thư mục này — xem hướng dẫn ở bước "Cách chạy" bên dưới.

## 3. Script chính: `spark_orders_lakehouse.py`

### Bronze layer

- Đọc dữ liệu raw từ `orders.csv` bằng PySpark.
- Thêm các cột metadata: `source_file`, `load_time`, `ingest_seq`.
- Ghi output local vào `output/lakehouse/bronze/orders/`.

### Silver layer

Làm sạch và chuẩn hóa dữ liệu Bronze:

- Parse `order_id`; loại các record thiếu hoặc `order_id` không hợp lệ.
- Chuẩn hóa `customer_id`.
- Chuẩn hóa tên `province`, hỗ trợ các biến thể như: `Hanoi`, `Ha Noi`, `HN`, `Da Nang`, `Danang`, `Ho Chi Minh`, `Sai Gon`, `Can Tho`.
- Parse `amount`:
  - Xử lý giá trị có dấu phẩy, ví dụ `1,250,000`.
  - Xử lý giá trị có hậu tố `k`, ví dụ `200k`.
  - Loại các giá trị không parse được hoặc `amount <= 0`.
- Chuẩn hóa `status` thành chữ hoa, sửa một số lỗi chính tả thường gặp:
  - `SUCCES` → `SUCCESS`
  - `FAIL` → `FAILED`
  - `CANCELED` → `CANCELLED`
- Parse `order_date`; loại các record có ngày không hợp lệ.
- Deduplicate theo `order_id`, giữ lại record được nạp sớm nhất dựa trên `ingest_seq`.
- Ghi output local vào `output/lakehouse/silver/orders/`.

### Gold layer

- Group theo `province`.
- Tính các cột tổng hợp: `total_orders`, `total_amount`, `success_orders`, `failed_orders`.
- Ghi output local vào `output/lakehouse/gold/order_summary/`.

### Upload MinIO

Script dùng `boto3` (S3-compatible API) để:

1. Kết nối tới MinIO.
2. Tạo bucket `lakehouse-demo` nếu bucket chưa tồn tại.
3. Upload các part-files do Spark tạo ra ở cả ba lớp lên các prefix tương ứng:

```text
s3://lakehouse-demo/bronze/orders/
s3://lakehouse-demo/silver/orders/
s3://lakehouse-demo/gold/order_summary/
```

> Lưu ý: việc upload dùng `boto3` gọi trực tiếp S3 API, **không** phải Spark ghi trực tiếp qua S3A connector. Iceberg/Nessie hiện **chưa** được dùng trong pipeline này — Nessie trong `docker-compose.yml` chỉ đang được chuẩn bị sẵn cho phần mở rộng kiến trúc Lakehouse/Iceberg trong tương lai.

## 4. Cấu hình MinIO

Giá trị mặc định trong script:

```text
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123
```

Có thể ghi đè bằng biến môi trường trước khi chạy:

```powershell
$env:MINIO_ENDPOINT = "http://localhost:9000"
$env:MINIO_ACCESS_KEY = "admin"
$env:MINIO_SECRET_KEY = "password123"
```

## 5. Yêu cầu môi trường

- Python 3.11 (cùng môi trường Conda/venv của toàn project)
- Java/JDK
- PySpark
- boto3
- Docker Desktop (để chạy MinIO)

```powershell
pip install pyspark boto3
```

## 6. Cách chạy

1. Về thư mục root của repository và khởi động MinIO:

```powershell
cd C:\đường-dẫn-tới\pyspark-course
docker compose up -d
```

2. Chuyển vào thư mục bài này:

```powershell
cd .\exercises\02-orders-lakehouse
```

3. Cài PySpark và boto3 (nếu chưa cài):

```powershell
pip install pyspark boto3
```

4. Nếu Windows báo lỗi Hadoop khi ghi file local, cấu hình `HADOOP_HOME`:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
```

5. Chạy script:

```powershell
python .\spark_orders_lakehouse.py
```

## 7. Kết quả mong đợi

- Ba thư mục output local, mỗi thư mục chứa `_SUCCESS`, các file `part-*.csv` (hoặc định dạng tương ứng) và file checksum — vì Spark luôn ghi ra một **thư mục part-files**, không phải một file duy nhất:

```text
output/lakehouse/bronze/orders/
output/lakehouse/silver/orders/
output/lakehouse/gold/order_summary/
```

- Các file trên được upload lên MinIO. Bạn có thể kiểm tra qua MinIO Console tại `http://localhost:9001` (user `admin` / password `password123`), trong bucket `lakehouse-demo`.

## 8. Troubleshooting

| Vấn đề | Cách xử lý |
|---|---|
| Docker Desktop chưa chạy | Mở Docker Desktop, sau đó `docker compose up -d` ở root |
| Không kết nối được MinIO (`Connection refused`) | Kiểm tra container `minio` đang chạy (`docker ps`), kiểm tra `MINIO_ENDPOINT` đúng cổng `9000` |
| Lỗi xác thực khi upload (`Access Denied`) | Kiểm tra `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` khớp với cấu hình trong `docker-compose.yml` (mặc định `admin`/`password123`) |
| Bucket không tồn tại | Script tự tạo bucket `lakehouse-demo` nếu chưa có; nếu vẫn lỗi, kiểm tra quyền tài khoản MinIO |
| Lỗi Hadoop khi ghi file local trên Windows | Cài `winutils.exe`, set `HADOOP_HOME` và thêm vào `Path` |
| Thiếu Java / `JAVA_HOME` | Cài JDK và set biến môi trường `JAVA_HOME` |

## 9. Mở rộng nhiều nguồn cho bài Airflow

Bài 2 gốc giữ nguyên pipeline đơn nguồn:

```text
orders.csv -> Bronze -> Silver -> Gold -> MinIO
```

Để phục vụ bài 3 về Airflow orchestration, repository bổ sung một pipeline
batch nhiều nguồn riêng biệt:

```text
orders_web.csv    ─┐
orders_mobile.csv ├─> Bronze riêng từng nguồn -> Union -> Silver -> Gold
orders_store.csv  ─┘
```

Các runner mở rộng gồm:

- `run_bronze_web.py`
- `run_bronze_mobile.py`
- `run_bronze_store.py`
- `run_silver.py`
- `run_gold.py`
- `lakehouse_batch_common.py`

Ba runner Bronze dùng chung hàm trong `lakehouse_batch_common.py` để tránh
lặp logic. Các bước Silver và Gold import trực tiếp `build_silver()`,
`build_gold()` và `upload_directory_to_minio()` từ
`spark_orders_lakehouse.py`, không sao chép lại logic của bài 2 gốc.

Pipeline mở rộng ghi thêm Parquet trung gian tại:

```text
output/bronze/web/
output/bronze/mobile/
output/bronze/store/
output/silver/
```

Phần điều phối bằng Airflow nằm trong
`exercises/03-airflow-orchestration/`, với ba task Bronze hội tụ vào Silver
và sau đó chạy Gold.
## 10. Quá trình phát triển và tư duy thiết kế

Phần mở rộng này được xây dựng theo các bước, với ưu tiên là không làm hỏng
pipeline bài 2 đang chạy được:

1. **Xác định vấn đề môi trường:** bài Iceberg cần Spark 3.5.x, trong khi các
   bài lakehouse hiện tại dùng PySpark 4.0.1. Vì vậy môi trường Iceberg được
   tách riêng, còn `pyspark_env` được giữ cho pipeline Spark 4.0.1.
2. **Giữ nguyên bài 2 gốc:** `spark_orders_lakehouse.py` vẫn xử lý
   `orders.csv` theo flow đơn nguồn và tiếp tục là nơi chứa logic
   `build_silver()`, `build_gold()` và upload MinIO.
3. **Mở rộng theo nhu cầu Airflow:** thay vì nhồi thêm nhiều nguồn vào một
   lần chạy, tạo ba nguồn batch tĩnh `web`, `mobile` và `store`. Mỗi nguồn có
   một Bronze job độc lập, có `source_channel` để truy vết nguồn dữ liệu.
4. **Tách phần dùng chung:** logic tạo Spark session, đọc raw schema, tạo
   Bronze, ghi Parquet và upload được đưa vào
   `lakehouse_batch_common.py`. Các runner riêng chỉ chọn nguồn và channel,
   tránh sao chép logic.
5. **Tạo điểm giao giữa các layer:** Bronze ghi Parquet trung gian để
   `run_silver.py` có thể union cả ba nguồn. Silver và Gold vẫn gọi lại các
   hàm xử lý của bài 2, nhờ đó quy tắc làm sạch và tổng hợp chỉ có một nơi
   chịu trách nhiệm.
6. **Đưa orchestration ra Airflow:** DAG dùng `TaskGroup` cho ba Bronze task,
   sau đó hội tụ vào Silver và Gold. Như vậy code xử lý dữ liệu vẫn thuộc về
   Spark, còn Airflow chỉ điều phối thứ tự, dependency và trạng thái.

Kết quả là có hai cách chạy cùng tồn tại: chạy từng script trực tiếp bằng
`pyspark_env` trên host để học và kiểm thử, hoặc để Airflow container gọi các
script đó qua DAG. Đây là mở rộng thêm cho bài 3, không phải thay thế pipeline
đơn nguồn của bài 2.
