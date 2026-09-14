# Bài tự thực hành: Orders Data Lakehouse

Mục tiêu của bài này là tự viết lại flow:

```text
orders.csv -> Bronze -> Silver -> Gold -> MinIO
```

Không xem `spark_orders_lakehouse.py` ngay từ đầu. Hãy tự hoàn thành từng
bước trong một file mới, ví dụ:

```text
practice_orders_lakehouse.py
```

## 1. Chuẩn bị môi trường

Từ thư mục root repository:

```powershell
docker compose up -d
```

Kiểm tra MinIO:

- API: http://localhost:9000
- Console: http://localhost:9001
- User: `admin`
- Password: `password123`

Chuyển vào thư mục bài học:

```powershell
cd .\exercises\02-orders-lakehouse
python -m pip install pyspark boto3
```

Trên Windows, nếu Spark báo lỗi Hadoop khi ghi file:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
```

## 2. Tạo SparkSession

Tự viết phần import và khởi tạo Spark:

- Import `os`, `sys`, `Path`.
- Cấu hình `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON` bằng
  `sys.executable`.
- Tạo `SparkSession` với `master("local[*]")`.
- Đặt log level thành `WARN`.
- Đảm bảo `spark.stop()` luôn được gọi sau khi chạy xong.

## 3. Đọc dữ liệu đầu vào

Đọc file `orders.csv` bằng schema dạng chuỗi để Bronze giữ được dữ liệu raw.

Các cột đầu vào:

```text
order_id, customer_id, province, amount, status, order_date
```

Checklist:

- Bật `header`.
- Khai báo `StructType`.
- Đọc CSV bằng `spark.read`.
- In schema và một vài dòng dữ liệu.

## 4. Bronze layer

Tạo DataFrame Bronze từ dữ liệu raw.

Thêm các cột:

```text
source_file
load_time
ingest_seq
```

Gợi ý:

- `source_file` lấy từ tên file đầu vào.
- `load_time` lấy thời điểm UTC hiện tại.
- `ingest_seq` có thể tạo bằng `monotonically_increasing_id()`.

Ghi Bronze ra:

```text
output/lakehouse/bronze/orders/
```

Lưu ý: Spark ghi ra một thư mục gồm nhiều part-file, không phải một file CSV
duy nhất.

## 5. Silver layer

Viết một hàm có dạng:

```python
def build_silver(bronze):
    ...
```

Hàm này cần làm các việc sau:

### Parse và chuẩn hóa

- Trim các cột chuỗi.
- Cast `order_id` sang số nguyên hoặc số nguyên lớn.
- Uppercase `customer_id`.
- Bỏ dấu `-` không cần thiết trong `customer_id`.
- Chuẩn hóa province về các tên thống nhất:
  - `Hanoi`
  - `Da Nang`
  - `Ho Chi Minh`
  - `Can Tho`
- Uppercase `status`.

### Làm sạch amount

Xử lý được các trường hợp:

```text
125000
1,250,000
200k
abc
NULL
```

Yêu cầu:

- Bỏ dấu phẩy trước khi cast.
- Nhận diện hậu tố `k` hoặc `K`.
- Đổi `200k` thành `200000`.
- Giá trị không parse được phải thành `NULL`.
- Chỉ giữ `amount > 0`.

### Làm sạch ngày

- Parse `order_date` theo định dạng `yyyy-MM-dd`.
- Giá trị sai định dạng phải thành `NULL`.
- Chỉ giữ record có `order_date` hợp lệ.

### Lọc record lỗi

Chỉ giữ record thỏa tất cả điều kiện:

```python
order_id is not null
customer_id is not null
province is not null
amount is not null
amount > 0
order_date is not null
```

### Khử trùng

- Deduplicate theo `order_id`.
- Nếu có nhiều record cùng `order_id`, giữ record có `ingest_seq` nhỏ nhất.

Ghi Silver ra:

```text
output/lakehouse/silver/orders/
```

## 6. Gold layer

Viết một hàm có dạng:

```python
def build_gold(silver):
    ...
```

Group theo:

```text
province
```

Tính các cột:

```text
total_orders
total_amount
success_orders
failed_orders
```

Ghi Gold ra:

```text
output/lakehouse/gold/order_summary/
```

## 7. Upload lên MinIO

Viết hàm upload một thư mục local lên MinIO bằng `boto3`.

Yêu cầu:

- Kết nối tới endpoint `http://localhost:9000`.
- Dùng access key `admin`.
- Dùng secret key `password123`.
- Tạo bucket `lakehouse-demo` nếu bucket chưa tồn tại.
- Duyệt tất cả file bên trong thư mục output.
- Upload giữ nguyên cấu trúc prefix.

Các đích cần upload:

```text
output/lakehouse/bronze/orders/
    -> s3://lakehouse-demo/bronze/orders/

output/lakehouse/silver/orders/
    -> s3://lakehouse-demo/silver/orders/

output/lakehouse/gold/order_summary/
    -> s3://lakehouse-demo/gold/order_summary/
```

## 8. Hàm `main`

Trong `main`, triển khai theo đúng thứ tự:

```text
1. Xác định project directory
2. Đọc orders.csv
3. Tạo Bronze
4. Ghi Bronze
5. Tạo Silver từ Bronze
6. Ghi Silver
7. Tạo Gold từ Silver
8. Ghi Gold
9. In kết quả từng layer
10. Upload cả ba layer lên MinIO
11. Stop Spark
```

## 9. Kiểm tra kết quả

Trước khi upload, hãy kiểm tra:

- Bronze còn dữ liệu raw.
- Silver không còn `order_id` rỗng.
- Silver không còn `amount <= 0`.
- Silver không còn ngày lỗi.
- Status đã được uppercase.
- Không còn duplicate `order_id`.
- Gold có đủ bốn cột thống kê.

Kiểm tra output local:

```powershell
Get-ChildItem .\output\lakehouse -Recurse
```

Kiểm tra container:

```powershell
docker compose ps
```

Kiểm tra object trên MinIO bằng Console hoặc MinIO Client.

## 10. Checklist hoàn thành

- [ ] Tạo SparkSession.
- [ ] Đọc CSV với schema.
- [ ] Tạo Bronze.
- [ ] Thêm `source_file`, `load_time`, `ingest_seq`.
- [ ] Ghi Bronze.
- [ ] Viết `build_silver`.
- [ ] Parse và làm sạch amount.
- [ ] Parse order date.
- [ ] Lọc record lỗi.
- [ ] Deduplicate order.
- [ ] Ghi Silver.
- [ ] Viết `build_gold`.
- [ ] Ghi Gold.
- [ ] Tạo bucket MinIO.
- [ ] Upload Bronze.
- [ ] Upload Silver.
- [ ] Upload Gold.
- [ ] Kiểm tra kết quả cuối cùng.

## 11. Câu hỏi tự kiểm tra

1. Vì sao Bronze nên giữ dữ liệu gần với raw input?
2. Vì sao không nên làm sạch trực tiếp trên file nguồn?
3. Vì sao Spark ghi ra thư mục thay vì một file CSV duy nhất?
4. Vì sao cần parse `amount` trước khi tính tổng?
5. Vì sao cần chuẩn hóa `status` trước khi đếm `SUCCESS` và `FAILED`?
6. Vì sao cần `ingest_seq` khi deduplicate?
7. Bucket MinIO đóng vai trò gì trong flow Data Lakehouse?
8. Silver khác Gold ở điểm nào?
