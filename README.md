# PySpark Course – Từ cơ bản đến Data Lakehouse

Khóa học thực hành PySpark trên Windows, đi từ các thao tác đọc/ghi dữ liệu cơ bản đến việc dựng một flow Data Lakehouse đơn giản (Bronze → Silver → Gold) và upload dữ liệu lên MinIO.

## 1. Mục tiêu

Sau khi hoàn thành các bài trong repo này, bạn sẽ:

- Biết cách đọc/ghi dữ liệu CSV, JSON bằng Spark và hiểu cách Spark ghi output ra thư mục part-files.
- Thực hành làm sạch dữ liệu (data cleaning) với DataFrame API.
- Sử dụng DataFrame API và Spark SQL để lọc, group, và tính toán aggregation.
- Xây dựng một pipeline dữ liệu theo kiến trúc Lakehouse (Bronze/Silver/Gold).
- Upload dữ liệu đã xử lý lên MinIO (S3-compatible storage) bằng `boto3`.

## 2. Lộ trình học

```text
1. Đọc/ghi dữ liệu cơ bản        (00-read-write-basics)
       ↓
2. Làm sạch dữ liệu               (00b-data-cleaning-practice)
       ↓
3. DataFrame API + Spark SQL      (01-orders-aggregation)
       ↓
4. Aggregation theo province      (01-orders-aggregation)
       ↓
5. Bronze / Silver / Gold          (02-orders-lakehouse)
       ↓
6. Upload output lên MinIO         (02-orders-lakehouse)
```

## 3. Cấu trúc project

```text
pyspark-course/
├── .gitignore
├── .vscode/
│   ├── launch.json
│   └── settings.json
├── docker-compose.yml
├── README.md                          ← file này
└── exercises/
    ├── 00-read-write-basics/
    │   ├── employee.csv
    │   └── employees.json
    │
    ├── 00b-data-cleaning-practice/
    │   └── prac.py
    │
    ├── 01-orders-aggregation/
    │   ├── README.md
    │   ├── dev-requirements.txt
    │   ├── orders.csv
    │   ├── pyproject.toml
    │   ├── spark_orders_exercise.py
    │   ├── pyspark_test/
    │   └── tests/
    │
    └── 02-orders-lakehouse/
        ├── README.md
        ├── orders.csv
        └── spark_orders_lakehouse.py
```

## 4. Vai trò từng bài

| Bài | Nội dung chính | Tài liệu chi tiết |
|---|---|---|
| `00-read-write-basics` | Đọc CSV/JSON, xem schema, ghi output | (xem code trong thư mục) |
| `00b-data-cleaning-practice` | Loại record lỗi, lọc theo khoảng giá trị, xử lý `None`/`NaN` | `prac.py` |
| `01-orders-aggregation` | DataFrame API, Spark SQL, group theo province | [README chi tiết](exercises/01-orders-aggregation/README.md) |
| `02-orders-lakehouse` | Bronze/Silver/Gold, upload MinIO | [README chi tiết](exercises/02-orders-lakehouse/README.md) |

### 00-read-write-basics

Bài nhập môn: đọc `employee.csv` và `employees.json` bằng Spark, in schema bằng `printSchema()`, xem dữ liệu bằng `show()`, sau đó ghi lại ra file để quan sát cách Spark tạo output directory.

### 00b-data-cleaning-practice

Minh họa các thao tác làm sạch DataFrame: tạo DataFrame từ dữ liệu mẫu, loại các record thiếu `name`, lọc theo khoảng giá trị bằng `where()`/`between()`, xử lý giá trị `None`/`NaN` bằng `na.drop()`.

### 01-orders-aggregation

Bài chính về DataFrame API và Spark SQL, dùng dữ liệu `orders.csv` (đơn hàng) để lọc, group theo `province`, tính `order_count`/`total_amount`, và chạy truy vấn tương đương bằng Spark SQL. Xem chi tiết tại [README của bài này](exercises/01-orders-aggregation/README.md).

### 02-orders-lakehouse

Bài nâng cao, mô phỏng flow Data Lakehouse: đọc `orders.csv` → làm sạch và chuẩn hóa (Bronze → Silver) → tổng hợp theo province (Gold) → upload các part-files lên MinIO bằng `boto3`. Xem chi tiết tại [README của bài này](exercises/02-orders-lakehouse/README.md).

## 5. Yêu cầu môi trường

- Python 3.11 (khuyến nghị)
- Java/JDK (Spark yêu cầu JVM)
- PySpark
- boto3 (dùng ở bài `02-orders-lakehouse` để upload lên MinIO)
- pytest (dùng ở bài `01-orders-aggregation` để chạy test)
- Docker Desktop (để chạy MinIO qua `docker-compose.yml`)

### Tạo môi trường ảo (Conda)

```powershell
conda create -n pyspark_env python=3.11 -y
conda activate pyspark_env
pip install pyspark boto3 pytest
```

Bạn cũng có thể dùng `venv` thay cho Conda nếu muốn, miễn là cài đủ các package trên.

## 6. Chạy Docker Compose (MinIO)

`docker-compose.yml` nằm ở root và dùng chung cho toàn bộ project. Trước khi chạy bài `02-orders-lakehouse`, cần khởi động MinIO:

```powershell
docker compose up -d
```

Các service chính trong `docker-compose.yml`:

- **minio** – S3-compatible object storage
  - S3 API: `http://localhost:9000`
  - Web Console: `http://localhost:9001`
  - Username: `admin`
  - Password: `password123`
- **minio-init** – tạo sẵn bucket `warehouse` khi container khởi động
- **nessie** – hiện được cấu hình để phục vụ phần mở rộng kiến trúc Lakehouse/Iceberg trong tương lai; bài hiện tại **chưa** sử dụng Nessie catalog, chỉ dùng MinIO qua S3-compatible API thông qua `boto3`

Bucket `lakehouse-demo` (dùng để lưu output của bài `02-orders-lakehouse`) được script PySpark tự tạo khi upload nếu bucket chưa tồn tại.

## 7. Ghi chú về Spark output directory và part-files

Khi Spark ghi dữ liệu ra (CSV hoặc bất kỳ định dạng nào), nó **không** tạo ra một file duy nhất mà tạo ra một **thư mục**, bên trong gồm:

```text
output/ten-thu-muc/
├── _SUCCESS
├── part-00000-....csv
├── part-00001-....csv
└── .part-*.csv.crc   (checksum, file ẩn)
```

Số lượng file `part-*` phụ thuộc vào số partition của DataFrame lúc ghi. Các thư mục `output/`, `__pycache__/` và file `.pyc` không nên commit vào git (đã có trong `.gitignore`).

## 8. Troubleshooting (Windows)

| Vấn đề | Cách xử lý |
|---|---|
| Docker Desktop chưa chạy | Mở Docker Desktop trước khi `docker compose up -d` |
| Thiếu Java / `JAVA_HOME` chưa set | Cài JDK và set biến môi trường `JAVA_HOME` trỏ tới thư mục cài đặt |
| Thiếu PySpark | `pip install pyspark` trong môi trường ảo đang active |
| Spark tìm `python3` trên Windows nhưng không thấy | Set `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON` bằng `sys.executable` trong script (đã được xử lý trong code mẫu) |
| Lỗi liên quan Hadoop khi ghi file local | Cài `winutils.exe`, sau đó set: |

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
```

| Test không chạy được ở `01-orders-aggregation` | Cài `pytest`: `pip install pytest` |
| Không kết nối được MinIO ở `02-orders-lakehouse` | Kiểm tra `docker compose up -d` đã chạy, và các biến `MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` đúng |

## 9. Tài liệu tham khảo

- [Spark 4.0.1 User Guide](https://spark.apache.org/docs/4.0.1/)
