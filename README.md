# PySpark Course — From Spark Basics to Lakehouse Orchestration

Khóa học thực hành PySpark theo lộ trình từ đọc/ghi dữ liệu, DataFrame API,
Spark SQL đến Data Lakehouse và Airflow orchestration.

Tài liệu tham khảo chính: [PySpark User Guide (Spark 4.0.1)](https://spark.apache.org/docs/4.0.1/api/python/user_guide/index.html)
- Chapter 1 — DataFrames: Create, View, Manipulation
- Chapter 3 — Function Junction: Clean / Transform / Summarize data
- Chapter 6 — Old SQL, New Tricks: Running SQL with PySpark
- Chapter 7 — Load and Behold: Reading & Writing Data

## Mục tiêu

Hiểu các flow:

```text
đọc/tạo dữ liệu → xử lý bằng Spark → ghi kết quả
orders.csv → Bronze → Silver → Gold → MinIO
Spark jobs → Airflow điều phối theo dependency
```

## Cấu trúc project

```
pyspark-course/
├── docker-compose.yml         # MinIO, MinIO init và Nessie dùng chung
├── exercises/
│   ├── 00-read-write-basics/  # Đọc/ghi CSV và JSON
│   ├── 00b-data-cleaning-practice/
│   │                           # Các thao tác làm sạch DataFrame
│   ├── 01-orders-aggregation/ # DataFrame API và Spark SQL
│   ├── 02-orders-lakehouse/   # Bronze/Silver/Gold và upload MinIO
│   └── 03-airflow-orchestration/
│                               # Airflow mô phỏng điều phối pipeline
└── README.md
```

## Lộ trình học

1. [Đọc/ghi dữ liệu cơ bản](./exercises/00-read-write-basics/): làm quen với
   CSV và JSON.
2. [Data cleaning](./exercises/00b-data-cleaning-practice/): xử lý null,
   NaN và lọc dữ liệu bằng DataFrame API.
3. [Orders aggregation](./exercises/01-orders-aggregation/): đọc đơn hàng,
   `select`, `filter`, `groupBy`, aggregation và Spark SQL.
4. [Orders lakehouse](./exercises/02-orders-lakehouse/): xây dựng Bronze,
   Silver, Gold và lưu output lên MinIO.
5. [Airflow orchestration](./exercises/03-airflow-orchestration/): điều phối
   tuần tự Bronze → Silver → Gold. DAG hiện tại chỉ mô phỏng task bằng log,
   chưa chạy Spark hay xử lý dữ liệu thật.

## MinIO, Iceberg và Nessie

`docker-compose.yml` ở root cung cấp MinIO và Nessie dùng chung. MinIO đóng
vai trò object storage S3-compatible. Bài lakehouse dùng MinIO để lưu output;
Iceberg/Nessie là hướng mở rộng để quản lý table và metadata. Bài Airflow tập
trung vào orchestration, không thay thế Spark trong việc xử lý dữ liệu.

Khởi động các service:

```powershell
docker compose up -d
```

MinIO API chạy tại `http://localhost:9000`, console tại
`http://localhost:9001`.

## Yêu cầu môi trường

- Python (khuyến nghị dùng qua **conda/venv riêng**, không dùng Python hệ thống)
- Java 8/11/17 (Spark cần JDK, không chạy được nếu chỉ có JRE hoặc thiếu `JAVA_HOME`)
- PySpark (cài theo README của từng bài)

Cài đặt nhanh:

```bash
conda create -n pyspark_env python=3.10 -y
conda activate pyspark_env
pip install pyspark==4.0.1 boto3 pytest
```

## Chạy bài orders aggregation

```powershell
cd .\exercises\01-orders-aggregation
python .\spark_orders_exercise.py
```

Script sẽ:
1. Tạo `SparkSession`
2. Tạo DataFrame mock + đọc `orders.csv`, gộp lại bằng `unionByName`
3. In schema, show dữ liệu, select cột, filter `status = SUCCESS`
4. Group by `province`, tính `count` order và `sum amount`
5. Tạo temp view, chạy SQL tương đương
6. Ghi kết quả ra `output/orders_by_province` (CSV)

Chi tiết xem [README của bài aggregation](./exercises/01-orders-aggregation/README.md).

## Chạy bài lakehouse

```powershell
cd .\exercises\02-orders-lakehouse
python -m pip install pyspark boto3
python .\spark_orders_lakehouse.py
```

Chi tiết flow Bronze/Silver/Gold xem [README của bài lakehouse](./exercises/02-orders-lakehouse/README.md).

## Chạy bài Airflow

DAG nằm tại:

```text
exercises/03-airflow-orchestration/dags/orders_lakehouse_dag.py
```

DAG có ba task tuần tự:

```text
bronze_task → silver_task → gold_task
```

Mỗi task chỉ in log để minh họa orchestration. Cài Airflow bằng Docker Compose
hoặc chạy trong WSL/Linux, mount thư mục `dags/` vào Airflow scheduler rồi
trigger DAG `orders_lakehouse_pipeline` thủ công trên UI.

Chi tiết xem [README của bài Airflow](./exercises/03-airflow-orchestration/README.md).

---

## Các lỗi gặp phải khi chạy Spark và kinh nghiệm rút ra

**1. `Python worker failed to connect back` / Windows gợi ý tải Python từ Microsoft Store**
Windows chặn lệnh `python` bằng "App Execution Alias" giả. → Tắt ở Settings → Apps → Advanced app settings → App execution aliases, và khai báo rõ interpreter trong code, **trước khi** tạo `SparkSession`:
```python
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

**2. `Cannot run program "python3": ... cannot find the file specified`**
Windows/Conda chỉ có `python.exe`, không có `python3.exe`, nhưng Spark mặc định tìm `python3` kiểu Linux/Mac. → Set `PYSPARK_PYTHON` như trên (không phải do App Execution Alias nên tắt nó không giải quyết được lỗi này).

**3. `HADOOP_HOME and hadoop.home.dir are unset` khi `.write().csv()`**
Đọc/`.show()` thì không sao, nhưng ghi file ra ổ đĩa Windows thì Spark cần `winutils.exe` để giả lập filesystem kiểu Hadoop. → Tải `winutils.exe` + `hadoop.dll` từ `github.com/cdarlint/winutils`, bỏ vào `C:\...\hadoop\bin\`, set `HADOOP_HOME` và thêm `%HADOOP_HOME%\bin` vào `Path`, rồi mở lại terminal.

**4. `ModuleNotFoundError: No module named 'pyspark'`**
Terminal đang ở env `base` chứ chưa activate đúng env cài pyspark. → `conda activate pyspark_env` rồi `pip install pyspark`.

---

## Lưu ý về thư mục output

Khi ghi CSV, Spark không tạo 1 file duy nhất mà tạo cả folder gồm `_SUCCESS`, các file `.crc` (checksum) và `part-00000-*.csv` (mỗi partition ghi 1 file) — đây là hành vi bình thường, không phải lỗi. Nếu muốn ra đúng 1 file CSV, gọi `.coalesce(1)` trước khi `.write`. Nên thêm `.gitignore` để không commit nhầm các file này và `__pycache__/`:

```
__pycache__/
*.pyc
output/
.venv/
```
