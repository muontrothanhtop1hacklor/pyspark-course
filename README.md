# PySpark Course - From Spark Basics to Lakehouse Orchestration

Khóa học thực hành PySpark trên Windows, đi từ đọc/ghi dữ liệu cơ bản đến
DataFrame API, Spark SQL, Lakehouse Bronze/Silver/Gold, MinIO và Airflow.

## Mục tiêu

- Đọc và ghi CSV/JSON bằng Spark, hiểu output dạng thư mục part-file.
- Làm sạch dữ liệu bằng DataFrame API.
- Lọc, group và aggregation bằng DataFrame API và Spark SQL.
- Xây dựng pipeline Bronze/Silver/Gold.
- Upload output lên MinIO bằng `boto3`.
- Dùng Airflow điều phối nhiều job Spark theo dependency.

## Cấu trúc

```text
pyspark-course/
├── docker-compose.yml
├── docker/
│   └── airflow/
│       └── Dockerfile
├── exercises/
│   ├── 00-read-write-basics/
│   ├── 00b-data-cleaning-practice/
│   ├── 01-orders-aggregation/
│   ├── 02-orders-lakehouse/
│   └── 03-airflow-orchestration/
└── README.md
```

## Môi trường host

Khuyến nghị dùng Python 3.11 qua Conda:

```powershell
conda create -n pyspark_env python=3.11 -y
conda activate pyspark_env
pip install pyspark==4.0.1 boto3 pytest
```

Java/JDK 17 hoặc 21 là cần thiết cho Spark 4.0.1.

## Chạy Orders Lakehouse

Khởi động MinIO và Nessie:

```powershell
docker compose up -d
```

MinIO API chạy tại `http://localhost:9000`, console tại
`http://localhost:9001`. Tài khoản MinIO là `admin/password123`.

Pipeline nhiều nguồn có thể chạy thủ công bằng host environment:

```powershell
$python = "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe"
$dir = ".\exercises\02-orders-lakehouse"
& $python "$dir\run_bronze_web.py"
& $python "$dir\run_bronze_mobile.py"
& $python "$dir\run_bronze_store.py"
& $python "$dir\run_silver.py"
& $python "$dir\run_gold.py"
```

Các output trung gian nằm tại:

```text
exercises/02-orders-lakehouse/output/bronze/web/
exercises/02-orders-lakehouse/output/bronze/mobile/
exercises/02-orders-lakehouse/output/bronze/store/
exercises/02-orders-lakehouse/output/silver/
```

Output CSV được upload vào bucket `lakehouse-demo`. Endpoint mặc định là
`http://localhost:9000` khi chạy trên host; trong Airflow container Compose
override endpoint thành `http://minio:9000`.

## Airflow orchestration

DAG nằm tại:

```text
exercises/03-airflow-orchestration/dags/orders_lakehouse_dag.py
```

Flow:

```text
bronze_web_task ─┐
bronze_mobile_task├──> silver_task -> gold_task
bronze_store_task ┘
```

Ba task Bronze nằm trong `TaskGroup` và hội tụ vào Silver. Airflow container
được build từ `docker/airflow/Dockerfile`, bao gồm Java 17, PySpark 4.0.1 và
`boto3`. Đăng nhập Airflow tại `http://localhost:8080` bằng:

```text
Username: admin
Password: admin
```

Executor hiện tại là `SequentialExecutor`, nên DAG giữ đúng dependency nhưng
ba Bronze sẽ được thực thi tuần tự. Đổi sang executor hỗ trợ concurrency nếu
cần chạy đồng thời thật.

## MinIO, Iceberg và Nessie

- `minio`: S3-compatible object storage, API `http://localhost:9000`.
- `minio-init`: tạo bucket `warehouse`.
- `nessie`: catalog cho phần mở rộng Iceberg.
- `lakehouse-demo`: bucket được các script PySpark tự tạo khi upload.

## Spark output và troubleshooting

Spark ghi CSV/Parquet thành thư mục gồm `_SUCCESS`, các file `part-*` và
checksum, không phải một file duy nhất. `output/`, `__pycache__/` và `.pyc`
đã được loại khỏi Git bằng `.gitignore`.

Nếu Spark trên Windows cần Hadoop native helper:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
```

Nếu không kết nối được MinIO, kiểm tra `docker compose up -d` và các biến
`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`.

## Tài liệu tham khảo

- [Spark 4.0.1 User Guide](https://spark.apache.org/docs/4.0.1/)
- [Orders Lakehouse README](./exercises/02-orders-lakehouse/README.md)
- [Airflow orchestration README](./exercises/03-airflow-orchestration/README.md)
