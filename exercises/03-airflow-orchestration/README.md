# Airflow orchestration

Bài này minh họa vai trò của Airflow trong pipeline lakehouse. Airflow **không
xử lý dữ liệu trực tiếp như Spark**; Airflow điều phối và lên lịch các job.

## Flow tổng thể

```text
source CSV
    -> Spark xử lý Bronze/Silver/Gold
    -> MinIO lưu dữ liệu
    -> Airflow điều phối các job
    -> Iceberg/Nessie/Data Catalog quản lý table và metadata
```

DAG ở `dags/orders_lakehouse_dag.py` điều phối ba nguồn Bronze chạy song song,
sau đó hội tụ vào Silver và Gold. DAG gọi các runner Spark thật; tuy nhiên
container Airflow hiện tại chưa có PySpark/Java nên trigger trong container sẽ
thất bại theo giới hạn đã biết.

## Mô hình nhiều nguồn

```text
bronze_web_task ─┐
bronze_mobile_task├─> silver_task -> gold_task
bronze_store_task ┘
```

Có thể kiểm chứng pipeline thủ công trên Windows bằng environment `pyspark_env`
(PySpark 4.0.1), từ thư mục root repository:

```powershell
$python = "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe"
$dir = ".\exercises\02-orders-lakehouse"
& $python "$dir\run_bronze_web.py"
& $python "$dir\run_bronze_mobile.py"
& $python "$dir\run_bronze_store.py"
& $python "$dir\run_silver.py"
& $python "$dir\run_gold.py"
```

## Vai trò các thành phần

- **Spark**: đọc, biến đổi, làm sạch và tổng hợp dữ liệu.
- **MinIO**: lưu file/object dạng S3-compatible cho data lake.
- **Iceberg**: table format, quản lý dữ liệu và schema trên lake.
- **Nessie**: catalog có versioning cho table và metadata.
- **Data Catalog**: nơi biết dataset/table nào tồn tại, schema gì và dữ liệu
  nằm ở đâu.
- **Airflow**: điều phối, lên lịch, quản lý dependency và theo dõi log của
  các job.

## Cấu trúc

```text
03-airflow-orchestration/
├── README.md
└── dags/
    └── orders_lakehouse_dag.py
```
