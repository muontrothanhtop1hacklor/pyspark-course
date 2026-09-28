# PySpark Course - From Spark Basics to Lakehouse Orchestration

Khóa học và lộ trình thực hành PySpark toàn diện trên Windows: đi từ đọc/ghi dữ liệu cơ bản, Data Types & Schema Control, Joins, Window Functions, Partitioning, UDFs đến Full ETL Pipeline, Data Lakehouse (Bronze/Silver/Gold/MinIO/Iceberg) và Workflow Orchestration với Airflow.

---

## 1. Mục tiêu khóa học

- **Cơ bản:** Khởi tạo SparkSession, đọc/ghi CSV, JSON, Parquet, hiểu output dạng thư mục part-file và commit flag `_SUCCESS`.
- **Làm sạch & Kiểm soát kiểu dữ liệu:** Khai báo schema thủ công (`StructType`), cast dữ liệu, kiểm soát lỗi khi cast (ANSI mode), xử lý dữ liệu khuyết (`na.drop`, `na.fill`).
- **Phép toán cốt lõi:** Filter, groupBy, aggregation bằng DataFrame API và Spark SQL.
- **Quan hệ & Join:** Inner join, left join, tách và kiểm soát dữ liệu không mapping được (unmapped/orphan records).
- **Window Functions & Dedup:** Khử trùng lặp theo mốc thời gian mới nhất (`row_number() == 1`), xếp hạng (`rank()`, `dense_rank()`), audit reconciliation.
- **Tối ưu & Phân vùng:** Hiểu sâu `repartition` (shuffle) vs `coalesce` (no shuffle) vs `partitionBy` (ổ đĩa), xử lý data skew, điều chỉnh `shuffle.partitions`.
- **Hàm tự định nghĩa (UDF):** So sánh hiệu năng thực tế trên 1 triệu dòng giữa Built-in Functions, Python UDF, Vectorized Pandas UDF (`@pandas_udf`) và UDTF (`@udtf` với `LATERAL`).
- **Quy trình ETL hoàn chỉnh:** Xây dựng pipeline đọc thô → kiểm tra schema → phân loại lỗi (quarantine) → dedup → join → transform → aggregate → partitioned write → round-trip validation.
- **Data Lakehouse & Điều phối:** Xây dựng kiến trúc 3 lớp Bronze → Silver → Gold, upload MinIO bằng `boto3`, tích hợp Nessie/Iceberg và điều phối DAGs đa nguồn bằng Apache Airflow.

---

## 2. Cấu trúc thư mục bài tập (`exercises/`)

```text
pyspark-course/
├── docker-compose.yml
├── docker/
│   └── airflow/
│       └── Dockerfile
├── exercises/
│   ├── 00-read-write-basics/             ← Đọc/ghi CSV & JSON, tìm hiểu part-files
│   ├── 00b-data-cleaning-practice/       ← Làm sạch cơ bản: na.drop, lọc khoảng giá trị
│   ├── 01-orders-aggregation/            ← DataFrame API & Spark SQL aggregation
│   ├── 02-orders-lakehouse/              ← Lakehouse Bronze/Silver/Gold + MinIO + Iceberg
│   ├── 03-airflow-orchestration/         ← Điều phối Airflow DAG đa nguồn dữ liệu
│   ├── 04-data-types/                    ← Kiểu dữ liệu, StructType thủ công, cast & ANSI
│   ├── 05-joins/                         ← Inner join, left join & tách unmapped records
│   ├── 06-customer-transactions/         ← Window functions, dedup mới nhất, top-N, SCD
│   ├── 07-partitioning-performance/      ← repartition vs coalesce vs partitionBy & skew
│   ├── 08-read-write-advance/            ← Schema tự khai báo, quarantine lỗi, write modes
│   ├── 09-full-etl-pipeline/             ← Flow PySpark hoàn chỉnh 10 bước khép kín
│   └── 10-udf-pandas-udtf/               ← Benchmark 1M dòng: UDF vs Pandas UDF vs Built-in
├── logs/                                 ← Mẫu nhật ký học tập theo từng buổi
└── README.md
```

---

## 3. Bản đồ lộ trình học tập (Learning Roadmap)

| STT | Thư mục | Chủ đề trọng tâm | Dữ liệu chính |
|:---:|---|---|---|
| **00** | [`00-read-write-basics`](./exercises/00-read-write-basics/README.md) | SparkSession, đọc/ghi CSV, JSON, Parquet, part-files | `employee.csv`, `employees.json` |
| **00b** | [`00b-data-cleaning-practice`](./exercises/00b-data-cleaning-practice/README.md) | Xử lý NULL/NaN, `na.drop`, `between` filter | In-memory DataFrame |
| **01** | [`01-orders-aggregation`](./exercises/01-orders-aggregation/README.md) | DataFrame API vs Spark SQL, groupBy, agg, orderBy | `orders.csv` |
| **02** | [`02-orders-lakehouse`](./exercises/02-orders-lakehouse/README.md) | Kiến trúc Bronze/Silver/Gold, MinIO S3, Iceberg | `orders_{web,mobile,store}.csv` |
| **03** | [`03-airflow-orchestration`](./exercises/03-airflow-orchestration/README.md) | Điều phối DAG Airflow, TaskGroups, Docker Airflow | Airflow DAGs |
| **04** | [`04-data-types`](./exercises/04-data-types/README.md) | Schema thủ công, String/Int/Decimal/Date/Timestamp, cast | `raw_orders_types.csv` |
| **05** | [`05-joins`](./exercises/05-joins/README.md) | Inner vs Left join, bắt bản ghi unmapped/mồ côi | `orders.csv` x `customers.csv` |
| **06** | [`06-customer-transactions`](./exercises/06-customer-transactions/README.md) | Window `row_number()` dedup, `rank()` top-3, audit reconciliation | `customers.csv` x `transactions.csv` |
| **07** | [`07-partitioning-performance`](./exercises/07-partitioning-performance/README.md) | `repartition` vs `coalesce` vs `partitionBy`, data skew | `orders.csv` (200k dòng) |
| **08** | [`08-read-write-advance`](./exercises/08-read-write-advance/README.md) | Phân loại lỗi `error_reason`, overwrite/append/partitionBy | `orders_dirty.csv` (2k dòng) |
| **09** | [`09-full-etl-pipeline`](./exercises/09-full-etl-pipeline/README.md) | Full Flow: read → clean → validate → dedup → join → aggregate → write | `orders_dup.csv` (3.15k dòng) |
| **10** | [`10-udf-pandas-udtf`](./exercises/10-udf-pandas-udtf/README.md) | So sánh hiệu năng UDF vs Built-in vs Pandas UDF vs UDTF | `customers_1m.csv` (1M dòng) |

---

## 4. Môi trường chạy trên Windows

Khuyến nghị sử dụng Python 3.11 qua Conda:

```powershell
conda create -n pyspark_env python=3.11 -y
conda activate pyspark_env
pip install pyspark==4.0.1 boto3 pytest pandas pyarrow
```

> **Yêu cầu bắt buộc:** Đã cài Java/JDK 17 hoặc 21 (đặt biến môi trường `JAVA_HOME`).
> Nếu Spark trên Windows cần Hadoop native helper:
> ```powershell
> $env:HADOOP_HOME = "C:\hadoop"
> $env:Path = "$env:HADOOP_HOME\bin;$env:Path"
> ```

---

## 5. Chạy Orders Lakehouse & Airflow (Docker Compose)

Khởi động hệ thống MinIO, Nessie và Airflow:

```powershell
docker compose up -d
```

- **MinIO Console:** `http://localhost:9001` (Tài khoản: `admin` / `password123`).
- **Airflow Web UI:** `http://localhost:8080` (Tài khoản: `admin` / `admin`).
- **Nessie Catalog:** `http://localhost:19120`.

Chạy thủ công pipeline lakehouse trên máy host:

```powershell
$python = "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe"
$dir = ".\exercises\02-orders-lakehouse"
& $python "$dir\run_bronze_web.py"
& $python "$dir\run_bronze_mobile.py"
& $python "$dir\run_bronze_store.py"
& $python "$dir\run_silver.py"
& $python "$dir\run_gold.py"
```

---

## 6. Tài liệu chi tiết từng bài

- [00 – Read & Write Basics](./exercises/00-read-write-basics/README.md)
- [00b – Data Cleaning Practice](./exercises/00b-data-cleaning-practice/README.md)
- [01 – Orders Aggregation](./exercises/01-orders-aggregation/README.md)
- [02 – Orders Lakehouse (Bronze-Silver-Gold)](./exercises/02-orders-lakehouse/README.md)
- [03 – Airflow Orchestration](./exercises/03-airflow-orchestration/README.md)
- [04 – Data Types & Schema Control](./exercises/04-data-types/README.md)
- [05 – Joins: Orders x Customers](./exercises/05-joins/README.md)
- [06 – Customer Transactions Pipeline](./exercises/06-customer-transactions/README.md)
- [07 – Partitioning & Performance](./exercises/07-partitioning-performance/README.md)
- [08 – Advanced Read/Write & Validation](./exercises/08-read-write-advance/README.md)
- [09 – Full End-to-End ETL Pipeline](./exercises/09-full-etl-pipeline/README.md)
- [10 – UDF, Pandas UDF & UDTF Benchmark](./exercises/10-udf-pandas-udtf/README.md)

---

## 7. Nhật ký tiến độ học tập

- **07/09 – Nền tảng Data Lakehouse:** Tìm hiểu Data Lake vs Warehouse vs Lakehouse, Ingest, Bronze/Silver/Gold, ETL/ELT, MinIO, Nessie, Iceberg, Apache Spark.
- **09/09 – Đọc/Ghi dữ liệu cơ bản:** Chuẩn bị dataset CSV/JSON và hoàn thiện bài `00-read-write-basics`.
- **10/09 – Làm sạch và tổng hợp:** Thực hành làm sạch (`00b`), xây dựng bài toán orders aggregation với DataFrame API và Spark SQL (`01`).
- **11/09 – Lakehouse đa nguồn & Airflow:** Xây dựng pipeline Bronze → Silver → Gold cho 3 nguồn Web/Mobile/Store, tích hợp MinIO S3 và điều phối bằng Airflow DAG (`02`, `03`).
- **21/09 – Kiểu dữ liệu & Schema Control:** Hoàn thành bài `04-data-types`, làm chủ StructType thủ công, ép kiểu tiền tệ `Decimal` và kiểm soát ANSI mode.
- **22/09 – Phép Join & Quản lý dữ liệu mồ côi:** Hoàn thành bài `05-joins`, so sánh Inner vs Left join, cô lập bản ghi unmapped để đối soát nghiệp vụ.
- **23/09 – Deduplication & Window Functions:** Hoàn thành bài `06-customer-transactions`, khử duplicate bằng `row_number()`, xếp hạng Top-3 bằng `rank()` theo tỉnh, đối soát reconciliation 100% khớp.
- **24/09 – Partitioning & Performance:** Hoàn thành bài `07-partitioning-performance`, đo lường thực nghiệm `repartition` vs `coalesce` vs `partitionBy`, phân tích hiện tượng hash collision và data skew.
- **25/09 – Nâng cao Read/Write & Quarantine:** Hoàn thành bài `08-read-write-advance`, bắt lỗi định dạng ngày (`try_to_date`), phân loại lỗi `error_reason`, kiểm soát write modes.
- **26/09 – Full ETL Pipeline:** Hoàn thành bài `09-full-etl-pipeline`, tích hợp toàn bộ các kỹ thuật thành một quy trình ETL 10 bước hoàn chỉnh chuẩn sản xuất.
- **27/09 – UDF & Vectorized Execution:** Hoàn thành bài `10-udf-pandas-udtf`, benchmark trên 1.000.000 dòng giữa Python UDF (9.21s), Built-in function (3.98s), Pandas UDF vector hóa và UDTF đa dòng.
- **28/09 – Chuẩn hóa cấu trúc Repository:** Tái cấu trúc toàn bộ thư mục `exercises/` theo chuẩn kebab-case đánh số thống nhất từ 00 đến 10, bổ sung đầy đủ script và tài liệu cho từng bài.