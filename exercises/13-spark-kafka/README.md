# Bài 13: Spark Structured Streaming Đọc Kafka

Thực hành kết nối Apache Spark (Structured Streaming) với cụm Apache Kafka: Đọc dữ liệu luồng thô, Parse JSON bảo tồn dữ liệu lỗi (Dead Letter Queue pattern), chuẩn hóa dữ liệu (Data Cleaning), Join luồng với bảng tĩnh `customers.csv` (Stream-Static Join) và kiểm chứng thực nghiệm cơ chế Offset, Partition mapping.

---

## 1. Cấu Trúc Thư Mục Bài 13

```text
exercises/13-spark-kafka/
├── customers.csv             # Dữ liệu tĩnh 10 khách hàng (customer_id, customer_name, customer_type)
├── req1_raw_stream.py        # Yêu cầu 1: Đọc thô từ Kafka (so sánh earliest vs latest)
├── req2_parse_and_clean.py   # Yêu cầu 2: Parse JSON, bảo tồn bản ghi lỗi, làm sạch dữ liệu
├── req3_stream_static_join.py# Yêu cầu 3: Stream-Static Join giữa luồng orders và customers.csv
├── req4_experiments.py       # Yêu cầu 4: 3 thử nghiệm quan sát (Live push, Offline push + latest, Partition mapping)
├── spark_kafka_lab_13.py     # Script tổng hợp Master chạy cả 4 yêu cầu hoặc từng yêu cầu
└── README.md                 # Hướng dẫn chi tiết bài thực hành
```

---

## 2. Chuẩn Bị Môi Trường

### 2.1. Cụm Kafka (Docker KRaft)
Kiểm tra Kafka container đang chạy:
```powershell
docker ps
```
Topic `orders_stream` được cấu hình 3 partition tại `localhost:9092`.

### 2.2. Package Spark SQL Kafka
- **Môi trường:** Python Conda `pyspark_env` (PySpark 4.0.1, Scala 2.13).
- **Maven Package:** `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1`.
- **Cấu hình SparkSession:**
  ```python
  spark = (
      SparkSession.builder
      .appName("Spark_Kafka_Lab_13")
      .master("local[2]")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
  )
  ```

---

## 3. Hướng Dẫn Chạy Từng Yêu Cầu

### Yêu cầu 1: Đọc thô từ Kafka
```powershell
& "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe" exercises\13-spark-kafka\req1_raw_stream.py both
```
Hoặc dùng master script:
```powershell
& "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe" exercises\13-spark-kafka\spark_kafka_lab_13.py req1
```

### Yêu cầu 2: Parse JSON & Làm sạch dữ liệu
```powershell
& "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe" exercises\13-spark-kafka\req2_parse_and_clean.py
```
Hoặc:
```powershell
& "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe" exercises\13-spark-kafka\spark_kafka_lab_13.py req2
```

### Yêu cầu 3: Stream - Static Join
```powershell
& "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe" exercises\13-spark-kafka\req3_stream_static_join.py
```
Hoặc:
```powershell
& "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe" exercises\13-spark-kafka\spark_kafka_lab_13.py req3
```

### Yêu cầu 4: 3 Thử Nghiệm Quan Sát
Chạy riêng từng thử nghiệm:
```powershell
# Thử nghiệm 1: Gửi message lúc job đang chạy
& "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe" exercises\13-spark-kafka\req4_experiments.py 1

# Thử nghiệm 2: Dừng job, gửi message, chạy lại với latest
& "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe" exercises\13-spark-kafka\req4_experiments.py 2

# Thử nghiệm 3: So sánh partition trong Kafka và Spark
& "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe" exercises\13-spark-kafka\req4_experiments.py 3
```
Hoặc chạy toàn bộ thử nghiệm:
```powershell
& "C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe" exercises\13-spark-kafka\spark_kafka_lab_13.py req4
```

---

## 4. Tóm Tắt Kết Quả & Trả Lời Câu Hỏi Cuối Bài

Toàn bộ báo cáo chi tiết, log thực tế và phần trả lời 5 câu hỏi lý thuyết được cập nhật đầy đủ tại file [kafka_notes.md](../../kafka_notes.md).
