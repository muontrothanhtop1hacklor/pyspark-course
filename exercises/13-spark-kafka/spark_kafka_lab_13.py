"""
spark_kafka_lab_13.py
---------------------
Bài thực hành (ngày 3): Spark đọc Kafka
Bao gồm toàn bộ 4 Yêu cầu:
- Yêu cầu 1: Đọc thô từ Kafka (earliest vs latest)
- Yêu cầu 2: Parse JSON, Dead-Letter Queue bảo tồn dòng lỗi, Data Cleaning
- Yêu cầu 3: Stream-Static Join với customers.csv
- Yêu cầu 4: 3 thử nghiệm quan sát (Live push, Offline push + latest, Kafka vs Spark Partition Mapping)

Cách chạy:
  python spark_kafka_lab_13.py req1     # Chạy Yêu cầu 1
  python spark_kafka_lab_13.py req2     # Chạy Yêu cầu 2
  python spark_kafka_lab_13.py req3     # Chạy Yêu cầu 3
  python spark_kafka_lab_13.py req4     # Chạy Yêu cầu 4 (3 thử nghiệm)
  python spark_kafka_lab_13.py all      # Chạy tuần tự tất cả các yêu cầu
"""

import os
import sys
import time
import json
import threading

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CUSTOMERS_CSV_PATH = os.path.join(BASE_DIR, "customers.csv")
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "orders_stream"


def get_spark_session(app_name="Spark_Kafka_Lab_13"):
    """Khởi tạo SparkSession chuẩn cho Spark 4.0.1 và Kafka connector."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==============================================================================
# YÊU CẦU 1: ĐỌC THÔ TỪ KAFKA (EARLIEST & LATEST)
# ==============================================================================
def run_requirement_1():
    print("\n" + "=" * 80)
    print("YÊU CẦU 1: ĐỌC THÔ TỪ KAFKA (SO SÁNH startingOffsets: earliest vs latest)")
    print("=" * 80)

    # 1. Thử nghiệm với startingOffsets = earliest
    print("\n--- [1.1] Đọc với startingOffsets = 'earliest' ---")
    spark = get_spark_session("Req1_Earliest")
    kafka_earliest_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    query_earliest = (
        kafka_earliest_df.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp")
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 15)
        .start()
    )
    print("[REQ 1.1] Đang đọc từ earliest... Chờ 12s...")
    query_earliest.awaitTermination(timeout=12)
    query_earliest.stop()
    spark.stop()

    # 2. Thử nghiệm với startingOffsets = latest
    print("\n--- [1.2] Đọc với startingOffsets = 'latest' ---")
    spark = get_spark_session("Req1_Latest")
    kafka_latest_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    query_latest = (
        kafka_latest_df.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp")
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )
    print("[REQ 1.2] Đang đọc từ latest (không đọc message cũ)... Chờ 10s...")
    query_latest.awaitTermination(timeout=10)
    query_latest.stop()
    spark.stop()

    print("\n[KẾT QUẢ YÊU CẦU 1]")
    print("+ 'earliest': Đọc lại toàn bộ message lịch sử từ offset 0 của từng partition (Batch 0 chứa đầy đủ dữ liệu cũ).")
    print("+ 'latest': Bắt đầu từ offset cuối cùng hiện tại của partition, Batch 0 rỗng vì bỏ qua tất cả message đã tồn tại.")


# ==============================================================================
# YÊU CẦU 2: PARSE DỮ LIỆU & DATA CLEANING
# ==============================================================================
def run_requirement_2():
    print("\n" + "=" * 80)
    print("YÊU CẦU 2: PARSE JSON VÀ CLEAN DỮ LIỆU STREAMING TỪ KAFKA")
    print("=" * 80)

    spark = get_spark_session("Req2_ParseClean")

    order_json_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("province", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Cast value sang string & parse JSON
    # Dòng lỗi parse ra null: Giữ lại raw_value và đánh dấu is_valid_json
    parsed_df = (
        kafka_df
        .withColumn("value_str", F.col("value").cast(StringType()))
        .withColumn("parsed", F.from_json(F.col("value_str"), order_json_schema))
        .withColumn("is_valid_json", F.col("parsed.order_id").isNotNull())
        .select(
            F.col("parsed.order_id").alias("order_id"),
            F.col("parsed.customer_id").alias("customer_id"),
            F.col("parsed.province").alias("province"),
            F.col("parsed.amount").alias("amount_raw"),
            F.col("parsed.status").alias("status_raw"),
            F.col("parsed.order_date").alias("order_date_raw"),
            F.col("parsed.updated_at").alias("updated_at"),
            F.col("is_valid_json"),
            F.col("value_str").alias("raw_value")
        )
    )

    cleaned_df = (
        parsed_df
        # 1. Chuẩn hóa status: chữ hoa, bỏ khoảng trắng
        .withColumn("status_clean", F.when(F.col("status_raw").isNotNull(), F.upper(F.trim(F.col("status_raw")))).otherwise(None))
        # 2. Cast amount sang Double
        .withColumn("amount_clean", F.expr("try_cast(amount_raw AS DOUBLE)"))
        # 3. Parse order_date sang DateType
        .withColumn("order_date_clean", F.to_date(F.try_to_timestamp(F.col("order_date_raw"), F.lit("yyyy-MM-dd"))))
        # 4. Kiểm tra hợp lệ
        .withColumn("is_valid_amount", (F.col("amount_clean").isNotNull()) & (F.col("amount_clean") > 0))
        .withColumn("is_valid_date", F.col("order_date_clean").isNotNull())
        .withColumn("error_reason",
            F.when(~F.col("is_valid_json"), F.lit("MALFORMED_JSON"))
            .when(~F.col("is_valid_amount") & ~F.col("is_valid_date"), F.lit("INVALID_AMOUNT_AND_DATE"))
            .when(~F.col("is_valid_amount"), F.lit("INVALID_AMOUNT"))
            .when(~F.col("is_valid_date"), F.lit("INVALID_DATE"))
            .otherwise(F.lit("CLEAN_RECORD"))
        )
    )

    query = (
        cleaned_df.select(
            "order_id",
            "customer_id",
            "province",
            "amount_clean",
            "status_clean",
            "order_date_clean",
            "is_valid_json",
            "error_reason",
            "raw_value"
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 25)
        .start()
    )

    print("[REQ 2] Query đang chạy, lắng nghe và parse/clean dữ liệu... Chờ 18s...")
    query.awaitTermination(timeout=18)
    query.stop()
    spark.stop()
    print("\n[KẾT QUẢ YÊU CẦU 2]")
    print("+ Message không phải JSON (như chuỗi thô INVALID_RAW_STRING) không bị mất mà lưu trong raw_value, đánh dấu error_reason = MALFORMED_JSON.")
    print("+ Các trường status được chuẩn hóa chữ in hoa (SUCCESS, PENDING, COMPLETED, SHIPPING).")
    print("+ Amount <= 0 hoặc null, date sai định dạng được phát hiện và gắn nhãn tương ứng mà không làm gián đoạn luồng stream.")


# ==============================================================================
# YÊU CẦU 3: JOIN VỚI DỮ LIỆU TĨNH (STREAM-STATIC JOIN)
# ==============================================================================
def run_requirement_3():
    print("\n" + "=" * 80)
    print("YÊU CẦU 3: STREAM - STATIC JOIN GIỮA ORDERS VÀ CUSTOMERS.CSV")
    print("=" * 80)

    spark = get_spark_session("Req3_StreamStaticJoin")

    # 1. Đọc dữ liệu tĩnh customers.csv (Batch DataFrame)
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_type", StringType(), True)
    ])
    customers_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(customer_schema)
        .load(CUSTOMERS_CSV_PATH)
    )
    print("\n[BẢNG TĨNH CUSTOMERS.CSV]:")
    customers_df.show(truncate=False)

    # 2. Đọc luồng orders từ Kafka
    order_json_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("province", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_orders = (
        kafka_df
        .withColumn("value_str", F.col("value").cast(StringType()))
        .withColumn("parsed", F.from_json(F.col("value_str"), order_json_schema))
        .select(
            F.col("parsed.order_id").alias("order_id"),
            F.col("parsed.customer_id").alias("customer_id"),
            F.col("parsed.province").alias("province"),
            F.expr("try_cast(parsed.amount AS DOUBLE)").alias("amount_clean"),
            F.upper(F.trim(F.col("parsed.status"))).alias("status_clean"),
            F.to_date(F.try_to_timestamp(F.col("parsed.order_date"), F.lit("yyyy-MM-dd"))).alias("order_date_clean")
        )
    )

    # 3. Left join Stream orders với Static customers
    enriched_stream = (
        parsed_orders
        .join(customers_df, on="customer_id", how="left")
        .withColumn("is_customer_matched", F.col("customer_name").isNotNull())
    )

    query = (
        enriched_stream.select(
            "order_id",
            "customer_id",
            "customer_name",
            "customer_type",
            "is_customer_matched",
            "province",
            "amount_clean",
            "status_clean",
            "order_date_clean"
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 25)
        .start()
    )

    print("[REQ 3] Đang chạy Stream-Static Join... Chờ 18s...")
    query.awaitTermination(timeout=18)
    query.stop()
    spark.stop()
    print("\n[KẾT QUẢ YÊU CẦU 3]")
    print("+ Stream-Static Join enrich thành công customer_name, customer_type (VIP, REGULAR, NEW).")
    print("+ Các đơn hàng không map được (is_customer_matched = False): Đơn có customer_id 'C999' hoặc các message không parse được customer_id.")


# ==============================================================================
# YÊU CẦU 4: CÁC THỬ NGHIỆM QUAN SÁT
# ==============================================================================
def run_requirement_4():
    print("\n" + "=" * 80)
    print("YÊU CẦU 4: CÁC THỬ NGHIỆM QUAN SÁT HÀNH VI STREAMING & KAFKA PARTITION")
    print("=" * 80)

    # --- Thử nghiệm 4.1: Gửi message lúc job đang chạy ---
    print("\n--- [4.1] Gửi message lúc job đang chạy ---")
    spark = get_spark_session("Exp4_1_Live")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    query = (
        kafka_df.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp")
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="3 seconds")
        .start()
    )

    def live_send():
        time.sleep(6)
        producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("\n[PRODUCER] Push 2 orders mới: 4001, 4002 vào topic...")
        producer.send(TOPIC_NAME, key="4001", value={"order_id": "4001", "customer_id": "C001", "province": "HaNoi", "amount": 670000.0, "status": "SUCCESS", "order_date": "2025-02-15", "updated_at": "2025-02-15 15:00:00"})
        producer.send(TOPIC_NAME, key="4002", value={"order_id": "4002", "customer_id": "C002", "province": "HoChiMinh", "amount": 890000.0, "status": "COMPLETED", "order_date": "2025-02-15", "updated_at": "2025-02-15 15:05:00"})
        producer.flush()
        producer.close()
        print("[PRODUCER] Đã push xong.")

    t = threading.Thread(target=live_send, daemon=True)
    t.start()

    print("[EXP 4.1] Đang lắng nghe... Chờ 15s...")
    query.awaitTermination(timeout=15)
    query.stop()
    spark.stop()

    # --- Thử nghiệm 4.2: Dừng job, gửi message, chạy lại với latest ---
    print("\n--- [4.2] Dừng job, gửi message, chạy lại với startingOffsets='latest' ---")
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("[PRODUCER] Gửi offline message 5001 khi job đang tắt...")
    producer.send(TOPIC_NAME, key="5001", value={"order_id": "5001", "customer_id": "C003", "province": "DaNang", "amount": 450000.0, "status": "SUCCESS", "order_date": "2025-02-16", "updated_at": "2025-02-16 16:00:00"})
    producer.flush()
    producer.close()

    spark = get_spark_session("Exp4_2_OfflineLatest")
    kafka_latest = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )
    q2 = (
        kafka_latest.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            F.col("partition"),
            F.col("offset")
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )
    print("[EXP 4.2] Khởi động lại với latest... Chờ 10s...")
    q2.awaitTermination(timeout=10)
    q2.stop()
    spark.stop()

    # --- Thử nghiệm 4.3: So sánh partition trong Kafka và Spark ---
    print("\n--- [4.3] So sánh partition trong Kafka và partition Spark tạo ra ---")
    spark = get_spark_session("Exp4_3_Partitions")
    kafka_parts = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    def analyze_batch(batch_df, batch_id):
        print(f"\n{'*'*60}")
        print(f"KẾT QUẢ PHÂN TÍCH ÁNH XẠ PARTITION Ở MICRO-BATCH {batch_id}:")
        num_spark_parts = batch_df.rdd.getNumPartitions()
        total_rows = batch_df.count()
        print(f"-> Tổng số dòng trong Batch: {total_rows}")
        print(f"-> Số lượng Spark Partitions: {num_spark_parts}")

        summary_rows = (
            batch_df.groupBy("partition")
            .agg(
                F.count("*").alias("row_count"),
                F.min("offset").alias("min_offset"),
                F.max("offset").alias("max_offset")
            )
            .orderBy("partition")
            .collect()
        )
        for row in summary_rows:
            p = row["partition"]
            cnt = row["row_count"]
            min_off = row["min_offset"]
            max_off = row["max_offset"]
            print(f"  + Kafka Partition [{p}] -> Ánh xạ 1-1 vào Spark Partition [{p}]: {cnt} dòng | Offset: [{min_off} -> {max_off}]")
        print(f"{'*'*60}\n")

    q3 = kafka_parts.writeStream.foreachBatch(analyze_batch).start()
    print("[EXP 4.3] Đang phân tích partition... Chờ 12s...")
    q3.awaitTermination(timeout=12)
    q3.stop()
    spark.stop()


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "all"

    if target in ("req1", "1"):
        run_requirement_1()
    elif target in ("req2", "2"):
        run_requirement_2()
    elif target in ("req3", "3"):
        run_requirement_3()
    elif target in ("req4", "4"):
        run_requirement_4()
    elif target in ("all",):
        run_requirement_1()
        run_requirement_2()
        run_requirement_3()
        run_requirement_4()
    else:
        print(f"Tham số không hợp lệ: '{target}'. Chọn một trong: req1, req2, req3, req4, all")


if __name__ == "__main__":
    main()
