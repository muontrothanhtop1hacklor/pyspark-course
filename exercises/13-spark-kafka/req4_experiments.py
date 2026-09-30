"""
req4_experiments.py
-------------------
Bài thực hành (ngày 3): Spark đọc Kafka - Yêu cầu 4: Các Thử Nghiệm Quan Sát

3 Thử nghiệm:
1. Gửi thêm message trong lúc streaming query đang chạy -> Quan sát xuất hiện ở Batch kế tiếp.
2. Dừng job, gửi thêm message, chạy lại với startingOffsets=latest -> Quan sát hành vi bỏ qua dữ liệu cũ.
3. So sánh Partition trong Kafka với Partition do Spark tạo ra.
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
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "orders_stream"


def send_kafka_messages(messages, desc=""):
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, dict) else str(v).encode("utf-8")
    )
    print(f"\n[PRODUCER] Gửi {len(messages)} message {desc} vào Kafka topic '{TOPIC_NAME}'...")
    for item in messages:
        key = item.get("order_id") if isinstance(item, dict) else "raw_key"
        producer.send(TOPIC_NAME, key=key, value=item)
    producer.flush()
    producer.close()
    print(f"[PRODUCER] Đã gửi thành công {len(messages)} message.\n")


def experiment_1_live_send():
    print("\n" + "=" * 80)
    print("THỬ NGHIỆM 1: GỬI THÊM MESSAGE MỚI TRONG LÚC JOB ĐANG CHẠY")
    print("=" * 80)

    spark = (
        SparkSession.builder
        .appName("Exercise13_Exp1_Live")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")  # Chỉ nhận message mới từ bây giờ
        .load()
    )

    display_df = kafka_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp")
    )

    query = (
        display_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="3 seconds")
        .start()
    )

    def delayed_send():
        time.sleep(6)  # Đợi query bắt đầu ổn định ở Batch 0
        new_messages = [
            {"order_id": "2001", "customer_id": "C001", "province": "HaNoi", "amount": 999000.0, "status": "SUCCESS", "order_date": "2025-02-10", "updated_at": "2025-02-10 10:00:00"},
            {"order_id": "2002", "customer_id": "C002", "province": "HoChiMinh", "amount": 1250000.0, "status": "COMPLETED", "order_date": "2025-02-10", "updated_at": "2025-02-10 10:01:00"},
            {"order_id": "2003", "customer_id": "C999", "province": "DaNang", "amount": 550000.0, "status": "PENDING", "order_date": "2025-02-10", "updated_at": "2025-02-10 10:02:00"}
        ]
        send_kafka_messages(new_messages, desc="(Live Streaming Orders 2001, 2002, 2003)")

    sender_thread = threading.Thread(target=delayed_send, daemon=True)
    sender_thread.start()

    print("[STREAM] Query đang chạy lắng nghe message mới (startingOffsets=latest)... Chờ 15s...")
    query.awaitTermination(timeout=15)
    query.stop()
    spark.stop()
    print("[EXP 1] Hoàn thành Thử nghiệm 1.\n")


def experiment_2_stop_and_restart():
    print("\n" + "=" * 80)
    print("THỬ NGHIỆM 2: DỪNG JOB, GỬI MESSAGE, CHẠY LẠI VỚI startingOffsets='latest'")
    print("=" * 80)

    # Bước 1: Khi job ĐANG DỪNG, gửi thêm 2 message mới
    offline_messages = [
        {"order_id": "3001", "customer_id": "C003", "province": "DaNang", "amount": 777000.0, "status": "SUCCESS", "order_date": "2025-02-11", "updated_at": "2025-02-11 11:00:00"},
        {"order_id": "3002", "customer_id": "C004", "province": "CanTho", "amount": 888000.0, "status": "COMPLETED", "order_date": "2025-02-11", "updated_at": "2025-02-11 11:05:00"}
    ]
    send_kafka_messages(offline_messages, desc="(Gửi khi job đã tắt: 3001, 3002)")

    # Bước 2: Chạy lại Spark Structured Streaming với startingOffsets = 'latest'
    spark = (
        SparkSession.builder
        .appName("Exercise13_Exp2_RestartLatest")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

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
            F.col("offset")
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )

    print("[STREAM] Đang chạy query với startingOffsets=latest sau khi gửi message... Chờ 10s...")
    query.awaitTermination(timeout=10)
    query.stop()
    spark.stop()
    print("[EXP 2] Hoàn thành Thử nghiệm 2.\n")


def experiment_3_partition_comparison():
    print("\n" + "=" * 80)
    print("THỬ NGHIỆM 3: SO SÁNH PARTITION KAFKA VÀ PARTITION SPARK TẠO RA")
    print("=" * 80)

    spark = (
        SparkSession.builder
        .appName("Exercise13_Exp3_Partitions")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    def analyze_batch(batch_df, batch_id):
        print(f"\n{'*'*60}")
        print(f"PHÂN TÍCH PARTITION Ở MICRO-BATCH {batch_id}:")
        num_spark_parts = batch_df.rdd.getNumPartitions()
        total_rows = batch_df.count()
        print(f"-> Tổng số dòng trong Batch: {total_rows}")
        print(f"-> Số lượng Spark Partitions của DataFrame: {num_spark_parts}")

        # Phân tích ánh xạ 1-1 giữa Spark Partition và Kafka Partition
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
            print(f"  + Kafka Partition [{p}] -> Ánh xạ 1-1 vào Spark Partition [{p}]: {cnt} dòng | Offset Range: [{min_off} -> {max_off}]")
        print(f"{'*'*60}\n")

    query = (
        kafka_df.writeStream
        .foreachBatch(analyze_batch)
        .start()
    )

    query.awaitTermination(timeout=12)
    query.stop()
    spark.stop()
    print("[EXP 3] Hoàn thành Thử nghiệm 3.\n")


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "all"

    # --- RESET TOPIC LOGIC ---
    topic_file = ".current_topic.txt"
    if action in ("all", "reset"):
        TOPIC_NAME = f"orders_stream_{int(time.time())}"
        with open(topic_file, "w") as f:
            f.write(TOPIC_NAME)
        
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=[BOOTSTRAP_SERVERS])
            print(f"\n[ADMIN] Đang tạo topic mới '{TOPIC_NAME}' với 3 partitions để test từ offset 0...")
            topic = NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)
            admin_client.create_topics([topic])
            print(f"[ADMIN] Đã tạo topic thành công.\n")
            admin_client.close()
            time.sleep(2) # Đợi Kafka nhận diện topic mới
        except Exception as e:
            print(f"[ADMIN] Lỗi khi tạo topic: {e}")
    else:
        if os.path.exists(topic_file):
            with open(topic_file, "r") as f:
                TOPIC_NAME = f.read().strip()
            print(f"\n[ADMIN] Đang sử dụng lại topic từ lần chạy trước: '{TOPIC_NAME}'\n")
    # -------------------------

    if action in ("1", "all"):
        experiment_1_live_send()

    if action in ("2", "all"):
        experiment_2_stop_and_restart()

    if action in ("3", "all"):
        experiment_3_partition_comparison()
