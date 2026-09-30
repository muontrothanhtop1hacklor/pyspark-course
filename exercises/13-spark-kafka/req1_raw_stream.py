"""
req1_raw_stream.py
------------------
Bài thực hành (ngày 3): Spark đọc Kafka - Yêu cầu 1: Đọc thô từ Kafka

Mục tiêu:
1. Dùng readStream kết nối tới Kafka (orders_stream).
2. Lựa chọn startingOffsets: "earliest" và "latest".
3. In ra console các cột: key, value, topic, partition, offset, timestamp.
4. So sánh kết quả giữa 2 chế độ earliest và latest.
"""

import os
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "orders_stream"


def run_raw_stream(starting_offset="earliest", duration_seconds=12):
    print("\n" + "=" * 80)
    print(f"CHẠY TEST ĐỌC THÔ TỪ KAFKA VỚI startingOffsets = '{starting_offset}'")
    print("=" * 80)

    spark = (
        SparkSession.builder
        .appName(f"Exercise13_Req1_{starting_offset}")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Đọc stream thô từ Kafka
    kafka_raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", starting_offset)
        .load()
    )

    # Lấy các cột yêu cầu: key, value, topic, partition, offset, timestamp
    # Cast key và value sang string để dễ quan sát trên console
    display_df = kafka_raw_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp")
    )

    query = (
        display_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .start()
    )

    print(f"\n[STREAM] Đang lắng nghe topic '{TOPIC_NAME}' (startingOffsets={starting_offset}). Chờ {duration_seconds}s...")
    query.awaitTermination(timeout=duration_seconds)
    query.stop()
    print(f"[STREAM] Đã dừng query {starting_offset}.\n")
    spark.stop()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "both"

    if mode in ("earliest", "both"):
        run_raw_stream(starting_offset="earliest", duration_seconds=12)

    if mode in ("latest", "both"):
        run_raw_stream(starting_offset="latest", duration_seconds=10)
