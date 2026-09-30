"""
req3_stream_static_join.py
--------------------------
Bài thực hành (ngày 3): Spark đọc Kafka - Yêu cầu 3: Stream-Static Join

Mục tiêu:
1. Đọc customers.csv bằng spark.read (dữ liệu tĩnh dạng Batch DataFrame).
2. Left join Streaming DataFrame (orders từ Kafka) với Batch DataFrame (customers.csv) theo customer_id.
3. Kiểm tra các order không map được customer (is_customer_matched = False).
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
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CUSTOMERS_CSV_PATH = os.path.join(BASE_DIR, "customers.csv")
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "orders_stream"


def run_stream_static_join(duration_seconds=20):
    print("\n" + "=" * 80)
    print("YÊU CẦU 3: STREAM - STATIC JOIN GIỮA KAFKA ORDERS VÀ CUSTOMERS.CSV")
    print("=" * 80)

    spark = (
        SparkSession.builder
        .appName("Exercise13_Req3_StreamStaticJoin")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc bảng tĩnh customers.csv bằng spark.read (Batch DataFrame)
    print(f"\n[1] Đọc dữ liệu tĩnh từ {CUSTOMERS_CSV_PATH} bằng spark.read (Batch)...")
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
    print("[1] Bảng tĩnh customers.csv:")
    customers_df.show(truncate=False)

    # 2. Đọc luồng orders từ Kafka bằng spark.readStream
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
        .withColumn("status_clean", F.when(F.col("status_raw").isNotNull(), F.upper(F.trim(F.col("status_raw")))).otherwise(None))
        .withColumn("amount_clean", F.expr("try_cast(amount_raw AS DOUBLE)"))
        .withColumn("order_date_clean", F.to_date(F.try_to_timestamp(F.col("order_date_raw"), F.lit("yyyy-MM-dd"))))
    )

    # 3. Thực hiện Left Join giữa Stream (orders) và Static (customers)
    # Trong Spark Structured Streaming, join này là Stream-Static Join
    enriched_stream = (
        parsed_orders
        .join(customers_df, on="customer_id", how="left")
        .withColumn("is_customer_matched", F.col("customer_name").isNotNull())
    )

    # 4. Xuất kết quả ra console
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

    print(f"\n[STREAM] Đang chạy Stream-Static Join... Chờ {duration_seconds}s...")
    query.awaitTermination(timeout=duration_seconds)
    query.stop()
    print("[STREAM] Đã hoàn tất Yêu cầu 3.\n")
    spark.stop()


if __name__ == "__main__":
    run_stream_static_join(duration_seconds=18)
