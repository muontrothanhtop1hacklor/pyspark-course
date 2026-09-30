"""
req2_parse_and_clean.py
-----------------------
Bài thực hành (ngày 3): Spark đọc Kafka - Yêu cầu 2: Parse dữ liệu & Data Cleaning

Mục tiêu:
1. Cast cột value sang string.
2. Dùng from_json với schema tự khai báo để tách các cột:
   order_id, customer_id, province, amount, status, order_date, updated_at.
3. Message không phải JSON hợp lệ sẽ parse ra null:
   - Giữ lại các dòng này (Dead Letter Queue pattern), lưu raw_value, gắn flag is_valid_json.
4. Áp dụng lại các bước clean:
   - Chuẩn hóa status: upper(trim(status))
   - Cast amount sang Double: try_cast(amount AS DOUBLE)
   - Parse order_date: to_date(try_to_timestamp(order_date, 'yyyy-MM-dd'))
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

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "orders_stream"


def run_parse_and_clean(duration_seconds=20):
    print("\n" + "=" * 80)
    print("YÊU CẦU 2: PARSE JSON VÀ CLEAN DỮ LIỆU STREAMING TỪ KAFKA")
    print("=" * 80)

    spark = (
        SparkSession.builder
        .appName("Exercise13_Req2_ParseClean")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Khai báo schema cho JSON bên trong value
    order_json_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("province", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])

    # 2. Đọc stream từ Kafka với startingOffsets = earliest
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 3. Cast value sang string & dùng from_json
    # Khi chuỗi không phải JSON hợp lệ, from_json sẽ trả về NULL
    # Chúng ta giữ lại toàn bộ dòng lỗi bằng cách lưu raw_value và is_valid_json flag
    parsed_df = (
        kafka_df
        .withColumn("value_str", F.col("value").cast(StringType()))
        .withColumn("parsed", F.from_json(F.col("value_str"), order_json_schema))
        .withColumn("is_valid_json", F.col("parsed").isNotNull())
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

    # 4. Áp dụng logic Data Cleaning
    cleaned_df = (
        parsed_df
        # Chuẩn hóa status: chữ in hoa, bỏ khoảng trắng thừa
        .withColumn("status_clean", F.when(F.col("status_raw").isNotNull(), F.upper(F.trim(F.col("status_raw")))).otherwise(None))
        # Cast amount sang Double
        .withColumn("amount_clean", F.expr("try_cast(amount_raw AS DOUBLE)"))
        # Parse order_date sang DateType (yyyy-MM-dd) an toàn
        .withColumn("order_date_clean", F.to_date(F.try_to_timestamp(F.col("order_date_raw"), F.lit("yyyy-MM-dd"))))
        # Đánh giá tính hợp lệ từng trường
        .withColumn("is_valid_amount", (F.col("amount_clean").isNotNull()) & (F.col("amount_clean") > 0))
        .withColumn("is_valid_date", F.col("order_date_clean").isNotNull())
        # Phân loại nguyên nhân lỗi để quản lý chất lượng dữ liệu
        .withColumn("error_reason",
            F.when(~F.col("is_valid_json"), F.lit("MALFORMED_JSON"))
            .when(~F.col("is_valid_amount") & ~F.col("is_valid_date"), F.lit("INVALID_AMOUNT_AND_DATE"))
            .when(~F.col("is_valid_amount"), F.lit("INVALID_AMOUNT"))
            .when(~F.col("is_valid_date"), F.lit("INVALID_DATE"))
            .otherwise(F.lit("CLEAN_RECORD"))
        )
    )

    # 5. Xuất ra console hiển thị đầy đủ cả dòng sạch và dòng lỗi
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

    print(f"\n[STREAM] Query đang chạy, lắng nghe và parse/clean dữ liệu... Chờ {duration_seconds}s...")
    query.awaitTermination(timeout=duration_seconds)
    query.stop()
    print("[STREAM] Đã hoàn tất Yêu cầu 2.\n")
    spark.stop()


if __name__ == "__main__":
    run_parse_and_clean(duration_seconds=18)
