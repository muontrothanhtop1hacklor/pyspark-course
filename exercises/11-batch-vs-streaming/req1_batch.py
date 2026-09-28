"""
req1_batch.py
-------------
Yêu cầu 1 - Batch:
1. Tự khai báo schema, đọc cả thư mục batch_input bằng spark.read.
2. Chuẩn hoá status về uppercase, cast amount, parse order_date.
3. Đánh dấu các dòng lỗi (amount không hợp lệ, date sai) và chỉ tính các dòng hợp lệ.
4. Tổng hợp theo province: total_orders, total_amount.
5. show() kết quả và ghi lại.
"""

import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BATCH_INPUT_DIR = os.path.join(BASE_DIR, "data", "batch_input")


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Exercise11_Req1_Batch")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 70)
    print("YÊU CẦU 1: BATCH PROCESSING & TỔNG HỢP THEO TỈNH THÀNH")
    print("=" * 70)

    # 1. Khai báo schema tường minh
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("province", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])

    # 2. Đọc toàn bộ thư mục batch_input
    print(f"\n[1] Đọc dữ liệu từ thư mục: {BATCH_INPUT_DIR}")
    raw_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(BATCH_INPUT_DIR)
    )

    total_rows = raw_df.count()
    print(f"-> Tổng số dòng đọc được từ batch_input: {total_rows}")

    # 3. Chuẩn hoá và đánh dấu hợp lệ
    # - status -> UPPERCASE
    # - amount -> cast sang Double
    # - order_date -> parse date yyyy-MM-dd
    cleaned_df = (
        raw_df
        .withColumn("status_clean", F.upper(F.trim(F.col("status"))))
        .withColumn("amount_clean", F.expr("try_cast(amount AS DOUBLE)"))
        .withColumn("order_date_clean", F.to_date(F.try_to_timestamp(F.col("order_date"), F.lit("yyyy-MM-dd"))))
        .withColumn("is_valid_amount", (F.col("amount_clean").isNotNull()) & (F.col("amount_clean") > 0))
        .withColumn("is_valid_date", F.col("order_date_clean").isNotNull())
        .withColumn("is_valid", F.col("is_valid_amount") & F.col("is_valid_date"))
    )

    print("\n[2] Chi tiết các dòng dữ liệu sau khi clean và đánh dấu lỗi:")
    cleaned_df.select(
        "order_id", "province", "amount", "amount_clean",
        "status_clean", "order_date", "order_date_clean", "is_valid"
    ).show(35, truncate=False)

    invalid_df = cleaned_df.filter(~F.col("is_valid"))
    valid_df = cleaned_df.filter(F.col("is_valid"))

    print(f"-> Số dòng lỗi: {invalid_df.count()}")
    print(f"-> Số dòng hợp lệ: {valid_df.count()}")

    # 4. Tổng hợp theo province
    print("\n[3] Tổng hợp theo province (total_orders, total_amount):")
    agg_df = (
        valid_df
        .groupBy("province")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("amount_clean").alias("total_amount")
        )
        .orderBy("province")
    )

    agg_df.show(truncate=False)

    # 5. Đối chiếu số tổng toàn bộ
    totals = valid_df.agg(
        F.count("order_id").alias("grand_total_orders"),
        F.sum("amount_clean").alias("grand_total_amount")
    ).collect()[0]

    print("=" * 70)
    print("KẾT QUẢ ĐỐI CHIẾU VỚI TÍNH TOÁN BAN ĐẦU:")
    print(f"- Tổng số đơn hợp lệ: {totals['grand_total_orders']} (Dự kiến: 21)")
    print(f"- Tổng amount hợp lệ: {totals['grand_total_amount']:,.0f} VND (Dự kiến: 40,000,000 VND)")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
