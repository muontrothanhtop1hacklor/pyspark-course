"""
req5_write_and_check.py
-----------------------
Yêu cầu 5 - Write + Check:
1. Batch: ghi dữ liệu đã clean và hợp lệ ra Parquet.
2. Streaming: ghi dữ liệu đã clean và hợp lệ ra Parquet với outputMode("append") và checkpointLocation.
   Thả lại 3 file vào một stream_input mới.
3. Đọc lại hai output và kiểm tra:
   - count
   - schema
   - tổng amount có khớp với số tính ở phần Chuẩn bị (21 đơn, 40,000,000 VND)
4. Mở thư mục checkpoint và liệt kê các thư mục con quan sát được.
"""

import os
import sys
import time
import shutil
import threading

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
BATCH_INPUT_DIR = os.path.join(DATA_DIR, "batch_input")
RAW_SOURCE_DIR = os.path.join(DATA_DIR, "source_files")
STREAM_INPUT_WRITE_DIR = os.path.join(DATA_DIR, "stream_input_write")

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
BATCH_PARQUET_DIR = os.path.join(OUTPUT_DIR, "batch_parquet")
STREAM_PARQUET_DIR = os.path.join(OUTPUT_DIR, "stream_parquet")

CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoint")
STREAM_CHECKPOINT_DIR = os.path.join(CHECKPOINT_DIR, "stream_parquet_write")

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("province", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("updated_at", StringType(), True),
])


def clean_dataframe(df):
    cleaned = (
        df
        .withColumn("status_clean", F.upper(F.trim(F.col("status"))))
        .withColumn("amount_clean", F.expr("try_cast(amount AS DOUBLE)"))
        .withColumn("order_date_clean", F.to_date(F.try_to_timestamp(F.col("order_date"), F.lit("yyyy-MM-dd"))))
        .withColumn("is_valid_amount", (F.col("amount_clean").isNotNull()) & (F.col("amount_clean") > 0))
        .withColumn("is_valid_date", F.col("order_date_clean").isNotNull())
        .withColumn("is_valid", F.col("is_valid_amount") & F.col("is_valid_date"))
    )
    return (
        cleaned.filter(F.col("is_valid"))
        .select(
            "order_id",
            "customer_id",
            "province",
            "amount_clean",
            "status_clean",
            "order_date_clean",
            "updated_at"
        )
    )


def reset_dir(d):
    if os.path.exists(d):
        shutil.rmtree(d)
    os.makedirs(d, exist_ok=True)


def feed_stream_files(query):
    files = ["orders_1.csv", "orders_2.csv", "orders_3.csv"]
    print("\n[FEEDER] Đang nạp lần lượt 3 file vào stream_input_write...")
    time.sleep(5)
    for i, fname in enumerate(files):
        src = os.path.join(RAW_SOURCE_DIR, fname)
        dst = os.path.join(STREAM_INPUT_WRITE_DIR, fname)
        print(f"[FEEDER] Thả {fname}...")
        shutil.copy(src, dst)
        time.sleep(12)
    print("[FEEDER] Đã nạp đủ 3 file. Đợi hoàn tất micro-batch...")
    time.sleep(8)
    query.stop()


def main():
    print("=" * 70)
    print("YÊU CẦU 5: WRITE PARQUET (BATCH VS STREAMING) & CHECKPOINT AUDIT")
    print("=" * 70)

    # Dọn dẹp output và stream input
    reset_dir(BATCH_PARQUET_DIR)
    reset_dir(STREAM_PARQUET_DIR)
    reset_dir(STREAM_INPUT_WRITE_DIR)
    reset_dir(STREAM_CHECKPOINT_DIR)

    spark = (
        SparkSession.builder
        .appName("Exercise11_Req5_WriteAndCheck")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # -------------------------------------------------------------
    # 1. BATCH WRITE TO PARQUET
    # -------------------------------------------------------------
    print("\n[BƯỚC 1] BATCH: Đọc từ batch_input, clean dữ liệu và ghi Parquet...")
    batch_raw = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(BATCH_INPUT_DIR)
    )
    batch_valid = clean_dataframe(batch_raw)
    (
        batch_valid.write
        .mode("overwrite")
        .parquet(BATCH_PARQUET_DIR)
    )
    print(f"-> Đã ghi Batch Parquet thành công vào: {BATCH_PARQUET_DIR}")

    # -------------------------------------------------------------
    # 2. STREAMING WRITE TO PARQUET
    # -------------------------------------------------------------
    print("\n[BƯỚC 2] STREAMING: readStream từ stream_input_write, ghi Parquet (mode append)...")
    stream_raw = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("maxFilesPerTrigger", "1")
        .schema(schema)
        .load(STREAM_INPUT_WRITE_DIR)
    )
    stream_valid = clean_dataframe(stream_raw)

    query = (
        stream_valid.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", STREAM_PARQUET_DIR)
        .option("checkpointLocation", STREAM_CHECKPOINT_DIR)
        .trigger(processingTime="4 seconds")
        .start()
    )

    feeder = threading.Thread(target=feed_stream_files, args=(query,), daemon=True)
    feeder.start()
    query.awaitTermination()
    feeder.join()
    print(f"-> Đã ghi Streaming Parquet thành công vào: {STREAM_PARQUET_DIR}")

    # -------------------------------------------------------------
    # 3. ĐỌC LẠI HAI OUTPUT VÀ KIỂM TRA ĐỐI CHIẾU
    # -------------------------------------------------------------
    print("\n" + "=" * 70)
    print("[BƯỚC 3] ĐỌC LẠI HAI OUTPUT VÀ SO SÁNH ĐỐI CHIẾU:")
    print("=" * 70)

    df_batch_read = spark.read.parquet(BATCH_PARQUET_DIR)
    df_stream_read = spark.read.parquet(STREAM_PARQUET_DIR)

    count_batch = df_batch_read.count()
    count_stream = df_stream_read.count()

    amount_batch = df_batch_read.agg(F.sum("amount_clean")).collect()[0][0]
    amount_stream = df_stream_read.agg(F.sum("amount_clean")).collect()[0][0]

    print(f"1. Tổng số dòng (Row count):")
    print(f"   - Batch Parquet    : {count_batch} dòng")
    print(f"   - Streaming Parquet: {count_stream} dòng")
    print(f"   - Đối chiếu chuẩn bị : 21 dòng hợp lệ -> {'KHỚP TUYỆT ĐỐI' if count_batch == count_stream == 21 else 'LỆCH'}")

    print(f"\n2. Tổng số tiền (Total Amount):")
    print(f"   - Batch Parquet    : {amount_batch:,.0f} VND")
    print(f"   - Streaming Parquet: {amount_stream:,.0f} VND")
    print(f"   - Đối chiếu chuẩn bị : 40,000,000 VND -> {'KHỚP TUYỆT ĐỐI' if amount_batch == amount_stream == 40000000.0 else 'LỆCH'}")

    print(f"\n3. So sánh Schema giữa Batch và Streaming:")
    schema_batch_str = df_batch_read.schema.simpleString()
    schema_stream_str = df_stream_read.schema.simpleString()
    print(f"   - Batch Schema    : {schema_batch_str}")
    print(f"   - Streaming Schema: {schema_stream_str}")
    print(f"   - Kết luận        : {'GIỐNG NHAU 100%' if schema_batch_str == schema_stream_str else 'KHÁC NHAU'}")

    print(f"\n4. Đối chiếu tổng hợp theo Province giữa hai output:")
    print("--- BATCH PARQUET ---")
    df_batch_read.groupBy("province").agg(F.count("order_id").alias("orders"), F.sum("amount_clean").alias("amount")).orderBy("province").show(truncate=False)
    print("--- STREAMING PARQUET ---")
    df_stream_read.groupBy("province").agg(F.count("order_id").alias("orders"), F.sum("amount_clean").alias("amount")).orderBy("province").show(truncate=False)

    # -------------------------------------------------------------
    # 4. KHÁM PHÁ CẤU TRÚC THƯ MỤC CHECKPOINT
    # -------------------------------------------------------------
    print("=" * 70)
    print("[BƯỚC 4] KHÁM PHÁ THƯ MỤC CHECKPOINT CỦA STREAMING:")
    print(f"Đường dẫn: {STREAM_CHECKPOINT_DIR}")
    print("=" * 70)

    for root, dirs, files in os.walk(STREAM_CHECKPOINT_DIR):
        rel_root = os.path.relpath(root, STREAM_CHECKPOINT_DIR)
        indent = "  " * (0 if rel_root == "." else rel_root.count(os.sep) + 1)
        dir_name = os.path.basename(root) if rel_root != "." else "checkpoint/"
        print(f"{indent}[DIR] {dir_name}/")
        for f in files:
            file_path = os.path.join(root, f)
            sz = os.path.getsize(file_path)
            print(f"{indent}   └── {f} ({sz} bytes)")

    spark.stop()
    print("\n" + "=" * 70)
    print("HOÀN THÀNH YÊU CẦU 5!")
    print("=" * 70)


if __name__ == "__main__":
    main()
