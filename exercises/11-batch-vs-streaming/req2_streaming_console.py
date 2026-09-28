"""
req2_streaming_console.py
-------------------------
Yêu cầu 2 - Streaming cùng logic:
1. Đọc stream_input bằng readStream với cùng schema và các bước clean như Yêu cầu 1.
2. Tổng hợp theo province (total_orders, total_amount), outputMode("complete"),
   format("console"), trigger(processingTime="5 seconds").
3. Đặt maxFilesPerTrigger=1 để mỗi micro-batch xử lý đúng 1 file.
4. Lần lượt thả orders_1.csv, orders_2.csv, orders_3.csv vào stream_input cách nhau 12 giây.
5. In kết quả console của từng Batch (Batch 0, Batch 1, Batch 2).
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
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
STREAM_INPUT_DIR = os.path.join(DATA_DIR, "stream_input")
RAW_SOURCE_DIR = os.path.join(DATA_DIR, "source_files")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoint", "stream_console")


class MicroBatchProgressLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"\n[STREAM-LISTENER] Query đã khởi động! ID: {event.id}")

    def onQueryProgress(self, event):
        p = event.progress
        num_rows = p.numInputRows
        batch_id = p.batchId
        print(f"\n[STREAM-LISTENER] >>> HOÀN TẤT MICRO-BATCH {batch_id} | Input rows: {num_rows} <<<")

    def onQueryTerminated(self, event):
        print("\n[STREAM-LISTENER] Query đã dừng.")


def feed_files_worker(query):
    """Tiến trình thả file tuần tự vào stream_input để mô phỏng dữ liệu streaming đến theo thời gian."""
    files = ["orders_1.csv", "orders_2.csv", "orders_3.csv"]
    print("\n[FEEDER] Bắt đầu tiến trình thả file vào stream_input...")
    time.sleep(6)  # Chờ query chạy ổn định

    for i, fname in enumerate(files):
        src = os.path.join(RAW_SOURCE_DIR, fname)
        dst = os.path.join(STREAM_INPUT_DIR, fname)
        print(f"\n{'*'*50}")
        print(f"[FEEDER] THẢ FILE {i+1}/3: '{fname}' vào {STREAM_INPUT_DIR}")
        print(f"{'*'*50}")
        shutil.copy(src, dst)
        # Đợi 14 giây để trigger 5s bắt được file và thực thi xong micro-batch
        time.sleep(14)

    print("\n[FEEDER] Đã thả đủ cả 3 file. Đợi 8 giây cho streaming cập nhật ổn định...")
    time.sleep(8)
    print("[FEEDER] Yêu cầu dừng streaming query...")
    query.stop()


def main():
    print("=" * 70)
    print("YÊU CẦU 2: STRUCTURED STREAMING VỚI COMPLETE MODE")
    print("=" * 70)

    # Xóa checkpoint cũ để bài thực hành luôn tính toán từ đầu mà không bị ảnh hưởng bởi các lần chạy trước
    if os.path.exists(STREAM_INPUT_DIR):
        shutil.rmtree(STREAM_INPUT_DIR)
    os.makedirs(STREAM_INPUT_DIR, exist_ok=True)

    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Exercise11_Req2_Streaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    listener = MicroBatchProgressLogger()
    spark.streams.addListener(listener)

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("province", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])

    # Giới hạn mỗi micro-batch đúng 1 file để quan sát sự thay đổi trạng thái tích lũy qua từng đợt
    raw_stream = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("maxFilesPerTrigger", "1")
        .schema(schema)
        .load(STREAM_INPUT_DIR)
    )

    cleaned_stream = (
        raw_stream
        .withColumn("status_clean", F.upper(F.trim(F.col("status"))))
        .withColumn("amount_clean", F.expr("try_cast(amount AS DOUBLE)"))
        .withColumn("order_date_clean", F.to_date(F.try_to_timestamp(F.col("order_date"), F.lit("yyyy-MM-dd"))))
        .withColumn("is_valid_amount", (F.col("amount_clean").isNotNull()) & (F.col("amount_clean") > 0))
        .withColumn("is_valid_date", F.col("order_date_clean").isNotNull())
        .withColumn("is_valid", F.col("is_valid_amount") & F.col("is_valid_date"))
    )

    valid_stream = cleaned_stream.filter(F.col("is_valid"))

    agg_stream = (
        valid_stream
        .groupBy("province")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("amount_clean").alias("total_amount")
        )
    )

    # 5. Output console với Complete mode và trigger 5 seconds
    print("\n[2] Khởi chạy writeStream với outputMode('complete') và trigger 5s...")
    query = (
        agg_stream.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    # Bắt đầu thread thả file
    feeder_thread = threading.Thread(target=feed_files_worker, args=(query,), daemon=True)
    feeder_thread.start()

    # Chờ query kết thúc
    query.awaitTermination()
    feeder_thread.join()

    print("\n" + "=" * 70)
    print("HOÀN THÀNH YÊU CẦU 2: STREAMING COMPLETE RUN THÀNH CÔNG")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
