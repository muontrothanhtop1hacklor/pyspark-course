"""
req4_experiments.py
-------------------
Yêu cầu 4 - Thử nghiệm và quan sát:
1. Bỏ .schema(...) khỏi readStream. Ghi lại lỗi.
2. Đổi trigger thành 20 giây rồi thả file mới. Quan sát độ trễ.
3. Thả lại cùng một file đã thả trước đó. Spark có xử lý lại không?
4. Đổi outputMode sang "append" trên bản aggregate. Ghi lại lỗi.
5. Thử dedup order_id bằng Window + row_number() trên stream. Ghi lại lỗi.
"""

import os
import sys
import time
import shutil

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.streaming import StreamingQueryListener

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
STREAM_INPUT_DIR = os.path.join(DATA_DIR, "stream_input_exp")
RAW_SOURCE_DIR = os.path.join(DATA_DIR, "source_files")
CHECKPOINT_EXP_DIR = os.path.join(BASE_DIR, "checkpoint", "stream_exp")

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("province", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("updated_at", StringType(), True),
])


def reset_dir(d):
    if os.path.exists(d):
        shutil.rmtree(d)
    os.makedirs(d, exist_ok=True)


def exp1_no_schema(spark):
    print("\n" + "=" * 70)
    print("THỬ NGHIỆM 1: BỎ .schema(...) KHỎI readStream")
    print("=" * 70)
    reset_dir(STREAM_INPUT_DIR)
    try:
        df = (
            spark.readStream
            .format("csv")
            .option("header", "true")
            .load(STREAM_INPUT_DIR)
        )
        print("Bất ngờ: Không có lỗi!")
    except Exception as e:
        print("[KẾT QUẢ THỬ NGHIỆM 1 - BẮT ĐƯỢC NGOẠI LỆ]:")
        print(f"Loại ngoại lệ: {type(e).__name__}")
        print(f"Chi tiết lỗi:\n{str(e)[:500]}...")


def exp4_append_on_aggregation(spark):
    print("\n" + "=" * 70)
    print("THỬ NGHIỆM 4: ĐỔI outputMode SANG 'append' TRÊN BẢN AGGREGATE")
    print("=" * 70)
    reset_dir(STREAM_INPUT_DIR)
    ckpt = os.path.join(CHECKPOINT_EXP_DIR, "exp4")
    reset_dir(ckpt)

    df = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(STREAM_INPUT_DIR)
    )
    agg = df.groupBy("province").count()

    try:
        query = (
            agg.writeStream
            .outputMode("append")
            .format("console")
            .option("checkpointLocation", ckpt)
            .start()
        )
        query.stop()
    except Exception as e:
        print("[KẾT QUẢ THỬ NGHIỆM 4 - BẮT ĐƯỢC NGOẠI LỆ]:")
        print(f"Loại ngoại lệ: {type(e).__name__}")
        print(f"Chi tiết lỗi:\n{str(e)[:500]}...")


def exp5_window_dedup_on_stream(spark):
    print("\n" + "=" * 70)
    print("THỬ NGHIỆM 5: THỬ DEDUP BẰNG Window + row_number() TRÊN STREAM")
    print("=" * 70)
    reset_dir(STREAM_INPUT_DIR)
    ckpt = os.path.join(CHECKPOINT_EXP_DIR, "exp5")
    reset_dir(ckpt)

    df = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(STREAM_INPUT_DIR)
    )

    try:
        w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
        dedup_df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)

        query = (
            dedup_df.writeStream
            .outputMode("append")
            .format("console")
            .option("checkpointLocation", ckpt)
            .start()
        )
        query.stop()
    except Exception as e:
        print("[KẾT QUẢ THỬ NGHIỆM 5 - BẮT ĐƯỢC NGOẠI LỆ]:")
        print(f"Loại ngoại lệ: {type(e).__name__}")
        print(f"Chi tiết lỗi:\n{str(e)[:500]}...")


def exp3_redrop_same_file(spark):
    print("\n" + "=" * 70)
    print("THỬ NGHIỆM 3: THẢ LẠI CÙNG MỘT FILE ĐÃ THẢ TRƯỚC ĐÓ")
    print("=" * 70)
    reset_dir(STREAM_INPUT_DIR)
    ckpt = os.path.join(CHECKPOINT_EXP_DIR, "exp3")
    reset_dir(ckpt)

    # Đưa orders_1.csv vào trước
    shutil.copy(os.path.join(RAW_SOURCE_DIR, "orders_1.csv"), os.path.join(STREAM_INPUT_DIR, "orders_1.csv"))

    batches_processed = []

    class TestListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            pass
        def onQueryProgress(self, event):
            batches_processed.append((event.progress.batchId, event.progress.numInputRows))
            print(f"[EXP 3] Micro-batch {event.progress.batchId} processed, numInputRows: {event.progress.numInputRows}")
        def onQueryTerminated(self, event):
            pass

    listener = TestListener()
    spark.streams.addListener(listener)

    df = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(STREAM_INPUT_DIR)
    )

    query = (
        df.writeStream
        .outputMode("append")
        .format("console")
        .trigger(processingTime="2 seconds")
        .option("checkpointLocation", ckpt)
        .start()
    )

    # Chờ file 1 xử lý xong
    time.sleep(5)
    print("\n--> Đang ghi đè / thả lại file 'orders_1.csv' vào stream_input...")
    shutil.copy(os.path.join(RAW_SOURCE_DIR, "orders_1.csv"), os.path.join(STREAM_INPUT_DIR, "orders_1.csv"))
    time.sleep(6)

    query.stop()
    spark.streams.removeListener(listener)

    print("\n[KẾT QUẢ THỬ NGHIỆM 3]:")
    print(f"Lịch sử các batch đã kích hoạt: {batches_processed}")
    if len(batches_processed) == 1:
        print("=> HIỆN TƯỢNG: Spark hoàn toàn BỎ QUA file đã thả lại! Không kích hoạt thêm micro-batch nào vì tên file đã nằm trong checkpoint log.")
    else:
        print(f"=> HIỆN TƯỢNG: Batch thứ 2 có số dòng là {batches_processed[-1][1]}.")


def exp2_trigger_20s(spark):
    print("\n" + "=" * 70)
    print("THỬ NGHIỆM 2: ĐỔI TRIGGER THÀNH 20 GIÂY RỒI THẢ FILE MỚI")
    print("=" * 70)
    reset_dir(STREAM_INPUT_DIR)
    ckpt = os.path.join(CHECKPOINT_EXP_DIR, "exp2")
    reset_dir(ckpt)

    timestamps = []

    class TriggerListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            pass
        def onQueryProgress(self, event):
            now = time.time()
            timestamps.append((event.progress.batchId, now))
            print(f"[EXP 2] Micro-batch {event.progress.batchId} hoàn thành lúc: {time.strftime('%H:%M:%S', time.localtime(now))}")
        def onQueryTerminated(self, event):
            pass

    listener = TriggerListener()
    spark.streams.addListener(listener)

    df = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(STREAM_INPUT_DIR)
    )

    query = (
        df.writeStream
        .outputMode("append")
        .format("console")
        .trigger(processingTime="20 seconds")
        .option("checkpointLocation", ckpt)
        .start()
    )

    print(f"Trigger = 20s. Bắt đầu đếm thời gian lúc: {time.strftime('%H:%M:%S', time.localtime(time.time()))}")
    time.sleep(2)
    print(f"Thả orders_1.csv vào lúc: {time.strftime('%H:%M:%S', time.localtime(time.time()))}")
    shutil.copy(os.path.join(RAW_SOURCE_DIR, "orders_1.csv"), os.path.join(STREAM_INPUT_DIR, "orders_1.csv"))

    # Chờ 25s để thấy chu kỳ 20s quét
    time.sleep(25)
    query.stop()
    spark.streams.removeListener(listener)

    print("\n[KẾT QUẢ THỬ NGHIỆM 2]:")
    print("=> HIỆN TƯỢNG: Khi đặt trigger 20 giây, dù file mới được thả ngay lập tức, Spark Engine vẫn phải chờ đến mốc chu kỳ 20 giây tiếp theo của Trigger clock thì mới quét thư mục và xử lý micro-batch.")


def main():
    spark = (
        SparkSession.builder
        .appName("Exercise11_Req4_Experiments")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    exp1_no_schema(spark)
    exp4_append_on_aggregation(spark)
    exp5_window_dedup_on_stream(spark)
    exp3_redrop_same_file(spark)
    exp2_trigger_20s(spark)

    spark.stop()
    print("\n" + "=" * 70)
    print("ĐÃ HOÀN THÀNH TẤT CẢ 5 THỬ NGHIỆM CỦA YÊU CẦU 4")
    print("=" * 70)


if __name__ == "__main__":
    main()
