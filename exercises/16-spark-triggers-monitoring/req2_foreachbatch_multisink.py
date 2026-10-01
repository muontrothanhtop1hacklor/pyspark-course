import os
import sys
import shutil

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "orders_stream"

OUTPUT_DIR = os.path.join(BASE_DIR, "output", "multisink_parquet")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints", "multisink")

def clean_dirs():
    if os.path.exists(os.path.join(BASE_DIR, "output")):
        shutil.rmtree(os.path.join(BASE_DIR, "output"))
    if os.path.exists(os.path.join(BASE_DIR, "checkpoints")):
        shutil.rmtree(os.path.join(BASE_DIR, "checkpoints"))

def run_multisink_foreachbatch():
    clean_dirs()
    print("="*80)
    print("YÊU CẦU 2: GHI NHIỀU SINK BẰNG 1 FOREACHBATCH")
    print("="*80)

    spark = (
        SparkSession.builder
        .appName("Exercise16_MultiSink")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
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

    parsed = kafka_df.select(F.col("value").cast("string").alias("json_payload"))

    # Định nghĩa hàm foreachBatch để ghi ra cả Parquet và Console trong CÙNG MỘT BATCH
    def write_multiple_sinks(batch_df, batch_id):
        # Lưu cache để không phải thực thi lại transformation nhiều lần (nếu có logic phức tạp)
        batch_df.cache()
        count = batch_df.count()
        print(f"\n[Batch {batch_id}] Đang xử lý {count} bản ghi.")
        
        if count > 0:
            # 1. Ghi ra Console (Sink 1)
            print(f"> Ghi Sink 1 (Console) cho Batch {batch_id}:")
            batch_df.show(5, truncate=False)

            # 2. Ghi ra Parquet (Sink 2)
            print(f"> Ghi Sink 2 (Parquet) cho Batch {batch_id} vào {OUTPUT_DIR}")
            batch_df.write.mode("append").parquet(OUTPUT_DIR)
        
        batch_df.unpersist()

    print("[STREAM] Khởi động query...")
    query = (
        parsed
        .writeStream
        .foreachBatch(write_multiple_sinks)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .queryName("MultiSinkQuery")
        .start()
    )

    try:
        query.awaitTermination(timeout=30)
    except KeyboardInterrupt:
        pass
    finally:
        query.stop()
        spark.stop()
        print("\n[STREAM] Đã hoàn tất test foreachBatch.\n")

if __name__ == "__main__":
    run_multisink_foreachbatch()
