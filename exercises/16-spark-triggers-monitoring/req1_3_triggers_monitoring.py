import os
import sys
import shutil
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CUSTOMERS_CSV_PATH = os.path.join(BASE_DIR, "customers.csv")
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "orders_stream"

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints")

def run_trigger_monitoring(trigger_type="processingTime"):
    print("="*80)
    print(f"YÊU CẦU 1 & 3: THỬ NGHIỆM TRIGGER '{trigger_type}' VÀ MONITORING")
    print("="*80)

    spark = (
        SparkSession.builder
        .appName(f"Exercise16_Triggers_{trigger_type}")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Bảng customers.csv
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_type", StringType(), True)
    ])
    customers_df = spark.read.format("csv").option("header", "true").schema(customer_schema).load(CUSTOMERS_CSV_PATH)

    # Đọc Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Đếm số lượng record theo thời gian processing
    agg_stream = (
        kafka_df
        .withColumn("timestamp", F.current_timestamp())
        .groupBy(F.window("timestamp", "5 minutes"))
        .count()
    )

    # Setup Trigger
    writer = agg_stream.writeStream.format("console").outputMode("complete")
    
    if trigger_type == "once":
        # Deprecated trong bản Spark mới, nhưng để minh hoạ
        writer = writer.trigger(once=True)
    elif trigger_type == "availableNow":
        writer = writer.trigger(availableNow=True)
    else:
        # Mặc định là processingTime
        writer = writer.trigger(processingTime="5 seconds")

    query = writer.queryName("TriggerTestQuery").start()

    # YÊU CẦU 3: Theo dõi query đang chạy
    print(f"\n[MONITORING] Query Status ban đầu: {query.status}")
    
    # Vòng lặp monitor
    for _ in range(15):  # Theo dõi trong khoảng 30s
        time.sleep(2)
        if query.isActive:
            print("\n" + "-"*40)
            print(f"Status: {query.status}")
            if query.lastProgress:
                progress = query.lastProgress
                print(f"Batch ID: {progress.get('batchId')}")
                print(f"Input Rows/sec: {progress.get('inputRowsPerSecond', 0)}")
                print(f"Processed Rows/sec: {progress.get('processedRowsPerSecond', 0)}")
                print(f"Total Rows Processed in this batch: {progress.get('numInputRows', 0)}")
            else:
                print("Chưa có progress nào.")
        else:
            print("\n[MONITORING] Query đã dừng (Inactive).")
            break

    # Nếu trigger là once/availableNow thì nó tự tắt, 
    # Nếu processingTime thì ta chủ động tắt sau vòng lặp monitor
    if query.isActive:
        print("\n[STREAM] Đang chủ động dừng query...")
        query.stop()
        
    # In recent progress
    recent = query.recentProgress
    if recent:
        print(f"\n[MONITORING] recentProgress chứa {len(recent)} object(s).")
    
    spark.stop()

if __name__ == "__main__":
    t_type = "processingTime"
    if len(sys.argv) > 1:
        t_type = sys.argv[1]
    run_trigger_monitoring(t_type)
