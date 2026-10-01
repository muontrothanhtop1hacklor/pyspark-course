import os
import sys
import shutil

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


def run_output_mode(mode="complete", duration_seconds=30):
    print("\n" + "=" * 80)
    print(f"YÊU CẦU 1: TEST OUTPUT MODE - {mode.upper()}")
    print("=" * 80)

    spark = (
        SparkSession.builder
        .appName(f"Exercise14_Req1_OutputMode_{mode}")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Bảng customers.csv (Batch)
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_type", StringType(), True)
    ])
    customers_df = spark.read.format("csv").option("header", "true").schema(customer_schema).load(CUSTOMERS_CSV_PATH)

    # 2. Luồng orders (Streaming)
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
        .filter(F.col("parsed.order_id").isNotNull())
        .select(
            F.col("parsed.order_id").alias("order_id"),
            F.col("parsed.customer_id").alias("customer_id"),
            F.col("parsed.province").alias("province"),
            F.expr("try_cast(parsed.amount AS DOUBLE)").alias("amount_clean")
        )
    )

    # 3. Join và Aggregate
    enriched_stream = parsed_orders.join(customers_df, on="customer_id", how="left")
    
    agg_stream = enriched_stream.groupBy("province").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("amount_clean").alias("total_amount")
    )

    # 4. In ra console với output mode được chọn
    try:
        print(f"\n[STREAM] Đang chạy với Output Mode: {mode}... Chờ {duration_seconds}s...")
        query = (
            agg_stream.writeStream
            .format("console")
            .outputMode(mode)
            .option("truncate", "false")
            .start()
        )
        query.awaitTermination(timeout=duration_seconds)
        query.stop()
        print(f"[STREAM] Đã hoàn tất test {mode}.\n")
    except Exception as e:
        print(f"\n[LỖI] Xảy ra lỗi khi chạy chế độ '{mode}':\n{e}\n")
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        mode = "complete"
    
    # Chúng ta có thể test: complete, update, append
    run_output_mode(mode, duration_seconds=20)
