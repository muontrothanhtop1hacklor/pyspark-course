import os
import sys
import shutil

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CUSTOMERS_CSV_PATH = os.path.join(BASE_DIR, "customers.csv")
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "orders_stream"

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints")

def clean_dirs():
    # Only clean if user passes --clean flag to allow testing checkpoint recovery
    if "--clean" in sys.argv:
        print("[*] Đang xóa thư mục output và checkpoints cũ...")
        if os.path.exists(OUTPUT_DIR):
            shutil.rmtree(OUTPUT_DIR)
        if os.path.exists(CHECKPOINT_DIR):
            shutil.rmtree(CHECKPOINT_DIR)

def run_pipeline():
    clean_dirs()
    print("="*80)
    print("BÀI 15: SPARK STRUCTURED STREAMING - END-TO-END PIPELINE")
    print("="*80)

    spark = (
        SparkSession.builder
        .appName("Exercise15_EndToEndPipeline")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc customers.csv
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_type", StringType(), True)
    ])
    customers_df = spark.read.format("csv").option("header", "true").schema(customer_schema).load(CUSTOMERS_CSV_PATH)

    # 2. Đọc Kafka
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

    # 3. Parse & Transform & Clean
    parsed = (
        kafka_df
        .withColumn("value_str", F.col("value").cast(StringType()))
        .withColumn("parsed", F.from_json(F.col("value_str"), order_json_schema))
        .withColumn("is_valid_json", F.col("parsed.order_id").isNotNull())
    )

    flattened = parsed.select(
        F.col("parsed.order_id").alias("order_id"),
        F.col("parsed.customer_id").alias("customer_id"),
        F.col("parsed.province").alias("province"),
        F.col("parsed.amount").alias("amount_raw"),
        F.col("parsed.status").alias("status_raw"),
        F.col("parsed.order_date").alias("order_date_raw"),
        F.col("parsed.updated_at").alias("updated_at_raw"),
        F.col("is_valid_json"),
        F.col("value_str").alias("raw_value")
    )

    cleaned = (
        flattened
        .withColumn("status_clean", F.when(F.col("status_raw").isNotNull(), F.upper(F.trim(F.col("status_raw")))).otherwise(None))
        .withColumn("amount_clean", F.expr("try_cast(amount_raw AS DOUBLE)"))
        .withColumn("order_date_clean", F.to_date(F.try_to_timestamp(F.col("order_date_raw"), F.lit("yyyy-MM-dd"))))
        .withColumn("updated_at_ts", F.to_timestamp(F.col("updated_at_raw")))
        .withColumn("order_level", 
            F.when(F.col("amount_clean") > 1000, "HIGH")
             .when((F.col("amount_clean") > 500) & (F.col("amount_clean") <= 1000), "MEDIUM")
             .otherwise("LOW")
        )
    )

    # 4. Join Customer
    enriched = cleaned.join(customers_df, on="customer_id", how="left")

    # 5. Xác định error_reason
    final_stream = (
        enriched
        .withColumn("error_reason",
            F.when(~F.col("is_valid_json"), "INVALID_JSON")
             .when(F.col("amount_clean").isNull() | (F.col("amount_clean") <= 0), "INVALID_AMOUNT")
             .when(F.col("order_date_clean").isNull(), "INVALID_DATE")
             .when(F.col("customer_name").isNull(), "CUSTOMER_NOT_FOUND")
             .otherwise(F.lit(None).cast(StringType()))
        )
    )

    # =========================================================================
    # YÊU CẦU 3: WRITE + CHECKPOINT CHO TỪNG NHÁNH
    # =========================================================================
    valid_dir = os.path.join(OUTPUT_DIR, "valid_orders")
    invalid_dir = os.path.join(OUTPUT_DIR, "invalid_orders")
    report_dir = os.path.join(OUTPUT_DIR, "province_report")

    chk_valid = os.path.join(CHECKPOINT_DIR, "valid")
    chk_invalid = os.path.join(CHECKPOINT_DIR, "invalid")
    chk_report = os.path.join(CHECKPOINT_DIR, "report")

    # Dedup bằng foreachBatch 
    def write_valid(batch_df, batch_id):
        df = batch_df.filter(F.col("error_reason").isNull())
        if df.count() > 0:
            window_spec = Window.partitionBy("order_id").orderBy(F.col("updated_at_ts").desc())
            df_dedup = df.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1).drop("rn")
            df_dedup.write.mode("append").partitionBy("province").parquet(valid_dir)

    def write_invalid(batch_df, batch_id):
        df = batch_df.filter(F.col("error_reason").isNotNull())
        if df.count() > 0:
            df_has_id = df.filter(F.col("order_id").isNotNull())
            df_no_id = df.filter(F.col("order_id").isNull())

            if df_has_id.count() > 0:
                window_spec = Window.partitionBy("order_id").orderBy(F.col("updated_at_ts").desc())
                df_has_id_dedup = df_has_id.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1).drop("rn")
            else:
                df_has_id_dedup = df_has_id

            final_invalid = df_has_id_dedup.unionByName(df_no_id, allowMissingColumns=True)
            final_invalid.write.mode("append").parquet(invalid_dir)

    print("[STREAM] Đang khởi động 3 query chạy song song...")

    # Query 1: Valid Orders
    q1 = (
        final_stream
        .writeStream
        .foreachBatch(write_valid)
        .option("checkpointLocation", chk_valid)
        .queryName("Query_ValidOrders")
        .start()
    )

    # Query 2: Invalid Orders
    q2 = (
        final_stream
        .writeStream
        .foreachBatch(write_invalid)
        .option("checkpointLocation", chk_invalid)
        .queryName("Query_InvalidOrders")
        .start()
    )

    # Query 3: Province Report 
    report_stream = (
        final_stream
        .filter(F.col("error_reason").isNull())
        .filter(F.col("updated_at_ts").isNotNull()) 
        .withWatermark("updated_at_ts", "1 minute")
        .dropDuplicates(["order_id"]) 
        .groupBy(F.window(F.col("updated_at_ts"), "5 minutes"), F.col("province"))
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("amount_clean").alias("total_amount")
        )
    )

    q3 = (
        report_stream
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", report_dir)
        .option("checkpointLocation", chk_report)
        .queryName("Query_ProvinceReport")
        .start()
    )

    # Thêm 1 query console cho report để quan sát
    q_console = (
        report_stream
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .queryName("Query_ConsoleReport")
        .start()
    )

    print("[STREAM] Đã khởi động xong. Chạy 'python verify_outputs.py' ở terminal khác để kiểm tra.")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[STREAM] Đang dừng các query...")
        q1.stop()
        q2.stop()
        q3.stop()
        q_console.stop()
    finally:
        spark.stop()

if __name__ == "__main__":
    run_pipeline()
