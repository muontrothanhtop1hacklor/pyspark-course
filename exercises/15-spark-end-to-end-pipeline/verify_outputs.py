import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

def verify():
    spark = (
        SparkSession.builder
        .appName("VerifyOutputs")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    valid_dir = os.path.join(OUTPUT_DIR, "valid_orders")
    invalid_dir = os.path.join(OUTPUT_DIR, "invalid_orders")
    report_dir = os.path.join(OUTPUT_DIR, "province_report")
    
    print("\n================= KIỂM TRA VALID ORDERS =================")
    if os.path.exists(valid_dir):
        try:
            df_valid = spark.read.parquet(valid_dir)
            df_valid.printSchema()
            count_valid = df_valid.count()
            print(f"Tổng số Valid Orders: {count_valid}")
            df_valid.show(10, truncate=False)
        except Exception as e:
            print("Chưa có data hoặc lỗi:", e)
    else:
        print(f"Thư mục không tồn tại: {valid_dir}")

    print("\n================ KIỂM TRA INVALID ORDERS ================")
    if os.path.exists(invalid_dir):
        try:
            df_invalid = spark.read.parquet(invalid_dir)
            df_invalid.printSchema()
            count_invalid = df_invalid.count()
            print(f"Tổng số Invalid Orders: {count_invalid}")
            df_invalid.select("order_id", "error_reason", "amount_clean", "order_date_raw", "raw_value").show(10, truncate=False)
        except Exception as e:
            print("Chưa có data hoặc lỗi:", e)
    else:
        print(f"Thư mục không tồn tại: {invalid_dir}")

    print("\n================ KIỂM TRA PROVINCE REPORT ===============")
    if os.path.exists(report_dir):
        try:
            df_report = spark.read.parquet(report_dir)
            df_report.printSchema()
            count_report = df_report.count()
            print(f"Tổng số bản ghi Report: {count_report}")
            df_report.show(10, truncate=False)
        except Exception as e:
            print("Chưa có data hoặc lỗi:", e)
    else:
        print(f"Thư mục không tồn tại: {report_dir}")

    spark.stop()

if __name__ == "__main__":
    verify()
