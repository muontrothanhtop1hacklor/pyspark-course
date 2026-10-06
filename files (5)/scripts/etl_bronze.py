import os
import argparse
from pyspark.sql import SparkSession

def run_bronze(spark: SparkSession, input_dir: str, output_dir: str):
    print("=== [BRONZE] Ingesting Raw Data ===")
    raw_path = os.path.join(input_dir, "*.parquet")
    df_raw = spark.read.parquet(raw_path)
    print(f"Raw Input count: {df_raw.count()}")
    
    bronze_out = os.path.join(output_dir, "bronze", "orders")
    df_raw.write.mode("overwrite").parquet(bronze_out)
    
    df_bronze_check = spark.read.parquet(bronze_out)
    print(f"Đã lưu Bronze layer tại: {bronze_out}")
    print(f"Bronze layer count: {df_bronze_check.count()}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="c:/Users/Administrator/spark introduce learn/files (5)/1m/data/raw", help="Thư mục chứa dữ liệu gốc")
    parser.add_argument("--output", type=str, default="c:/Users/Administrator/spark introduce learn/files (5)/1m/data/lakehouse", help="Thư mục Lakehouse")
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("ETL_BRONZE") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_bronze(spark, args.input, args.output)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
