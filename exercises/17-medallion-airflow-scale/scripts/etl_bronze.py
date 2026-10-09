import os
import argparse
from pyspark.sql import SparkSession

def run_bronze(spark: SparkSession, input_dir: str, output_dir: str):
    print("=== [BRONZE] Ingesting Raw BHXH Data ===")
    
    tables = ["MASTER", "DETAIL", "ML_LABELS", "ML_ANOMALY"]
    for table in tables:
        raw_path = os.path.join(input_dir, table)
        if not os.path.exists(raw_path):
            print(f"Warning: {raw_path} does not exist. Skipping.")
            continue
            
        print(f"Reading {table} from {raw_path}...")
        df_raw = spark.read.parquet(raw_path)
        print(f"Raw Input count for {table}: {df_raw.count()}")
        
        bronze_out = os.path.join(output_dir, "bronze", table)
        df_raw.write.mode("overwrite").parquet(bronze_out)
        
        df_bronze_check = spark.read.parquet(bronze_out)
        print(f"Đã lưu Bronze layer tại: {bronze_out}")
        print(f"Bronze layer count cho {table}: {df_bronze_check.count()}\n")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True, help="Thư mục chứa dữ liệu gốc")
    parser.add_argument("--output", type=str, required=True, help="Thư mục Lakehouse")
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("ETL_BRONZE_BHXH") \
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
