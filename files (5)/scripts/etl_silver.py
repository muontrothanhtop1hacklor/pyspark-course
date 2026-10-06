import os
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def run_silver(spark: SparkSession, input_dir: str, output_dir: str):
    print("=== [SILVER] Cleansing & Enrichment ===")
    from pyspark.sql.window import Window
    
    bronze_in = os.path.join(input_dir, "bronze", "orders")
    df_bronze = spark.read.parquet(bronze_in)
    
    print(f"Bronze count: {df_bronze.count()}")
    
    # 1. Deduplicate using Window
    window_spec = Window.partitionBy("order_id").orderBy(F.col("order_timestamp").desc_nulls_last())
    df_dedup = df_bronze.withColumn("rn", F.row_number().over(window_spec)) \
                        .filter(F.col("rn") == 1).drop("rn")
                        
    # 2. Add validation rules (Quarantine)
    df_validated = df_dedup.withColumn(
        "error_reason",
        F.when(F.col("total_amount") <= 0, "INVALID_AMOUNT")
         .when(F.col("quantity") < 1, "INVALID_QUANTITY")
         .when(F.col("order_date").isNull(), "INVALID_DATE")
         .otherwise("VALID")
    )
    
    # Split Valid and Invalid
    df_valid = df_validated.filter(F.col("error_reason") == "VALID").drop("error_reason")
    df_invalid = df_validated.filter(F.col("error_reason") != "VALID")
    
    # 3. Enrichment on Valid data
    df_silver = df_valid \
        .withColumn("status", F.upper(F.trim(F.col("status")))) \
        .withColumn("order_year", F.year("order_date")) \
        .withColumn("order_month", F.month("order_date")) \
        .withColumn(
            "order_level",
            F.when(F.col("total_amount") >= 5000, "HIGH")
             .when(F.col("total_amount") >= 1000, "MEDIUM")
             .otherwise("LOW")
        ) \
        .fillna({
            "rating": 3,
            "coupon_code": "NO_COUPON",
            "notes": "no_note"
        })
        
    silver_out = os.path.join(output_dir, "silver", "orders")
    invalid_out = os.path.join(output_dir, "silver", "invalid_orders")
    
    df_silver.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(silver_out)
    print(f"Đã lưu Silver layer tại: {silver_out}")
    print(f"Valid count: {df_silver.count()}")
    
    # Ghi riêng dữ liệu lỗi để kiểm toán (cách ly)
    df_invalid.write.mode("overwrite").parquet(invalid_out)
    print(f"Đã lưu Invalid Orders (Quarantine) tại: {invalid_out}")
    print(f"Invalid count: {df_invalid.count()}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lakehouse", type=str, default="c:/Users/Administrator/spark introduce learn/files (5)/1m/data/lakehouse")
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("ETL_SILVER") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_silver(spark, args.lakehouse, args.lakehouse)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
