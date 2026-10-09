import os
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def run_silver(spark: SparkSession, lakehouse_dir: str):
    print("=== [SILVER] Cleansing, Enriching BHXH Data ===")
    
    bronze_dir = os.path.join(lakehouse_dir, "bronze")
    silver_dir = os.path.join(lakehouse_dir, "silver")
    
    print("Reading Bronze data...")
    df_master = spark.read.parquet(os.path.join(bronze_dir, "MASTER"))
    df_detail = spark.read.parquet(os.path.join(bronze_dir, "DETAIL"))
    
    df_ml_labels = None
    if os.path.exists(os.path.join(bronze_dir, "ML_LABELS")):
        df_ml_labels = spark.read.parquet(os.path.join(bronze_dir, "ML_LABELS"))
        
    df_ml_anomaly = None
    if os.path.exists(os.path.join(bronze_dir, "ML_ANOMALY")):
        df_ml_anomaly = spark.read.parquet(os.path.join(bronze_dir, "ML_ANOMALY"))

    print("Enriching MASTER...")
    if df_ml_labels is not None:
        df_master_enriched = df_master.join(df_ml_labels, on="SO_SO_BHXH", how="left")
    else:
        df_master_enriched = df_master
        
    print("Enriching DETAIL...")
    if df_ml_anomaly is not None:
        df_detail_enriched = df_detail.join(df_ml_anomaly, on="ID_CHI_TIET", how="left")
    else:
        df_detail_enriched = df_detail
        
    # Write to Silver layer
    silver_master_out = os.path.join(silver_dir, "MASTER_ENRICHED")
    silver_detail_out = os.path.join(silver_dir, "DETAIL_ENRICHED")
    
    print(f"Writing Silver layer -> {silver_master_out}")
    df_master_enriched.write.mode("overwrite").parquet(silver_master_out)
    
    print(f"Writing Silver layer -> {silver_detail_out}")
    # Phân vùng theo MA_TINH vì dữ liệu lớn
    df_detail_enriched.write.mode("overwrite").partitionBy("MA_TINH").parquet(silver_detail_out)
    
    print("Hoàn thành quá trình Silver layer!")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lakehouse", type=str, required=True, help="Thư mục Lakehouse")
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("ETL_SILVER_BHXH") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        run_silver(spark, args.lakehouse)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
