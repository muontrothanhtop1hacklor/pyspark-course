import os
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def run_gold(spark: SparkSession, lakehouse_dir: str):
    print("=== [GOLD] Aggregation & Reporting on BHXH Data ===")
    
    silver_dir = os.path.join(lakehouse_dir, "silver")
    gold_dir = os.path.join(lakehouse_dir, "gold")
    
    print("Reading Silver data...")
    df_master = spark.read.parquet(os.path.join(silver_dir, "MASTER_ENRICHED"))
    df_detail = spark.read.parquet(os.path.join(silver_dir, "DETAIL_ENRICHED"))
    
    print("Computing AGG_PERSON (Tổng hợp cá nhân)...")
    df_agg_person = df_detail.groupBy("SO_SO_BHXH").agg(
        F.sum("MUC_DONG_BHXH").alias("TONG_MUC_DONG"),
        F.sum("SO_THANG_DONG").alias("TONG_SO_THANG"),
        F.count("ID_CHI_TIET").alias("SO_LUOT_DONG"),
        F.avg("MUC_LUONG").alias("LUONG_BTHQ")
    ).join(df_master, on="SO_SO_BHXH", how="inner")
    
    print("Computing AGG_COMPANY (Thống kê doanh nghiệp)...")
    df_agg_company = df_detail.groupBy("MA_DON_VI", "TEN_DON_VI").agg(
        F.countDistinct("SO_SO_BHXH").alias("SO_NGUOI_DONG"),
        F.sum("MUC_DONG_BHXH").alias("TONG_DOANH_THU_BHXH"),
        F.sum(F.when(F.col("is_anomaly") == 1, 1).otherwise(0)).alias("SO_LUOT_BATHUONG")
    )
    
    # Write to Gold layer
    gold_person_out = os.path.join(gold_dir, "AGG_PERSON")
    gold_company_out = os.path.join(gold_dir, "AGG_COMPANY")
    
    print(f"Writing Gold layer -> {gold_person_out}")
    df_agg_person.write.mode("overwrite").parquet(gold_person_out)
    
    print(f"Writing Gold layer -> {gold_company_out}")
    df_agg_company.write.mode("overwrite").parquet(gold_company_out)
    
    print("Hoàn thành quá trình Gold layer!")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lakehouse", type=str, required=True, help="Thư mục Lakehouse")
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("ETL_GOLD_BHXH") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        run_gold(spark, args.lakehouse)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
