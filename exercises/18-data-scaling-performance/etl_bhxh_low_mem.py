import os
import argparse
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def get_paths(scale: str):
    # Trỏ đến dữ liệu của bài 17 để tận dụng dữ liệu đã có (tránh duplicate file)
    base_dir = f"c:/Users/Administrator/spark introduce learn/exercises/17-medallion-airflow-scale/{scale}/data"
    
    # Tạo output thư mục cho bài 18 để không ảnh hưởng dữ liệu cũ
    out_dir = f"c:/Users/Administrator/spark introduce learn/exercises/18-data-scaling-performance/{scale}/output"
    return os.path.join(base_dir, "raw"), out_dir

def build_bhxh_etl(spark: SparkSession, input_dir: str, output_dir: str, scale: str):
    """
    ETL job cho dữ liệu BHXH tuân theo kiến trúc Medallion (Low Mem Config).
    """
    start_time = time.time()
    
    # 1. BRONZE LAYER
    print(f"\n=== [BRONZE] Xử lý lớp dữ liệu thô (Raw) cho {scale} ===")
    tables = ["MASTER", "DETAIL", "ML_LABELS", "ML_ANOMALY"]
    bronze_dir = os.path.join(output_dir, "bronze")
    
    for table in tables:
        raw_path = os.path.join(input_dir, table)
        if not os.path.exists(raw_path):
            continue
        df_raw = spark.read.parquet(raw_path)
        bronze_out = os.path.join(bronze_dir, table)
        df_raw.write.mode("overwrite").parquet(bronze_out)
    
    print(f"Đã lưu Bronze layer tại: {bronze_dir}")
    
    # 2. SILVER LAYER
    print(f"=== [SILVER] Xử lý lớp dữ liệu sạch (Cleansed) cho {scale} ===")
    df_master = spark.read.parquet(os.path.join(bronze_dir, "MASTER"))
    df_detail = spark.read.parquet(os.path.join(bronze_dir, "DETAIL"))
    
    df_ml_labels = None
    if os.path.exists(os.path.join(bronze_dir, "ML_LABELS")):
        df_ml_labels = spark.read.parquet(os.path.join(bronze_dir, "ML_LABELS"))
        
    df_ml_anomaly = None
    if os.path.exists(os.path.join(bronze_dir, "ML_ANOMALY")):
        df_ml_anomaly = spark.read.parquet(os.path.join(bronze_dir, "ML_ANOMALY"))

    # Enrich
    if df_ml_labels is not None:
        df_master_enriched = df_master.join(df_ml_labels, on="SO_SO_BHXH", how="left")
    else:
        df_master_enriched = df_master
        
    if df_ml_anomaly is not None:
        df_detail_enriched = df_detail.join(df_ml_anomaly, on="ID_CHI_TIET", how="left")
    else:
        df_detail_enriched = df_detail
        
    silver_dir = os.path.join(output_dir, "silver")
    df_master_enriched.write.mode("overwrite").parquet(os.path.join(silver_dir, "MASTER_ENRICHED"))
    df_detail_enriched.write.mode("overwrite").partitionBy("MA_TINH").parquet(os.path.join(silver_dir, "DETAIL_ENRICHED"))
    print(f"Đã lưu Silver layer tại: {silver_dir}")

    # 3. GOLD LAYER
    print(f"=== [GOLD] Xử lý lớp dữ liệu phân tích (Aggregated) cho {scale} ===")
    gold_dir = os.path.join(output_dir, "gold")
    
    df_agg_person = df_detail_enriched.groupBy("SO_SO_BHXH").agg(
        F.sum("MUC_DONG_BHXH").alias("TONG_MUC_DONG"),
        F.sum("SO_THANG_DONG").alias("TONG_SO_THANG"),
        F.count("ID_CHI_TIET").alias("SO_LUOT_DONG"),
        F.avg("MUC_LUONG").alias("LUONG_BTHQ")
    ).join(df_master_enriched, on="SO_SO_BHXH", how="inner")
    
    df_agg_person.write.mode("overwrite").parquet(os.path.join(gold_dir, "AGG_PERSON"))
    
    df_agg_company = df_detail_enriched.groupBy("MA_DON_VI", "TEN_DON_VI").agg(
        F.countDistinct("SO_SO_BHXH").alias("SO_NGUOI_DONG"),
        F.sum("MUC_DONG_BHXH").alias("TONG_DOANH_THU_BHXH"),
        F.sum(F.when(F.col("is_anomaly") == 1, 1).otherwise(0)).alias("SO_LUOT_BATHUONG")
    )
    
    df_agg_company.write.mode("overwrite").parquet(os.path.join(gold_dir, "AGG_COMPANY"))
    print(f"Đã lưu Gold layer tại: {gold_dir}")
    
    end_time = time.time()
    print(f"Hoàn tất toàn bộ quy trình ETL Medallion Architecture cho tập {scale}!")
    print(f"Thời gian thực thi: {end_time - start_time:.2f} giây")

def main():
    parser = argparse.ArgumentParser(description="Medallion ETL Process - Low Memory Config (BHXH)")
    parser.add_argument("--scale", type=str, choices=["1m", "10m", "100m"], default="1m", help="Quy mô dữ liệu (1m, 10m, 100m)")
    args = parser.parse_args()
    
    scale_configs = {
        "1m": {"memory": "1g", "partitions": "10"},     
        "10m": {"memory": "1g", "partitions": "50"},    
        "100m": {"memory": "4g", "partitions": "200"}   
    }
    cfg = scale_configs[args.scale]
    
    input_dir, output_dir = get_paths(args.scale)
    
    print(f"Đang khởi tạo Spark Session cho {args.scale} với cấu hình:")
    print(f" - memory: {cfg['memory']}")
    print(f" - partitions: {cfg['partitions']}")

    spark = SparkSession.builder \
        .appName(f"Medallion_ETL_BHXH_{args.scale.upper()}_LOW_MEM") \
        .config("spark.driver.memory", cfg["memory"]) \
        .config("spark.executor.memory", cfg["memory"]) \
        .config("spark.sql.shuffle.partitions", cfg["partitions"]) \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.3") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        build_bhxh_etl(spark, input_dir, output_dir, args.scale)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
