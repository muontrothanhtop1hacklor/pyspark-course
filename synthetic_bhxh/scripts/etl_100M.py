import os
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

def build_etl(spark: SparkSession, input_dir: str, error_keys_path: str, output_dir: str):
    """
    ETL job cho dữ liệu BHXH 100M dòng.
    
    Các bước:
    1. Đọc dữ liệu từ MASTER, DETAIL, ML_LABELS, ML_ANOMALY, ERROR_KEYS.
    2. Join các bảng.
    3. Tạo các bảng tổng hợp (Aggregations) phục vụ phân tích.
    4. Ghi kết quả ra thư mục output.
    """
    
    print("1. Đọc dữ liệu từ thư mục 100M...")
    
    # 1. Load Data
    df_master = spark.read.parquet(os.path.join(input_dir, "MASTER"))
    df_detail = spark.read.parquet(os.path.join(input_dir, "DETAIL"))
    df_ml_labels = spark.read.parquet(os.path.join(input_dir, "ML_LABELS"))
    df_ml_anomaly = spark.read.parquet(os.path.join(input_dir, "ML_ANOMALY"))
    
    # Đọc error keys (nếu có)
    if os.path.exists(error_keys_path):
        # Định dạng file CSV error keys
        error_schema = StructType([
            StructField("SO_SO_BHXH", StringType(), True),
            StructField("ID_CHI_TIET", StringType(), True),
            StructField("ERROR_CODE", StringType(), True)
        ])
        df_errors = spark.read.csv(error_keys_path, schema=error_schema, header=False)
    else:
        df_errors = None
        print(f"Không tìm thấy file error keys tại {error_keys_path}. Bỏ qua.")

    print("2. Chuyển đổi và Join dữ liệu (Transform)...")
    
    # 2. Transform & Join
    # Gắn nhãn ML vào MASTER
    df_master_enriched = df_master.join(
        df_ml_labels,
        on="SO_SO_BHXH",
        how="left"
    )
    
    # Gắn cờ Anomaly và Error vào DETAIL
    df_detail_enriched = df_detail.join(
        df_ml_anomaly,
        on="ID_CHI_TIET",
        how="left"
    )
    
    if df_errors is not None:
        df_detail_enriched = df_detail_enriched.join(
            df_errors,
            on=["SO_SO_BHXH", "ID_CHI_TIET"],
            how="left"
        )
    else:
        df_detail_enriched = df_detail_enriched.withColumn("ERROR_CODE", F.lit(None))
        
    # Tạo bảng Denormalized (MASTER + DETAIL)
    # df_denorm = df_detail_enriched.join(df_master_enriched, on="SO_SO_BHXH", how="inner")
    
    print("3. Tạo các bảng tổng hợp (Aggregations)...")
    
    # Tính tổng số tiền đóng BHXH và số tháng đóng của mỗi cá nhân
    df_agg_person = df_detail_enriched.groupBy("SO_SO_BHXH").agg(
        F.sum("MUC_DONG_BHXH").alias("TONG_MUC_DONG"),
        F.sum("SO_THANG_DONG").alias("TONG_SO_THANG"),
        F.count("ID_CHI_TIET").alias("SO_LUOT_DONG"),
        F.avg("MUC_LUONG").alias("LUONG_BTHQ")
    ).join(df_master_enriched, on="SO_SO_BHXH", how="inner")
    
    # Thống kê theo Đơn vị (Doanh nghiệp)
    df_agg_company = df_detail_enriched.groupBy("MA_DON_VI", "TEN_DON_VI").agg(
        F.countDistinct("SO_SO_BHXH").alias("SO_NGUOI_DONG"),
        F.sum("MUC_DONG_BHXH").alias("TONG_DOANH_THU_BHXH"),
        F.sum(F.when(F.col("is_anomaly") == 1, 1).otherwise(0)).alias("SO_LUOT_BATHUONG")
    )
    
    print("4. Ghi kết quả ra thư mục đích (Load)...")
    
    # 4. Load (Write)
    output_master_path = os.path.join(output_dir, "MASTER_ENRICHED")
    output_detail_path = os.path.join(output_dir, "DETAIL_ENRICHED")
    output_agg_person_path = os.path.join(output_dir, "AGG_PERSON")
    output_agg_company_path = os.path.join(output_dir, "AGG_COMPANY")
    
    # Cấu hình ghi Parquet (Overwrite nếu chạy lại)
    df_master_enriched.write.mode("overwrite").parquet(output_master_path)
    print(f" Đã ghi: {output_master_path}")
    
    # DETAIL bảng lớn, nên phân vùng theo MA_TINH (nếu có nhu cầu)
    df_detail_enriched.write.mode("overwrite").partitionBy("MA_TINH").parquet(output_detail_path)
    print(f" Đã ghi: {output_detail_path}")
    
    df_agg_person.write.mode("overwrite").parquet(output_agg_person_path)
    print(f" Đã ghi: {output_agg_person_path}")
    
    df_agg_company.write.mode("overwrite").parquet(output_agg_company_path)
    print(f" Đã ghi: {output_agg_company_path}")
    
    print("Hoàn tất quy trình ETL!")

def main():
    parser = argparse.ArgumentParser(description="ETL Process cho BHXH 100M")
    parser.add_argument("--input-dir", type=str, default="output/100M", help="Thư mục chứa dữ liệu 100M")
    parser.add_argument("--error-keys", type=str, default="_answer_key/error_keys_100M.csv", help="Đường dẫn file error keys")
    parser.add_argument("--output-dir", type=str, default="output/etl_results_100M", help="Thư mục ghi kết quả")
    
    args = parser.parse_args()
    
    # Khởi tạo Spark Session
    # Lưu ý: Với 100M dòng, cần cấp thêm RAM cho executor/driver
    spark = SparkSession.builder \
        .appName("ETL_BHXH_100M") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        build_etl(spark, args.input_dir, args.error_keys, args.output_dir)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
