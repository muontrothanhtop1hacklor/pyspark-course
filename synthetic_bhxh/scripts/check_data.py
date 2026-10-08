import argparse
import sys
import yaml
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, isnull, expr

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scale', type=str, required=True, choices=['1M', '10M', '100M'])
    args = parser.parse_args()

    scale_dir = f"synthetic_bhxh/output/{args.scale}"
    if not Path(scale_dir).exists():
        print(f"Data for scale {args.scale} not found at {scale_dir}")
        sys.exit(1)

    spark = SparkSession.builder \
        .appName("CheckSyntheticData") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Loading data from {scale_dir}...")
    try:
        master_df = spark.read.parquet(f"{scale_dir}/MASTER")
        detail_df = spark.read.parquet(f"{scale_dir}/DETAIL")
    except Exception as e:
        print(f"Failed to load parquet files: {e}")
        sys.exit(1)

    with open("synthetic_bhxh/config/params.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    target_details = {'1M': 1000000, '10M': 10000000, '100M': 100000000}[args.scale]

    print("\n--- KIỂM TRA DỮ LIỆU ---")
    
    # 1. Row counts
    master_count = master_df.count()
    detail_count = detail_df.count()
    print(f"[1] Số dòng Master: {master_count}")
    
    if detail_count == target_details:
        print(f"[1] Số dòng Detail: {detail_count} (ĐẠT)")
    else:
        print(f"[1] Số dòng Detail: {detail_count} - Kì vọng: {target_details} (KHÔNG ĐẠT)")

    # 2. FK consistency
    missing_fk = detail_df.join(master_df, "SO_SO_BHXH", "left_anti").count()
    if missing_fk == 0:
        print(f"[2] FK Master-Detail: 0 dòng mồ côi (ĐẠT)")
    else:
        print(f"[2] FK Master-Detail: {missing_fk} dòng mồ côi (DO CỐ Ý CHÈN LỖI/KHÔNG ĐẠT)")

    # 3. Rate calculation consistency (Tolerance due to intentional errors)
    # Since we injected intentional negative salaries or wrong rates, we might see some violations.
    # We check if the majority of records obey the rule.
    rates = config['rates']
    
    # Check TIEN_BHYT = base * 0.045
    bhyt_check = detail_df.withColumn(
        "expected_bhyt", (col("MUC_DONG_BHXH") * rates['bhyt']).cast("int")
    ).filter(expr("abs(TIEN_BHYT - expected_bhyt) <= 1"))
    
    valid_rate_count = bhyt_check.count()
    valid_ratio = valid_rate_count / detail_count
    
    if valid_ratio > 0.95:
        print(f"[3] Nhất quán tiền đóng (BHYT): {valid_ratio*100:.2f}% khớp công thức (ĐẠT - Lỗi cố ý chiếm {(1-valid_ratio)*100:.2f}%)")
    else:
        print(f"[3] Nhất quán tiền đóng (BHYT): {valid_ratio*100:.2f}% khớp công thức (KHÔNG ĐẠT)")

    # 4. Same seed check
    # We verify the first detail row id and value
    first_row = detail_df.orderBy("ID_CHI_TIET").first()
    if first_row:
        print(f"[4] Seed check (ID nhỏ nhất): {first_row['ID_CHI_TIET']} - SO_SO_BHXH: {first_row['SO_SO_BHXH']} (Xem xét đối chiếu thủ công giữa các scale)")

    print("Hoàn tất kiểm tra.")
    spark.stop()

if __name__ == "__main__":
    main()
