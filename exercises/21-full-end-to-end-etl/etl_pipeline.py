"""
Bài 21: Full End-to-End ETL Pipeline (Dữ liệu Bảo hiểm xã hội - BHXH)
Áp dụng Best Practices: Khai báo Schema, Validate, Join Optimization, và Partitioning Performance
==========================================================================================
Chạy: python3 etl_pipeline.py
"""

import os
import sys
import time
import shutil

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Sử dụng nguồn dữ liệu 1M từ bài sinh dữ liệu BHXH chuẩn của dự án (Detail table)
INPUT_DATA = os.path.join(BASE_DIR, "..", "..", "synthetic_bhxh", "output", "1M", "detail")
OUT_ROOT = os.path.join(BASE_DIR, "output_bhxh_etl")

def measure_write_time(df, path, partition_col=None):
    """Hàm hỗ trợ ghi Parquet và đo thời gian."""
    if os.path.exists(path):
        shutil.rmtree(path)
    t0 = time.time()
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)
    return time.time() - t0

spark = (
    SparkSession.builder
    .appName("Full_ETL_Pipeline_BHXH_1M")
    .master("local[*]")
    .config("spark.sql.broadcastTimeout", "36000")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("BƯỚC 1: EXTRACT - TỰ KHAI BÁO SCHEMA ĐỂ ĐỌC PARQUET")
print("=" * 70)
# Việc chỉ định Schema rõ ràng giúp bỏ qua quá trình infer metadata, tiết kiệm chi phí I/O ban đầu
bhxh_schema = StructType([
    StructField("ID_CHI_TIET", StringType(), True),
    StructField("SO_SO_BHXH", StringType(), True),
    StructField("MA_TINH", StringType(), True),
    StructField("LOAI_HINH_DN", StringType(), True),
    StructField("MUC_LUONG", LongType(), True),
    StructField("MUC_DONG_BHXH", LongType(), True)
])

# Khi đọc file parquet đã có schema sẵn, ta dùng df = read.parquet rồi select để cast hoặc filter
df_raw = spark.read.schema(bhxh_schema).parquet(INPUT_DATA).select(
    "ID_CHI_TIET", "SO_SO_BHXH", "MA_TINH", "LOAI_HINH_DN", "MUC_LUONG", "MUC_DONG_BHXH"
)
total_raw = df_raw.count()
print(f"Đã đọc {total_raw:,} dòng dữ liệu BHXH gốc (Detail 1M).")

print("\n" + "=" * 70)
print("BƯỚC 2: VALIDATE - LÀM SẠCH VÀ LỌC DỮ LIỆU RÁC")
print("=" * 70)
# Filter Rule: Yêu cầu SO_SO_BHXH không rỗng và MUC_LUONG hợp lệ (> 0)
df_valid = df_raw.filter(
    F.col("SO_SO_BHXH").isNotNull() & 
    (F.col("MUC_LUONG") > 0)
)
total_valid = df_valid.count()
print(f"Số dòng hợp lệ sau Validate: {total_valid:,} (Loại bỏ {total_raw - total_valid:,} dòng lỗi).")

print("\n" + "=" * 70)
print("BƯỚC 3: TRANSFORM - TỐI ƯU HÓA BẰNG BUILT-IN VÀ BROADCAST JOIN")
print("=" * 70)
# Chuẩn hóa tên bằng Built-in functions
df_clean = df_valid.withColumn(
    "MA_TINH_CLEAN",
    F.trim(F.upper(F.col("MA_TINH")))
)

# THAY THẾ UDF / WHEN.OTHERWISE bằng BẢNG MAP (BROADCAST JOIN)
# Phân khúc đối tượng đóng bảo hiểm dựa vào mức lương
print("=> Khởi tạo bảng Dimension (Phân loại Mức đóng) để Broadcast Join...")
df_segments = spark.sql("""
    SELECT 0 as min_luong, 4999999 as max_luong, 'PHO_THONG' as phan_khuc
    UNION ALL 
    SELECT 5000000, 14999999, 'TRUNG_CAP'
    UNION ALL 
    SELECT 15000000, 9999999999, 'CAO_CAP'
""")

# Range Join sử dụng Broadcast (Bảng df_segments cực nhỏ nên đẩy thẳng vào Memory các Executor)
df_transformed = df_clean.join(
    F.broadcast(df_segments),
    (df_clean.MUC_LUONG >= df_segments.min_luong) & (df_clean.MUC_LUONG <= df_segments.max_luong),
    "left"
).drop("min_luong", "max_luong")

print("Dữ liệu BHXH sau khi làm sạch và gắn Phân khúc (5 dòng đầu):")
df_transformed.select("SO_SO_BHXH", "MA_TINH_CLEAN", "MUC_LUONG", "phan_khuc").show(5, truncate=False)

print("\n" + "=" * 70)
print("BƯỚC 4: LOAD - PARTITIONING PERFORMANCE TEST (THỬ NGHIỆM GHI 3 CHIẾN LƯỢC)")
print("=" * 70)
# Cache DataFrame sau Transform để đảm bảo phép đo thời gian ghi (Write) chỉ tính đúng thời gian I/O
df_transformed.cache()
df_transformed.count() # Action mồi

# 1. Ghi KHÔNG Partition
path_nopart = os.path.join(OUT_ROOT, "1_no_partition")
t_nopart = measure_write_time(df_transformed, path_nopart, partition_col=None)
print(f"1. Ghi KHÔNG Partition (gom chung toàn bộ vào file): {t_nopart:.2f}s")

# 2. Ghi Partition theo mã tỉnh thành (Cardinality trung bình: 63 tỉnh)
path_part_prov = os.path.join(OUT_ROOT, "2_partition_tinh_thanh")
t_part_prov = measure_write_time(df_transformed, path_part_prov, partition_col="MA_TINH_CLEAN")
print(f"2. Ghi Partition theo 'MA_TINH_CLEAN' (Tối ưu để phân tích theo tỉnh): {t_part_prov:.2f}s")

# 3. Ghi Partition theo phân khúc (Cardinality rất thấp: 3 nhóm -> Skew)
path_part_seg = os.path.join(OUT_ROOT, "3_partition_phan_khuc")
t_part_seg = measure_write_time(df_transformed, path_part_seg, partition_col="phan_khuc")
print(f"3. Ghi Partition theo 'phan_khuc' (Ít nhóm, dữ liệu tập trung lớn): {t_part_seg:.2f}s")

print("\n=> KẾT LUẬN PARTITIONING MLOPS:")
print("- Partition theo cột có Cardinality quá cao sẽ tạo ra 'Small Files Problem' (Quá nhiều file nhỏ).")
print("- Partition theo cột quá ít giá trị (ví dụ Phân khúc) có thể dẫn tới Data Skewness.")
print("- Lựa chọn 'MA_TINH_CLEAN' là điểm cân bằng lý tưởng nhất cho Dữ liệu BHXH khi lưu trữ Lakehouse.")

spark.stop()
print("\nHoàn tất End-to-End ETL Pipeline (Bài 21).")
