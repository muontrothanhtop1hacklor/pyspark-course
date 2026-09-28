"""
Bài lab: Schema tự khai báo, validate dữ liệu, write mode (overwrite/append), partitionBy
===========================================================================================
Chạy: python3 schema_validate_lab.py
Yêu cầu: pyspark đã cài. Có orders_dirty.csv cùng thư mục.
"""

import os
import shutil
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(BASE_DIR, "orders_dirty.csv")
OUT_ROOT = os.path.join(BASE_DIR, "output")

spark = SparkSession.builder.appName("SchemaValidateLab").master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def count_part_files(path):
    files = glob.glob(os.path.join(path, "**", "part-*"), recursive=True)
    return [f for f in files if not f.endswith(".crc")]


def show_folder_tree(path, max_lines=30):
    lines = []
    for root, dirs, filenames in os.walk(path):
        rel = os.path.relpath(root, path)
        depth = 0 if rel == "." else rel.count(os.sep) + 1
        indent = "  " * depth
        base = os.path.basename(root) if rel != "." else os.path.basename(path)
        lines.append(f"{indent}{base}/")
        for fn in sorted(filenames):
            if fn.endswith(".crc"):
                continue
            lines.append(f"{indent}  {fn}")
    return "\n".join(lines[:max_lines]) + ("\n..." if len(lines) > max_lines else "")


# ============================================================================
# YÊU CẦU 1 — Đọc dữ liệu với schema tự khai báo (không inferSchema)
# ============================================================================
print("=" * 70)
print("YÊU CẦU 1: Đọc dữ liệu với schema tự khai báo")
print("=" * 70)

orders_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=True),
    StructField("province", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),  # để STRING, tự validate format sau
])

df = spark.read.schema(orders_schema).option("header", True).csv(INPUT_CSV)

print("\nSchema đã khai báo:")
df.printSchema()

total_count = df.count()
print(f"Tổng số dòng: {total_count}")

print("\nSố dòng null theo từng cột (do thiếu dữ liệu hoặc parse sai kiểu):")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# So sánh với inferSchema để thấy khác biệt
df_infer = spark.read.option("header", True).option("inferSchema", True).csv(INPUT_CSV)
print("Schema khi dùng inferSchema=True (để so sánh):")
df_infer.printSchema()

print("""
--- Giải thích ---
inferSchema=True: Spark phải ĐỌC QUA TOÀN BỘ (hoặc một phần lớn) file 1 lần chỉ để đoán kiểu
dữ liệu cho từng cột trước khi đọc thật -> tốn thêm 1 pass dữ liệu, và kiểu đoán được có thể
KHÔNG khớp ý muốn (vd cột amount có lẫn text "N/A" -> Spark có thể đoán ra StringType thay vì
DoubleType, khiến các bước tính toán số học phía sau bị sai hoặc lỗi).

Khai báo schema thủ công (StructType): không cần pass đọc-dò-kiểu, đọc nhanh hơn với file lớn,
và QUAN TRỌNG NHẤT là kiểm soát được kiểu dữ liệu mong muốn cho từng cột — giá trị nào không
đúng kiểu khai báo (vd "N/A" cho cột DoubleType) sẽ tự động thành NULL thay vì làm sai lệch
kiểu của cả cột.

Trong ETL thực tế, dữ liệu nguồn thường không đảm bảo sạch 100% và schema có thể đổi bất ngờ
(thêm cột, đổi tên, đổi kiểu) — khai báo schema tường minh giúp: (1) phát hiện lỗi SỚM ngay khi
đọc thay vì lỗi ngầm ở bước tính toán sau, (2) đảm bảo pipeline ổn định, không phụ thuộc vào
việc Spark "đoán đúng" kiểu dữ liệu mỗi lần chạy, (3) tài liệu hoá rõ ràng cấu trúc dữ liệu kỳ
vọng, dễ review và bảo trì.
""")


# ============================================================================
# YÊU CẦU 2 — Xử lý & validate: chuẩn hóa status, kiểm tra amount/date/province
# ============================================================================
print("=" * 70)
print("YÊU CẦU 2: Chuẩn hóa & validate, tách valid/invalid")
print("=" * 70)

df_std = df.withColumn("status", F.upper(F.trim(F.col("status"))))

# order_date hợp lệ nếu parse được theo format yyyy-MM-dd VÀ ra đúng ngày đó (không bị Spark tự "sửa")
df_std = df_std.withColumn("order_date_parsed", F.expr("try_to_date(order_date, 'yyyy-MM-dd')"))

df_flagged = df_std.withColumn(
    "error_reason",
    F.when(F.col("amount").isNull() | (F.col("amount") <= 0), "INVALID_AMOUNT")
     .when(F.col("order_date_parsed").isNull(), "INVALID_DATE")
     .when(F.col("province").isNull() | (F.trim(F.col("province")) == ""), "MISSING_PROVINCE")
     .otherwise(None)
)

valid_orders = df_flagged.filter(F.col("error_reason").isNull()).drop("error_reason", "order_date_parsed")
invalid_orders = df_flagged.filter(F.col("error_reason").isNotNull()).drop("order_date_parsed")

n_valid = valid_orders.count()
n_invalid = invalid_orders.count()
print(f"valid_orders: {n_valid} dòng")
print(f"invalid_orders: {n_invalid} dòng")
print(f"Tổng kiểm tra: {n_valid + n_invalid} (kỳ vọng = {total_count})")

print("\nPhân bố lỗi trong invalid_orders:")
invalid_orders.groupBy("error_reason").count().orderBy(F.desc("count")).show()

print("Ví dụ vài dòng invalid_orders:")
invalid_orders.select("order_id", "amount", "order_date", "province", "error_reason").show(10, truncate=False)


# ============================================================================
# YÊU CẦU 3 — Ghi Parquet: bình thường vs partitionBy("province")
# ============================================================================
print("=" * 70)
print("YÊU CẦU 3: Ghi Parquet — bình thường vs partitionBy")
print("=" * 70)

out_valid = os.path.join(OUT_ROOT, "valid_orders")
out_valid_part = os.path.join(OUT_ROOT, "valid_orders_partitioned")
out_invalid = os.path.join(OUT_ROOT, "invalid_orders")

for p in [out_valid, out_valid_part, out_invalid]:
    if os.path.exists(p):
        shutil.rmtree(p)

valid_orders.write.mode("overwrite").parquet(out_valid)
valid_orders.write.mode("overwrite").partitionBy("province").parquet(out_valid_part)
invalid_orders.write.mode("overwrite").parquet(out_invalid)

files_plain = count_part_files(out_valid)
files_part = count_part_files(out_valid_part)

print(f"\nCách 1 (ghi bình thường) -> {len(files_plain)} file part-*, cấu trúc phẳng")
print(f"Cách 2 (partitionBy('province')) -> {len(files_part)} file part-*, chia theo thư mục con province=<value>/")

print("\nCấu trúc folder — ghi bình thường:")
print(show_folder_tree(out_valid))
print("\nCấu trúc folder — partitionBy('province'):")
print(show_folder_tree(out_valid_part))

print("""
--- Giải thích khác biệt ---
Cách 1 ghi TẤT CẢ dữ liệu vào các file phẳng ngang hàng nhau (part-00000, part-00001...),
mỗi file có thể chứa lẫn lộn nhiều tỉnh khác nhau tuỳ theo partition trong bộ nhớ lúc ghi.
Muốn lọc theo 1 tỉnh, engine đọc phải quét (gần như) toàn bộ các file rồi filter sau.

Cách 2 (partitionBy) tạo CẤU TRÚC THƯ MỤC theo giá trị cột: mỗi tỉnh có 1 thư mục riêng
"province=<value>/", bên trong mới là các file part-*. Khi query có điều kiện lọc theo
province (vd WHERE province = 'HaNoi'), Spark/Hive/Presto có thể áp dụng "partition pruning"
— chỉ đọc đúng thư mục "province=HaNoi/" mà KHÔNG cần quét các thư mục tỉnh khác, giúp
truy vấn nhanh hơn nhiều trên dữ liệu lớn.
""")


# ============================================================================
# YÊU CẦU 4 — Append vs Overwrite
# ============================================================================
print("=" * 70)
print("YÊU CẦU 4: Append vs Overwrite")
print("=" * 70)

out_append_test = os.path.join(OUT_ROOT, "append_overwrite_test")
if os.path.exists(out_append_test):
    shutil.rmtree(out_append_test)

# Bước 1: ghi lần đầu bằng overwrite
valid_orders.write.mode("overwrite").parquet(out_append_test)
count_after_first_overwrite = spark.read.parquet(out_append_test).count()
print(f"[1] Ghi lần đầu (overwrite): count = {count_after_first_overwrite}")

# Bước 2: tạo thêm order mới rồi ghi tiếp bằng append
new_orders_data = [
    (90001, 999, "HaNoi", 1_500_000.0, "PENDING", "2025-12-01"),
    (90002, 998, "DaNang", 2_300_000.0, "COMPLETED", "2025-12-02"),
    (90003, 997, "CanTho", 800_000.0, "SHIPPING", "2025-12-03"),
]
new_orders = spark.createDataFrame(new_orders_data, orders_schema)

new_orders.write.mode("append").parquet(out_append_test)
count_after_append = spark.read.parquet(out_append_test).count()
print(f"[2] Ghi thêm {new_orders.count()} order mới (append): count = {count_after_append}")
print(f"    Kỳ vọng: {count_after_first_overwrite} + {new_orders.count()} = {count_after_first_overwrite + new_orders.count()}")

# Bước 3: chạy lại overwrite -> dữ liệu cũ có bị thay thế không?
valid_orders.write.mode("overwrite").parquet(out_append_test)
count_after_second_overwrite = spark.read.parquet(out_append_test).count()
print(f"[3] Ghi lại bằng overwrite (dữ liệu gốc, không có order mới): count = {count_after_second_overwrite}")
print(f"    Kỳ vọng = {count_after_first_overwrite} (bằng đúng lần overwrite đầu, "
      f"tức là 3 order mới thêm ở bước [2] đã BỊ XOÁ MẤT)")

check_new_orders_gone = spark.read.parquet(out_append_test).filter(F.col("order_id") >= 90001).count()
print(f"    Số order mới (id >= 90001) còn tồn tại sau overwrite: {check_new_orders_gone} (kỳ vọng = 0)")


# ============================================================================
# YÊU CẦU 5 — Đọc lại & validate output
# ============================================================================
print("=" * 70)
print("YÊU CẦU 5: Đọc lại Parquet & validate")
print("=" * 70)

df_read_back = spark.read.parquet(out_valid)

print("\nSchema sau khi đọc lại Parquet:")
df_read_back.printSchema()

count_read_back = df_read_back.count()
print(f"Total count sau khi đọc lại: {count_read_back} (trước khi ghi: {n_valid})")
print(f"=> {'KHỚP' if count_read_back == n_valid else 'KHÔNG KHỚP'}")

print("\nCount theo province (đọc lại từ Parquet):")
count_by_province_after = df_read_back.groupBy("province").count().orderBy("province")
count_by_province_after.show()

count_by_province_before = valid_orders.groupBy("province").count().orderBy("province")

diff_count = count_by_province_after.exceptAll(count_by_province_before).count() \
    + count_by_province_before.exceptAll(count_by_province_after).count()
print(f"Số dòng lệch giữa count-theo-tỉnh TRƯỚC và SAU khi ghi/đọc lại: {diff_count} (kỳ vọng = 0)")

print("\nTổng amount theo province (đọc lại từ Parquet):")
sum_by_province_after = df_read_back.groupBy("province").agg(F.sum("amount").alias("total_amount")).orderBy("province")
sum_by_province_after.show()

sum_by_province_before = valid_orders.groupBy("province").agg(F.sum("amount").alias("total_amount")).orderBy("province")

# so sánh tổng amount theo tỉnh giữa trước và sau khi ghi/đọc lại (làm tròn để tránh sai số float vặt)
compare_sum = sum_by_province_after.withColumnRenamed("total_amount", "total_amount_after") \
    .join(
        sum_by_province_before.withColumnRenamed("total_amount", "total_amount_before"),
        on="province"
    ).withColumn(
        "diff", F.round(F.col("total_amount_after") - F.col("total_amount_before"), 2)
    )
print("Đối chiếu tổng amount theo tỉnh (trước vs sau khi ghi/đọc lại):")
compare_sum.orderBy("province").show()
max_diff = compare_sum.agg(F.max(F.abs(F.col("diff")))).collect()[0][0]
print(f"Lệch lớn nhất giữa trước/sau: {max_diff} (kỳ vọng = 0.0 hoặc rất nhỏ do sai số float)")

spark.stop()
print("\nHoàn tất toàn bộ 5 yêu cầu.")
