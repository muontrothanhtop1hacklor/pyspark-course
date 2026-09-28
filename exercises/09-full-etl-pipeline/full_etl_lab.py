

import os
import shutil
import glob
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ORDERS_CSV = os.path.join(BASE_DIR, "orders_dup.csv")
CUSTOMERS_CSV = os.path.join(BASE_DIR, "customers_master.csv")
OUT_ROOT = os.path.join(BASE_DIR, "output")

spark = SparkSession.builder.appName("FullEtlLab").master("local[4]").getOrCreate()
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
# YÊU CẦU 1 — Read + Clean
# ============================================================================
print("=" * 70)
print("YÊU CẦU 1: Read + Clean")
print("=" * 70)

orders_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=True),
    StructField("province", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])

customers_schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("customer_name", StringType(), nullable=True),
    StructField("customer_type", StringType(), nullable=True),
])

orders_raw = spark.read.schema(orders_schema).option("header", True).csv(ORDERS_CSV)
customers = spark.read.schema(customers_schema).option("header", True).csv(CUSTOMERS_CSV)

print("Schema orders:")
orders_raw.printSchema()
print("Schema customers:")
customers.printSchema()

n_orders_raw = orders_raw.count()
n_customers = customers.count()
print(f"orders.csv: {n_orders_raw} dòng | customers.csv: {n_customers} dòng")

orders_clean = orders_raw \
    .withColumn("status", F.upper(F.trim(F.col("status")))) \
    .withColumn("order_date_parsed", F.expr("try_to_date(order_date, 'yyyy-MM-dd')")) \
    .withColumn("updated_at_parsed", F.expr("try_to_timestamp(updated_at, 'yyyy-MM-dd HH:mm:ss')"))

print("\nSố dòng null/lỗi theo từng tiêu chí kiểm tra:")
orders_clean.select(
    F.count(F.when(F.col("amount").isNull() | (F.col("amount") <= 0), True)).alias("amount_null_or_invalid"),
    F.count(F.when(F.col("order_date_parsed").isNull(), True)).alias("date_invalid"),
    F.count(F.when(F.col("updated_at_parsed").isNull(), True)).alias("updated_at_invalid"),
).show()


# ============================================================================
# YÊU CẦU 2 — Deduplicate (Window) + Validate
# ============================================================================
print("=" * 70)
print("YÊU CẦU 2: Deduplicate bằng Window + Validate")
print("=" * 70)

n_distinct_ids = orders_clean.select("order_id").distinct().count()
n_dup_ids = orders_clean.groupBy("order_id").count().filter(F.col("count") > 1).count()
print(f"order_id phân biệt: {n_distinct_ids} | order_id có bị duplicate: {n_dup_ids} "
      f"(tổng dòng gốc: {n_orders_raw}, chênh lệch = số bản ghi duplicate thừa)")

w = Window.partitionBy("order_id").orderBy(F.col("updated_at_parsed").desc_nulls_last())
orders_dedup = orders_clean.withColumn("rn", F.row_number().over(w)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

n_after_dedup = orders_dedup.count()
print(f"Sau dedup (giữ bản ghi updated_at mới nhất mỗi order_id): {n_after_dedup} dòng "
      f"(kỳ vọng = {n_distinct_ids})")

# validate + gắn error_reason, ưu tiên: INVALID_AMOUNT -> INVALID_DATE -> CUSTOMER_NOT_FOUND
valid_customer_ids = customers.select(F.col("customer_id").alias("cid"))

orders_checked = orders_dedup.join(
    valid_customer_ids, orders_dedup.customer_id == valid_customer_ids.cid, "left"
).withColumn(
    "error_reason",
    F.when(F.col("amount").isNull() | (F.col("amount") <= 0), "INVALID_AMOUNT")
     .when(F.col("order_date_parsed").isNull(), "INVALID_DATE")
     .when(F.col("cid").isNull(), "CUSTOMER_NOT_FOUND")
     .otherwise(None)
)

valid_orders_stage1 = orders_checked.filter(F.col("error_reason").isNull()) \
    .drop("error_reason", "cid", "updated_at_parsed")
invalid_orders = orders_checked.filter(F.col("error_reason").isNotNull()) \
    .drop("cid", "updated_at_parsed")

n_valid1 = valid_orders_stage1.count()
n_invalid = invalid_orders.count()
print(f"\nvalid_orders (sau validate, trước join transform): {n_valid1} dòng")
print(f"invalid_orders: {n_invalid} dòng | Tổng = {n_valid1 + n_invalid} (kỳ vọng = {n_after_dedup})")

print("\nPhân bố lỗi trong invalid_orders:")
invalid_orders.groupBy("error_reason").count().orderBy(F.desc("count")).show()


# ============================================================================
# YÊU CẦU 3 — Join + Transform
# ============================================================================
print("=" * 70)
print("YÊU CẦU 3: Join + Transform")
print("=" * 70)

# left join orders với customers để lấy customer_name/customer_type
orders_joined = valid_orders_stage1.join(customers, on="customer_id", how="left")

n_unmapped = orders_joined.filter(F.col("customer_name").isNull()).count()
print(f"Số order (trong valid_orders) không mapping được customer sau left join: {n_unmapped} "
      f"(kỳ vọng = 0, vì CUSTOMER_NOT_FOUND đã bị lọc sang invalid_orders ở bước trước)")

# order_level theo amount — NGƯỠNG TỰ CHỌN (giả định), cùng mức với các lab trước cho nhất quán:
# HIGH >= 5,000,000 | MEDIUM >= 1,000,000 | LOW: còn lại
valid_orders = orders_joined.withColumn(
    "order_level",
    F.when(F.col("amount") >= 5_000_000, "HIGH")
     .when(F.col("amount") >= 1_000_000, "MEDIUM")
     .otherwise("LOW")
).withColumn("order_date", F.col("order_date_parsed")).drop("order_date_parsed")

print("\nPhân bố order_level:")
valid_orders.groupBy("order_level").count().orderBy(F.desc("count")).show()

n_valid_final = valid_orders.count()
print(f"valid_orders sau join+transform: {n_valid_final} dòng (không đổi so với bước validate: {n_valid1})")


# ============================================================================
# YÊU CẦU 4 — Aggregate: province_report
# ============================================================================
print("=" * 70)
print("YÊU CẦU 4: Aggregate — province_report")
print("=" * 70)

# GIẢ ĐỊNH: success = COMPLETED, failed = CANCELLED (2 trạng thái "kết thúc" rõ ràng;
# PENDING/SHIPPING coi là đang xử lý, không tính vào success/failed)
province_report = valid_orders.groupBy("province").agg(
    F.count("*").alias("total_orders"),
    F.countDistinct("customer_id").alias("total_customers"),
    F.sum("amount").alias("total_amount"),
    F.avg("amount").alias("avg_amount"),
    F.count(F.when(F.col("status") == "COMPLETED", True)).alias("success_orders"),
    F.count(F.when(F.col("status") == "CANCELLED", True)).alias("failed_orders"),
).orderBy("province")

province_report.show(truncate=False)


# ============================================================================
# YÊU CẦU 5 — Write + Check
# ============================================================================
print("=" * 70)
print("YÊU CẦU 5: Write + Check")
print("=" * 70)

out_valid = os.path.join(OUT_ROOT, "valid_orders")
out_valid_part = os.path.join(OUT_ROOT, "valid_orders_partitioned")
out_invalid = os.path.join(OUT_ROOT, "invalid_orders")
out_report = os.path.join(OUT_ROOT, "province_report")

for p in [out_valid, out_valid_part, out_invalid, out_report]:
    if os.path.exists(p):
        shutil.rmtree(p)

valid_orders.write.mode("overwrite").parquet(out_valid)
valid_orders.write.mode("overwrite").partitionBy("province").parquet(out_valid_part)
invalid_orders.write.mode("overwrite").parquet(out_invalid)
province_report.write.mode("overwrite").parquet(out_report)

files_plain = count_part_files(out_valid)
files_part = count_part_files(out_valid_part)
print(f"valid_orders (bình thường): {len(files_plain)} file part-*")
print(f"valid_orders_partitioned (partitionBy province): {len(files_part)} file part-*")
print("\nCấu trúc folder valid_orders_partitioned:")
print(show_folder_tree(out_valid_part))

# --- Đọc lại & kiểm tra ---
print("\n--- Kiểm tra sau khi đọc lại ---")

read_back = spark.read.parquet(out_valid)
count_back = read_back.count()
print(f"Count: ghi {n_valid_final} -> đọc lại {count_back} -> {'KHỚP' if count_back == n_valid_final else 'LỆCH'}")

print("Schema đọc lại:")
read_back.printSchema()

sum_before = valid_orders.agg(F.sum("amount")).collect()[0][0]
sum_after = read_back.agg(F.sum("amount")).collect()[0][0]
print(f"Tổng amount: trước ghi = {sum_before:.2f} | sau đọc lại = {sum_after:.2f} "
      f"-> {'KHỚP' if round(sum_before, 2) == round(sum_after, 2) else 'LỆCH'}")

report_back = spark.read.parquet(out_report)
print(f"\nprovince_report đọc lại: {report_back.count()} dòng (kỳ vọng = {province_report.count()})")

spark.stop()
print("\nHoàn tất toàn bộ pipeline.")
