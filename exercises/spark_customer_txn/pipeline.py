"""
pipeline.py
-----------
Flow: raw data -> validate -> deduplicate -> join -> window -> aggregate -> partitioned output

Chạy:
    python3 pipeline.py
"""
import shutil
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUT_DIR = os.path.join(BASE_DIR, "output")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Dọn output cũ để chạy lại nhiều lần cho sạch (idempotent cho mục đích luyện tập)
for d in [OUT_DIR]:
    if os.path.exists(d):
        shutil.rmtree(d)
    os.makedirs(d)
os.makedirs(LOG_DIR, exist_ok=True)

plan_log_path = os.path.join(LOG_DIR, "execution_plans.txt")
plan_log = open(plan_log_path, "w", encoding="utf-8")


def log_plan(title, df, extended=False):
    """Ghi explain() ra file log kèm tiêu đề, để soi join/shuffle/sort."""
    plan_log.write("=" * 90 + "\n")
    plan_log.write(f"PLAN: {title}\n")
    plan_log.write("=" * 90 + "\n")
    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(True if extended else "formatted")
    plan_log.write(buf.getvalue())
    plan_log.write("\n\n")


spark = (
    SparkSession.builder.appName("customer-transaction-pipeline")
    # 4 shuffle partition cho dễ đọc plan/log với dữ liệu bé; production sẽ tune theo cluster
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# BƯỚC 1: Đọc dữ liệu với schema khai báo sẵn (KHÔNG dùng inferSchema)
# ---------------------------------------------------------------------------
customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("province", StringType(), True),
    StructField("created_at", TimestampType(), True),
])

transactions_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("transaction_time", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])

customers_raw = (
    spark.read.option("header", True)
    .schema(customers_schema)
    .csv(os.path.join(DATA_DIR, "customers.csv"))
)

# customer_id rỗng trong CSV có thể bị đọc thành chuỗi rỗng "" chứ không phải NULL
# -> chuẩn hoá về NULL ngay từ đầu để các bước check isNull() phía sau hoạt động đúng
transactions_raw = (
    spark.read.option("header", True)
    .schema(transactions_schema)
    .csv(os.path.join(DATA_DIR, "transactions.csv"))
    .withColumn(
        "customer_id",
        F.when(F.trim(F.col("customer_id")) == "", None).otherwise(F.col("customer_id")),
    )
)

print(f"[1] Đọc xong: customers_raw={customers_raw.count()} rows, "
      f"transactions_raw={transactions_raw.count()} rows")

# ---------------------------------------------------------------------------
# BƯỚC 2: Validate & tách record lỗi (thiếu customer_id, amount <= 0)
# ---------------------------------------------------------------------------
is_missing_customer = F.col("customer_id").isNull()
is_bad_amount = F.col("amount") <= 0

transactions_flagged = transactions_raw.withColumn(
    "error_reason",
    F.when(is_missing_customer & is_bad_amount, F.lit("MISSING_CUSTOMER_ID,NON_POSITIVE_AMOUNT"))
     .when(is_missing_customer, F.lit("MISSING_CUSTOMER_ID"))
     .when(is_bad_amount, F.lit("NON_POSITIVE_AMOUNT"))
     .otherwise(F.lit(None)),
)

txn_invalid = transactions_flagged.filter(F.col("error_reason").isNotNull())
txn_valid_raw = transactions_flagged.filter(F.col("error_reason").isNull()).drop("error_reason")

print(f"[2] Validate: invalid={txn_invalid.count()} rows, "
      f"valid_raw (trước dedup)={txn_valid_raw.count()} rows")

# ---------------------------------------------------------------------------
# BƯỚC 3: Dedup transaction_id trùng -> dùng Window Function, giữ updated_at mới nhất
# ---------------------------------------------------------------------------
dedup_window = Window.partitionBy("transaction_id").orderBy(F.col("updated_at").desc())

txn_deduped = (
    txn_valid_raw
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

n_before_dedup = txn_valid_raw.count()
n_after_dedup = txn_deduped.count()
print(f"[3] Dedup theo transaction_id (giữ updated_at mới nhất): "
      f"{n_before_dedup} -> {n_after_dedup} rows "
      f"(loại {n_before_dedup - n_after_dedup} bản ghi cũ hơn)")

log_plan("STEP 3 - Deduplicate với Window row_number()", txn_deduped)

# ---------------------------------------------------------------------------
# BƯỚC 4: Left join transactions (đã dedup) với customers
# ---------------------------------------------------------------------------
joined = txn_deduped.join(customers_raw, on="customer_id", how="left")

log_plan("STEP 4 - Left Join transactions x customers", joined)

# Tách các transaction không mapping được customer (join left nhưng customer_name null
# nghĩa là customer_id có tồn tại trên transaction nhưng KHÔNG khớp với customers.csv)
txn_unmapped = joined.filter(F.col("customer_name").isNull())
txn_mapped = joined.filter(F.col("customer_name").isNotNull())

print(f"[4] Join: mapped={txn_mapped.count()} rows, unmapped={txn_unmapped.count()} rows")

# ---------------------------------------------------------------------------
# BƯỚC 5: Với mỗi customer, dùng Window để tìm transaction gần nhất
#          (gần nhất theo transaction_time)
# ---------------------------------------------------------------------------
latest_txn_window = Window.partitionBy("customer_id").orderBy(F.col("transaction_time").desc())

txn_mapped_with_latest_flag = txn_mapped.withColumn(
    "is_latest_txn", F.row_number().over(latest_txn_window) == 1
)

customer_latest_txn = (
    txn_mapped_with_latest_flag.filter(F.col("is_latest_txn"))
    .select(
        "customer_id", "customer_name", "province",
        F.col("transaction_id").alias("latest_transaction_id"),
        F.col("transaction_time").alias("latest_transaction_time"),
        F.col("amount").alias("latest_amount"),
        F.col("status").alias("latest_status"),
    )
)

log_plan("STEP 5 - Window tìm transaction gần nhất theo customer", txn_mapped_with_latest_flag)

# ---------------------------------------------------------------------------
# BƯỚC 6: Aggregate theo customer: tổng số txn, tổng amount, số txn SUCCESS
# ---------------------------------------------------------------------------
customer_agg = (
    txn_mapped.groupBy("customer_id", "customer_name", "province")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_transactions"),
    )
)

log_plan("STEP 6 - GroupBy aggregate theo customer", customer_agg)

# Gộp thêm thông tin transaction gần nhất vào bảng tổng hợp customer (tiện cho output cuối)
customer_summary = customer_agg.join(
    customer_latest_txn.select(
        "customer_id", "latest_transaction_id", "latest_transaction_time",
        "latest_amount", "latest_status",
    ),
    on="customer_id",
    how="left",
)

print("[6] customer_summary (10 dòng đầu):")
customer_summary.orderBy(F.col("total_amount").desc()).show(10, truncate=False)

# ---------------------------------------------------------------------------
# BƯỚC 7: Top 3 customer có tổng amount cao nhất theo từng province
# ---------------------------------------------------------------------------
province_rank_window = Window.partitionBy("province").orderBy(F.col("total_amount").desc())

top3_by_province = (
    customer_summary
    .withColumn("rank_in_province", F.rank().over(province_rank_window))
    .filter(F.col("rank_in_province") <= 3)
    .orderBy("province", "rank_in_province")
)

log_plan("STEP 7 - Window rank() top3 theo province", top3_by_province)

print("[7] Top 3 customer theo tổng amount / province:")
top3_by_province.select(
    "province", "rank_in_province", "customer_id", "customer_name", "total_amount"
).show(50, truncate=False)

# ---------------------------------------------------------------------------
# BƯỚC 8: Ghi dữ liệu hợp lệ ra Parquet, partition theo province
# ---------------------------------------------------------------------------
valid_output_path = os.path.join(OUT_DIR, "valid_transactions_parquet")
txn_mapped.write.mode("overwrite").partitionBy("province").parquet(valid_output_path)
print(f"[8] Đã ghi valid transactions -> {valid_output_path} (partitionBy province)")

customer_summary_path = os.path.join(OUT_DIR, "customer_summary_parquet")
customer_summary.write.mode("overwrite").partitionBy("province").parquet(customer_summary_path)
print(f"[8] Đã ghi customer_summary -> {customer_summary_path} (partitionBy province)")

top3_path = os.path.join(OUT_DIR, "top3_by_province_parquet")
top3_by_province.write.mode("overwrite").partitionBy("province").parquet(top3_path)
print(f"[8] Đã ghi top3_by_province -> {top3_path} (partitionBy province)")

# ---------------------------------------------------------------------------
# BƯỚC 9: Ghi dữ liệu lỗi/unmapped ra output riêng (không partition, dataset nhỏ + cần
#          soi toàn bộ lý do lỗi dễ dàng, không cần partition theo province vì nhiều
#          record lỗi không join được để lấy province)
# ---------------------------------------------------------------------------
invalid_path = os.path.join(OUT_DIR, "invalid_transactions_parquet")
txn_invalid.write.mode("overwrite").parquet(invalid_path)
print(f"[9] Đã ghi invalid transactions -> {invalid_path}")

unmapped_path = os.path.join(OUT_DIR, "unmapped_transactions_parquet")
txn_unmapped.drop("customer_name", "province", "created_at").write.mode("overwrite").parquet(unmapped_path)
print(f"[9] Đã ghi unmapped transactions -> {unmapped_path}")

# ---------------------------------------------------------------------------
# BƯỚC 10: Đọc lại output và kiểm tra count (round-trip validation)
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("[10] KIỂM TRA LẠI OUTPUT SAU KHI GHI (round-trip check)")
print("=" * 60)

checks = [
    ("valid_transactions_parquet", valid_output_path, txn_mapped.count()),
    ("customer_summary_parquet", customer_summary_path, customer_summary.count()),
    ("top3_by_province_parquet", top3_path, top3_by_province.count()),
    ("invalid_transactions_parquet", invalid_path, txn_invalid.count()),
    ("unmapped_transactions_parquet", unmapped_path, txn_unmapped.count()),
]

all_ok = True
for name, path, expected_count in checks:
    reread = spark.read.parquet(path)
    actual_count = reread.count()
    status_ok = actual_count == expected_count
    all_ok = all_ok and status_ok
    print(f"  - {name:32s} expected={expected_count:4d}  actual_after_reread={actual_count:4d}  "
          f"{'OK' if status_ok else 'MISMATCH !!'}")

print(f"\n[10] Tổng kết round-trip check: {'TẤT CẢ KHỚP' if all_ok else 'CÓ SAI LỆCH'}")

# ---------------------------------------------------------------------------
# Tổng kết reconciliation: input = valid(mapped) + invalid + unmapped
# ---------------------------------------------------------------------------
total_raw = transactions_raw.count()
total_after_dedup_space = n_after_dedup + (n_before_dedup - n_after_dedup)  # = n_before_dedup
recon_valid_side = txn_mapped.count() + txn_unmapped.count()  # = n_after_dedup (mapped+unmapped)
print("\n" + "=" * 60)
print("[RECON] Đối chiếu số dòng toàn pipeline")
print("=" * 60)
print(f"  transactions_raw (đọc từ CSV)         : {total_raw}")
print(f"  - invalid (missing_cid / amount<=0)   : {txn_invalid.count()}")
print(f"  - loại bỏ do trùng transaction_id (cũ): {n_before_dedup - n_after_dedup}")
print(f"  = còn lại sau validate+dedup           : {n_after_dedup}")
print(f"      -> mapped (join thành công)        : {txn_mapped.count()}")
print(f"      -> unmapped (customer_id lạ)       : {txn_unmapped.count()}")
recon_ok = total_raw == (txn_invalid.count() + (n_before_dedup - n_after_dedup) + txn_mapped.count() + txn_unmapped.count())
print(f"  Reconciliation OK? {recon_ok}")

plan_log.close()
print(f"\nExecution plan đã ghi vào: {plan_log_path}")

spark.stop()
