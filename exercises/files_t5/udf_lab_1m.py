"""
Bài lab: UDF, Pandas UDF, UDTF trong PySpark vs built-in function (dữ liệu ~1 triệu dòng)
==========================================================================================
Chạy: python3 udf_lab_1m.py
Yêu cầu: pyspark, pandas, pyarrow đã cài. Có customers_1m.csv cùng thư mục.

So với bản 60 dòng: dùng .write (full-pass, ghi ra file) thay vì .show(10) để ĐO ĐÚNG
thời gian xử lý TOÀN BỘ 1 triệu dòng — show(10) chỉ tính vừa đủ 10 dòng đầu (do Spark
tối ưu LIMIT), không phản ánh đúng hiệu năng khi dữ liệu lớn.
"""

import os
import time
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.functions import udtf

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(BASE_DIR, "customers_1m.csv")
OUT_ROOT = os.path.join(BASE_DIR, "output_1m")


def full_pass_time(df, path):
    """Ghi ra Parquet (full-pass toàn bộ dữ liệu) và đo thời gian thực tế."""
    if os.path.exists(path):
        shutil.rmtree(path)
    t0 = time.time()
    df.write.mode("overwrite").parquet(path)
    return time.time() - t0

spark = (
    SparkSession.builder
    .appName("UdfLab")
    .master("local[4]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.option("header", True).option("inferSchema", True).csv(INPUT_CSV)
df.cache()
n_rows = df.count()
print("=" * 70)
print(f"Dữ liệu gốc: {n_rows:,} dòng. 5 dòng đầu:")
print("=" * 70)
df.show(5, truncate=False)


# ============================================================================
# YÊU CẦU 1 — Python UDF (row-by-row)
# ============================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 1: Python UDF")
print("=" * 70)


def clean_name(name: str) -> str:
    """Trim khoảng trắng thừa (kể cả khoảng trắng lặp ở giữa) + chuẩn hóa Title Case."""
    if name is None:
        return None
    # gộp nhiều khoảng trắng liên tiếp thành 1, rồi trim 2 đầu
    parts = name.split()
    return " ".join(parts).title()


def segment_customer(amount) -> str:
    """Phân loại khách hàng theo amount."""
    if amount is None:
        return None
    if amount >= 5_000_000:
        return "VIP"
    elif amount >= 1_000_000:
        return "STANDARD"
    else:
        return "BASIC"


clean_name_udf = udf(clean_name, StringType())
segment_udf = udf(segment_customer, StringType())

df_udf = df.withColumn("customer_name_clean", clean_name_udf(F.col("customer_name"))) \
           .withColumn("customer_segment", segment_udf(F.col("amount")))
df_udf.show(5, truncate=False)
t_udf = full_pass_time(df_udf, os.path.join(OUT_ROOT, "_bench_udf"))
print(f"(Python UDF) thời gian xử lý TOÀN BỘ {n_rows:,} dòng (ghi Parquet): {t_udf:.2f}s")


# ============================================================================
# YÊU CẦU 2 — Built-in Spark function (trim, upper, when/otherwise)
# ============================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 2: Built-in Spark function")
print("=" * 70)

df_builtin = df.withColumn(
        "customer_name_clean",
        F.initcap(F.trim(F.regexp_replace(F.col("customer_name"), r"\s+", " ")))
    ).withColumn(
        "customer_segment",
        F.when(F.col("amount") >= 5_000_000, "VIP")
         .when(F.col("amount") >= 1_000_000, "STANDARD")
         .otherwise("BASIC")
    )
df_builtin.show(5, truncate=False)
t_builtin = full_pass_time(df_builtin, os.path.join(OUT_ROOT, "_bench_builtin"))
print(f"(Built-in) thời gian xử lý TOÀN BỘ {n_rows:,} dòng (ghi Parquet): {t_builtin:.2f}s")
print(f"\n>>> UDF chậm hơn built-in khoảng {t_udf / t_builtin:.2f} lần trên {n_rows:,} dòng")

# Kiểm chứng 2 cách cho kết quả giống nhau (dùng sample để kiểm tra nhanh, không exceptAll toàn bộ 1M dòng)
diff_count = df_udf.select("customer_id", "customer_name_clean", "customer_segment") \
    .exceptAll(
        df_builtin.select("customer_id", "customer_name_clean", "customer_segment")
    ).count()
print(f"\nSố dòng KHÁC NHAU giữa kết quả UDF và built-in (trên toàn bộ {n_rows:,} dòng): {diff_count} (kỳ vọng = 0)")

# Xem query plan để thấy built-in được tối ưu (Catalyst) còn UDF là "black box"
print("\n--- Physical plan (Python UDF) ---")
df_udf.select("customer_segment").explain()
print("--- Physical plan (built-in) ---")
df_builtin.select("customer_segment").explain()


# ============================================================================
# YÊU CẦU 3 — Pandas UDF (vectorized, xử lý theo batch/Series)
# ============================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 3: Pandas UDF")
print("=" * 70)

import pandas as pd


@pandas_udf(DoubleType())
def amount_with_surcharge(amount: pd.Series) -> pd.Series:
    """Cộng thêm phụ phí 5% lên amount — xử lý theo VECTOR (cả Series), không phải từng dòng."""
    return amount * 1.05


df_pandas_udf = df.withColumn("amount_with_fee", amount_with_surcharge(F.col("amount")))
df_pandas_udf.select("customer_id", "amount", "amount_with_fee").show(5, truncate=False)
t_pudf = full_pass_time(df_pandas_udf, os.path.join(OUT_ROOT, "_bench_pandas_udf"))
print(f"(Pandas UDF) thời gian xử lý TOÀN BỘ {n_rows:,} dòng (ghi Parquet): {t_pudf:.2f}s")
print(f"\n>>> So sánh 3 cách trên {n_rows:,} dòng (chỉ tính riêng cột amount/segment):")
print(f"    Python UDF : {t_udf:.2f}s")
print(f"    Built-in   : {t_builtin:.2f}s")
print(f"    Pandas UDF : {t_pudf:.2f}s (cho phép tính cộng phụ phí, khác logic segment nên chỉ tham khảo tương đối)")


# ============================================================================
# YÊU CẦU 4 — UDTF (User Defined Table Function): tách 1 dòng -> nhiều dòng
# ============================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 4: UDTF — tách cột tags thành nhiều dòng")
print("=" * 70)

tags_data = [(1, "spark,python,etl"), (2, "sql,airflow"), (3, "spark,scala,kafka,delta")]
df_tags = spark.createDataFrame(tags_data, ["customer_id", "tags"])
print("DataFrame gốc:")
df_tags.show(truncate=False)


@udtf(returnType="tag: string")
class SplitTags:
    """UDTF: nhận 1 chuỗi tags, trả ra NHIỀU DÒNG, mỗi dòng 1 tag."""
    def eval(self, tags: str):
        if tags:
            for t in tags.split(","):
                yield (t.strip(),)


# Cách 1: gọi UDTF trực tiếp trên biểu thức (lateral-style qua LATERAL JOIN trong SQL)
spark.udtf.register("split_tags", SplitTags)
df_tags.createOrReplaceTempView("customer_tags_raw")

customer_tags = spark.sql("""
    SELECT customer_id, tag
    FROM customer_tags_raw, LATERAL split_tags(tags)
    ORDER BY customer_id, tag
""")
print("Kết quả sau UDTF (1 dòng -> nhiều dòng):")
customer_tags.show(truncate=False)

# So sánh nhanh với cách dùng built-in explode(split(...)) - không cần UDTF cho case đơn giản này
customer_tags_builtin = df_tags.select(
    "customer_id",
    F.explode(F.split(F.col("tags"), ",")).alias("tag")
)
print("Kết quả tương đương dùng built-in explode(split(...)):")
customer_tags_builtin.show(truncate=False)


# ============================================================================
# GHI OUTPUT
# ============================================================================
print("\n" + "=" * 70)
print("GHI OUTPUT")
print("=" * 70)

customers_processed = df_builtin.withColumn(
    "amount_with_fee", amount_with_surcharge(F.col("amount"))
)

out_customers = os.path.join(OUT_ROOT, "customers_processed")
out_tags = os.path.join(OUT_ROOT, "customer_tags")

t0 = time.time()
customers_processed.write.mode("overwrite").parquet(out_customers)
t_final = time.time() - t0
customer_tags.write.mode("overwrite").option("header", True).csv(out_tags)

n_files, _ = 0, None
part_files = [f for f in os.listdir(out_customers) if f.startswith("part-")]
print(f"Đã ghi: {out_customers} ({t_final:.2f}s, {len(part_files)} file part-*)")
print(f"Đã ghi: {out_tags}")

customers_processed.select(
    "customer_id", "customer_name", "customer_name_clean",
    "amount", "customer_segment", "amount_with_fee"
).show(5, truncate=False)

spark.stop()
print("\nHoàn tất.")
