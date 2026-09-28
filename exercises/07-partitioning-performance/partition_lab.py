"""
Bài lab: Partition trong Spark - repartition vs coalesce vs partitionBy
========================================================================
Chạy: python3 partition_lab.py
Yêu cầu: pyspark đã cài, có orders.csv cùng thư mục.
"""

import os
import shutil
import glob
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(BASE_DIR, "orders.csv")
OUT_ROOT = os.path.join(BASE_DIR, "output")

spark = (
    SparkSession.builder
    .appName("PartitionLab")
    .master("local[4]")               # giả lập 4 core -> để thấy rõ ảnh hưởng của số partition
    .config("spark.sql.shuffle.partitions", "8")  # cố định để dễ so sánh khi groupBy
    .config("spark.sql.adaptive.enabled", "false")  # tắt AQE để số partition sau shuffle KHÔNG bị Spark tự coalesce lại
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def count_part_files(path):
    """Đếm số file part-* (data files) thực sự được Spark ghi ra, bỏ qua _SUCCESS, CRC..."""
    files = glob.glob(os.path.join(path, "**", "part-*"), recursive=True)
    files = [f for f in files if not f.endswith(".crc")]
    return len(files), files


def show_folder_tree(path, max_lines=25):
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


def write_raw_direct(df, out_subdir):
    """Ghi thẳng df (KHÔNG qua groupBy/shuffle) để thấy trực tiếp: số partition = số file output."""
    out_path = os.path.join(OUT_ROOT, out_subdir)
    if os.path.exists(out_path):
        shutil.rmtree(out_path)
    df.write.mode("overwrite").parquet(out_path)
    n_files, _ = count_part_files(out_path)
    return n_files


def run_case(df, label, out_subdir, use_province_partition_by=False):
    """Thực hiện: kiểm tra số partition -> groupBy -> ghi parquet -> đếm file -> trả về thông tin."""
    n_part = df.rdd.getNumPartitions()

    # (a) ghi trực tiếp KHÔNG qua groupBy -> quan sát ảnh hưởng TRỰC TIẾP của repartition/coalesce
    n_files_direct = write_raw_direct(df, out_subdir + "_direct_no_groupby")

    agg = (
        df.groupBy("province")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
        )
    )

    out_path = os.path.join(OUT_ROOT, out_subdir)
    if os.path.exists(out_path):
        shutil.rmtree(out_path)

    t0 = time.time()
    writer = agg.write.mode("overwrite")
    if use_province_partition_by:
        writer = writer.partitionBy("province")
    writer.parquet(out_path)
    elapsed = time.time() - t0

    n_files, files = count_part_files(out_path)

    return {
        "label": label,
        "num_partitions_before_write": n_part,
        "num_files_direct_write_no_groupby": n_files_direct,
        "num_output_files_after_groupby": n_files,
        "elapsed_sec": round(elapsed, 2),
        "out_path": out_path,
        "sample_files": [os.path.relpath(f, out_path) for f in files[:6]],
    }


results = []

print("=" * 70)
print("BƯỚC 1: Đọc dữ liệu bằng PySpark")
print("=" * 70)

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_CSV)
)

print("Schema:")
df_raw.printSchema()
print("Số dòng:", df_raw.count())

n0 = df_raw.rdd.getNumPartitions()
print(f"\n>>> Số partition BAN ĐẦU (sau khi đọc CSV): {n0}")
results.append({"label": "ORIGINAL (sau khi đọc CSV)", "num_partitions_before_write": n0,
                 "num_files_direct_write_no_groupby": None, "num_output_files_after_groupby": None,
                 "elapsed_sec": None, "out_path": None, "sample_files": []})

print("\n" + "=" * 70)
print("BƯỚC 2: Thử các trường hợp repartition / coalesce")
print("=" * 70)

# --- repartition(2) ---
df2 = df_raw.repartition(2)
r = run_case(df2, "repartition(2)", "case_repartition_2")
results.append(r)
print(f"[repartition(2)] partitions={r['num_partitions_before_write']} | file ghi trực tiếp (ko groupBy)={r['num_files_direct_write_no_groupby']} | file sau groupBy={r['num_output_files_after_groupby']} ({r['elapsed_sec']}s)")

# --- repartition(4) ---
df4 = df_raw.repartition(4)
r = run_case(df4, "repartition(4)", "case_repartition_4")
results.append(r)
print(f"[repartition(4)] partitions={r['num_partitions_before_write']} | file ghi trực tiếp (ko groupBy)={r['num_files_direct_write_no_groupby']} | file sau groupBy={r['num_output_files_after_groupby']} ({r['elapsed_sec']}s)")

# --- repartition(8) ---
df8 = df_raw.repartition(8)
r = run_case(df8, "repartition(8)", "case_repartition_8")
results.append(r)
print(f"[repartition(8)] partitions={r['num_partitions_before_write']} | file ghi trực tiếp (ko groupBy)={r['num_files_direct_write_no_groupby']} | file sau groupBy={r['num_output_files_after_groupby']} ({r['elapsed_sec']}s)")

# --- repartition("province") ---
df_prov = df_raw.repartition("province")
r = run_case(df_prov, 'repartition("province")', "case_repartition_province")
results.append(r)
print(f'[repartition("province")] partitions={r["num_partitions_before_write"]} | file ghi trực tiếp (ko groupBy)={r["num_files_direct_write_no_groupby"]} | file sau groupBy={r["num_output_files_after_groupby"]} ({r["elapsed_sec"]}s)')

# --- coalesce(2) ---
# Lưu ý: coalesce phải áp dụng trên df đã có nhiều partition (df_raw ban đầu)
# để thấy rõ hiệu ứng GIẢM partition không qua shuffle.
df_coalesce2 = df_raw.coalesce(2)
r = run_case(df_coalesce2, "coalesce(2)", "case_coalesce_2")
results.append(r)
print(f"[coalesce(2)] partitions={r['num_partitions_before_write']} | file ghi trực tiếp (ko groupBy)={r['num_files_direct_write_no_groupby']} | file sau groupBy={r['num_output_files_after_groupby']} ({r['elapsed_sec']}s)")

print("\n" + "=" * 70)
print("BƯỚC 3: So sánh ghi KHÔNG dùng partitionBy vs CÓ dùng partitionBy(\"province\")")
print("=" * 70)

df_for_write = df_raw.repartition(4)
agg_for_write = (
    df_for_write.groupBy("province")
    .agg(F.count("*").alias("total_orders"), F.sum("amount").alias("total_amount"),
         F.avg("amount").alias("avg_amount"))
)

# Ghi KHÔNG partitionBy
out_no_pb = os.path.join(OUT_ROOT, "case_no_partitionBy")
if os.path.exists(out_no_pb):
    shutil.rmtree(out_no_pb)
agg_for_write.write.mode("overwrite").parquet(out_no_pb)
n_files_no_pb, _ = count_part_files(out_no_pb)

# Ghi CÓ partitionBy("province")
out_with_pb = os.path.join(OUT_ROOT, "case_with_partitionBy")
if os.path.exists(out_with_pb):
    shutil.rmtree(out_with_pb)
agg_for_write.write.mode("overwrite").partitionBy("province").parquet(out_with_pb)
n_files_with_pb, _ = count_part_files(out_with_pb)

print(f"KHÔNG partitionBy -> {n_files_no_pb} file part-*, cấu trúc phẳng")
print(f"CÓ partitionBy('province') -> {n_files_with_pb} file part-*, chia theo thư mục con province=<value>/")

print("\nCấu trúc folder KHÔNG partitionBy:")
print(show_folder_tree(out_no_pb))
print("\nCấu trúc folder CÓ partitionBy('province'):")
print(show_folder_tree(out_with_pb))

print("\n" + "=" * 70)
print("PHẦN NÂNG CAO: repartition với số partition lớn hơn bình thường")
print("=" * 70)

# local[4] nghĩa là chỉ có 4 core thực thi song song
# thử ép số partition lên rất cao so với dữ liệu (200k dòng, ~10MB)
df_high = df_raw.repartition(200)
r_high = run_case(df_high, "repartition(200) [OVER-PARTITION]", "case_repartition_200")
results.append(r_high)
print(f"[repartition(200)] partitions={r_high['num_partitions_before_write']} | file ghi trực tiếp (ko groupBy)={r_high['num_files_direct_write_no_groupby']} | file sau groupBy={r_high['num_output_files_after_groupby']} ({r_high['elapsed_sec']}s)")

# tính kích thước trung bình mỗi file để minh hoạ "small files problem"
# dùng thư mục ghi TRỰC TIẾP (không qua groupby) để thấy rõ small-files với 200 partition
files_high = glob.glob(os.path.join(OUT_ROOT, "case_repartition_200_direct_no_groupby", "part-*"))
files_high = [f for f in files_high if not f.endswith(".crc")]
sizes = [os.path.getsize(f) for f in files_high]
avg_size_kb = (sum(sizes) / len(sizes) / 1024) if sizes else 0
total_size_kb = sum(sizes) / 1024

print(f"Số file thực tế: {len(files_high)}, tổng dung lượng: {total_size_kb:.1f} KB, "
      f"trung bình mỗi file: {avg_size_kb:.2f} KB/file")

print("\n" + "=" * 70)
print("TỔNG HỢP KẾT QUẢ")
print("=" * 70)
for r in results:
    print(r)

spark.stop()

# Ghi kết quả ra file text để dùng viết báo cáo markdown
with open(os.path.join(BASE_DIR, "results_summary.txt"), "w", encoding="utf-8") as f:
    for r in results:
        f.write(str(r) + "\n")
    f.write(f"\nno_partitionBy_files={n_files_no_pb}\n")
    f.write(f"with_partitionBy_files={n_files_with_pb}\n")
    f.write(f"tree_no_pb=\n{show_folder_tree(out_no_pb)}\n")
    f.write(f"tree_with_pb=\n{show_folder_tree(out_with_pb)}\n")
    f.write(f"over_partition_num_files={len(files_high)}\n")
    f.write(f"over_partition_avg_size_kb={avg_size_kb:.2f}\n")
    f.write(f"over_partition_total_size_kb={total_size_kb:.2f}\n")

print("\nĐã ghi results_summary.txt")
