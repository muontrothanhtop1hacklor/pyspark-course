"""
00 - Read & Write Basics (PySpark)
==================================
Script thuc hanh doc va ghi du lieu co ban voi CSV va JSON trong PySpark.
Hieu ro cach Spark xu ly du lieu phan tan va tao ra cac file part-*.

Chay:
    python read_write_demo.py
"""

import os
import sys
import shutil

# Dam bao PySpark dung dung Python executable cua moi truong hien tai
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "employee.csv")
JSON_PATH = os.path.join(BASE_DIR, "employees.json")
OUT_DIR = os.path.join(BASE_DIR, "output")


def main():
    print("=" * 70)
    print("00 - READ & WRITE BASICS IN PYSPARK")
    print("=" * 70)

    # 1. Khoi tao SparkSession
    spark = (
        SparkSession.builder
        .appName("00-read-write-basics")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 2. Doc file CSV
    print("\n--- 1. Doc employee.csv ---")
    df_csv = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(CSV_PATH)
    )
    print("Schema cua file CSV:")
    df_csv.printSchema()
    print("Du lieu CSV (toan bo):")
    df_csv.show(truncate=False)

    # 3. Doc file JSON
    print("\n--- 2. Doc employees.json ---")
    # File JSON la 1 mang cac object JSON (multiline)
    df_json = (
        spark.read
        .option("multiline", "true")
        .json(JSON_PATH)
    )
    print("Schema cua file JSON:")
    df_json.printSchema()
    print("Du lieu JSON (toan bo):")
    df_json.show(truncate=False)

    # 4. Ghi du lieu ra thu muc output
    if os.path.exists(OUT_DIR):
        shutil.rmtree(OUT_DIR)
    os.makedirs(OUT_DIR, exist_ok=True)

    print("\n--- 3. Ghi du lieu ra output/ ---")
    
    # 4.1 Ghi Parquet
    parquet_out = os.path.join(OUT_DIR, "employees_parquet")
    print(f"Ghi Parquet vao: {parquet_out}")
    df_csv.write.mode("overwrite").parquet(parquet_out)

    # 4.2 Ghi CSV
    csv_out = os.path.join(OUT_DIR, "employees_csv")
    print(f"Ghi CSV vao: {csv_out}")
    df_csv.write.mode("overwrite").option("header", "true").csv(csv_out)

    # 5. Kiem tra thu muc output va giai thich part-file
    print("\n--- 4. Cau truc thu muc Spark sinh ra ---")
    for sub in ["employees_parquet", "employees_csv"]:
        target = os.path.join(OUT_DIR, sub)
        print(f"\nNoi dung thu muc '{sub}':")
        for f in os.listdir(target):
            size = os.path.getsize(os.path.join(target, f))
            print(f"  - {f} ({size} bytes)")

    print("\n[NOTE] Spark khong bao gio ghi 1 file don le, ma ghi thanh mot THU MUC gom:")
    print("  + _SUCCESS: file danh dau job da hoan tat thanh cong 100%.")
    print("  + part-*: file chua du lieu thuc te (moi partition tao ra 1 file).")
    print("  + .*crc: checksum dam bao toan ven du lieu tren dia.")

    spark.stop()
    print("\nHoan thanh bai 00!")


if __name__ == "__main__":
    main()
