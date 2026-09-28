"""
generate_streaming_data.py
--------------------------
Sinh 30 dong don hang mau cho bai thuc hanh Batch vs Streaming,
chia deu vao 3 file orders_1.csv, orders_2.csv, orders_3.csv (moi file 10 dong co header).

Chu dong tao cac truong hop:
  - Duplicate order_id xuyen file (1001 o file 1 & 2; 1002 o file 1 & 3 voi updated_at moi hon).
  - Amount invalid (<= 0, am, text 'N/A', null/empty).
  - Status viet hoa/thuong khong dong nhat (completed, Pending, Shipping...).
  - Order_date sai format (31/02/2025, 2025-13-40, rong).
"""

import os
import csv
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
BATCH_INPUT_DIR = os.path.join(DATA_DIR, "batch_input")
STREAM_INPUT_DIR = os.path.join(DATA_DIR, "stream_input")
STREAM_WRITE_DIR = os.path.join(DATA_DIR, "stream_input_write")
RAW_SOURCE_DIR = os.path.join(DATA_DIR, "source_files")

HEADER = ["order_id", "customer_id", "province", "amount", "status", "order_date", "updated_at"]

# 10 dong cho file 1: 7 valid, 3 invalid (amount=0, amount<0, date sai format)
ORDERS_1 = [
    ["1001", "C001", "HaNoi", "1000000.0", "SUCCESS", "2025-01-05", "2025-01-05 08:30:00"],
    ["1002", "C002", "HoChiMinh", "2500000.0", "completed", "2025-01-06", "2025-01-06 09:15:00"],
    ["1003", "C003", "DaNang", "1500000.0", "PENDING", "2025-01-07", "2025-01-07 10:00:00"],
    ["1004", "C004", "CanTho", "800000.0", "COMPLETED", "2025-01-08", "2025-01-08 11:20:00"],
    ["1005", "C001", "HaNoi", "0.0", "SUCCESS", "2025-01-09", "2025-01-09 14:00:00"],           # amount == 0
    ["1006", "C005", "HaiPhong", "1200000.0", "Shipping", "2025-01-10", "2025-01-10 15:30:00"],
    ["1007", "C002", "HoChiMinh", "-50000.0", "SUCCESS", "2025-01-11", "2025-01-11 16:45:00"],   # amount < 0
    ["1008", "C006", "DaNang", "3000000.0", "COMPLETED", "31/02/2025", "2025-01-12 17:00:00"],   # date sai format
    ["1009", "C007", "CanTho", "600000.0", "SUCCESS", "2025-01-13", "2025-01-13 18:10:00"],
    ["1010", "C003", "HaNoi", "2000000.0", "Pending", "2025-01-14", "2025-01-14 19:30:00"],
]

# 10 dong cho file 2: 7 valid, 3 invalid (amount text, date thang 13, amount rong)
# Co 1001 la duplicate cua file 1 nhung updated_at va amount moi hon
ORDERS_2 = [
    ["1001", "C001", "HaNoi", "1500000.0", "COMPLETED", "2025-01-15", "2025-01-15 10:00:00"],   # duplicate 1001
    ["1011", "C008", "HoChiMinh", "4000000.0", "SUCCESS", "2025-01-16", "2025-01-16 11:00:00"],
    ["1012", "C009", "DaNang", "N/A", "COMPLETED", "2025-01-17", "2025-01-17 12:30:00"],          # amount text
    ["1013", "C004", "CanTho", "1100000.0", "pending", "2025-01-18", "2025-01-18 13:45:00"],
    ["1014", "C010", "HaiPhong", "900000.0", "SUCCESS", "2025-01-19", "2025-01-19 14:15:00"],
    ["1015", "C005", "HaNoi", "1800000.0", "COMPLETED", "2025-13-40", "2025-01-20 15:00:00"],   # date khong ton tai
    ["1016", "C002", "HoChiMinh", "3200000.0", "Shipping", "2025-01-21", "2025-01-21 16:20:00"],
    ["1017", "C006", "DaNang", "2200000.0", "SUCCESS", "2025-01-22", "2025-01-22 17:35:00"],
    ["1018", "C007", "CanTho", "", "COMPLETED", "2025-01-23", "2025-01-23 18:40:00"],             # amount rong
    ["1019", "C001", "HaNoi", "2700000.0", "SUCCESS", "2025-01-24", "2025-01-24 19:50:00"],
]

# 10 dong cho file 3: 7 valid, 3 invalid (amount<0, date rong, amount=0)
# Co 1002 la duplicate cua file 1 nhung updated_at va amount moi hon
ORDERS_3 = [
    ["1002", "C002", "HoChiMinh", "2800000.0", "COMPLETED", "2025-01-25", "2025-01-25 09:00:00"], # duplicate 1002
    ["1020", "C003", "DaNang", "1700000.0", "SUCCESS", "2025-01-26", "2025-01-26 10:15:00"],
    ["1021", "C008", "CanTho", "500000.0", "pending", "2025-01-27", "2025-01-27 11:30:00"],
    ["1022", "C009", "HaiPhong", "-100000.0", "FAILED", "2025-01-28", "2025-01-28 12:45:00"],     # amount < 0
    ["1023", "C010", "HaNoi", "3500000.0", "COMPLETED", "", "2025-01-29 13:50:00"],               # date rong
    ["1024", "C004", "HoChiMinh", "1900000.0", "SUCCESS", "2025-01-30", "2025-01-30 14:20:00"],
    ["1025", "C005", "DaNang", "0.0", "CANCELLED", "2025-01-31", "2025-01-31 15:10:00"],           # amount == 0
    ["1026", "C006", "CanTho", "1300000.0", "Shipping", "2025-02-01", "2025-02-01 16:00:00"],
    ["1027", "C007", "HaiPhong", "2100000.0", "SUCCESS", "2025-02-02", "2025-02-02 17:15:00"],
    ["1028", "C001", "HaNoi", "4500000.0", "SUCCESS", "2025-02-03", "2025-02-03 18:30:00"],
]


def write_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        writer.writerows(rows)


def main():
    print("=" * 70)
    print("KHOI TAO DU LIEU CHO BAI 11: BATCH VS STREAMING")
    print("=" * 70)

    # 1. Tao cac thu muc can thiet
    for d in [BATCH_INPUT_DIR, STREAM_INPUT_DIR, STREAM_WRITE_DIR, RAW_SOURCE_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    for sub in ["output", "checkpoint"]:
        target = os.path.join(BASE_DIR, sub)
        if os.path.exists(target):
            shutil.rmtree(target)
        os.makedirs(target, exist_ok=True)

    # 2. Ghi 3 file nguon goc vao source_files de tien copy/feed vao stream
    f1_src = os.path.join(RAW_SOURCE_DIR, "orders_1.csv")
    f2_src = os.path.join(RAW_SOURCE_DIR, "orders_2.csv")
    f3_src = os.path.join(RAW_SOURCE_DIR, "orders_3.csv")
    write_csv(f1_src, ORDERS_1)
    write_csv(f2_src, ORDERS_2)
    write_csv(f3_src, ORDERS_3)
    print(f"Da sinh 3 file nguon goc tai: {RAW_SOURCE_DIR}")

    # 3. Copy ca 3 file vao batch_input (theo dung yeu cau)
    shutil.copy(f1_src, os.path.join(BATCH_INPUT_DIR, "orders_1.csv"))
    shutil.copy(f2_src, os.path.join(BATCH_INPUT_DIR, "orders_2.csv"))
    shutil.copy(f3_src, os.path.join(BATCH_INPUT_DIR, "orders_3.csv"))
    print(f"Da copy ca 3 file vao batch_input: {BATCH_INPUT_DIR}")
    print(f"Thu muc stream_input da duoc de trong: {STREAM_INPUT_DIR}")


if __name__ == "__main__":
    main()
