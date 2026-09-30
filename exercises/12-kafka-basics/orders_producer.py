"""
orders_producer.py
------------------
Bài thực hành (ngày 2): Kafka cơ bản - Yêu cầu 3
Producer gửi ~30 message JSON chia làm 3 đợt vào Kafka topic `orders_stream`.

Chủ động tạo các trường hợp kiểm thử cho ngày 3 (Spark Structured Streaming):
1. Duplicate order_id (1001 ở đợt 1 & 2; 1002 ở đợt 1 & 3 với updated_at mới hơn).
2. Amount invalid (amount = 0, amount < 0, amount null).
3. Status viết hoa/thường không đồng nhất (SUCCESS, completed, PENDING, Shipping, pending...).
4. Order_date sai format ('31/02/2025', '2025-13-40', '').
5. Customer_id không tồn tại trong danh mục khách hàng ('C999').
6. 2 message cố tình làm sai cú pháp JSON (không parse được JSON).
"""

import sys
import os
import json
import time
import re
import warnings
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

warnings.filterwarnings("ignore", category=DeprecationWarning)

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = ["127.0.0.1:9092"]
TOPIC_NAME = "orders_stream"

# ==============================================================================
# DỮ LIỆU 30 MESSAGE (3 ĐỢT x 10 MESSAGE)
# ==============================================================================

# Đợt 1: 10 message (6 valid, 4 invalid: 1 amount=0, 1 amount<0, 1 date sai format, 1 invalid JSON)
BATCH_1 = [
    {"order_id": "1001", "customer_id": "C001", "province": "HaNoi", "amount": 1000000.0, "status": "SUCCESS", "order_date": "2025-01-05", "updated_at": "2025-01-05 08:30:00"},
    {"order_id": "1002", "customer_id": "C002", "province": "HoChiMinh", "amount": 2500000.0, "status": "completed", "order_date": "2025-01-06", "updated_at": "2025-01-06 09:15:00"},
    {"order_id": "1003", "customer_id": "C003", "province": "DaNang", "amount": 1500000.0, "status": "PENDING", "order_date": "2025-01-07", "updated_at": "2025-01-07 10:00:00"},
    {"order_id": "1004", "customer_id": "C004", "province": "CanTho", "amount": 800000.0, "status": "COMPLETED", "order_date": "2025-01-08", "updated_at": "2025-01-08 11:20:00"},
    {"order_id": "1005", "customer_id": "C001", "province": "HaNoi", "amount": 0.0, "status": "SUCCESS", "order_date": "2025-01-09", "updated_at": "2025-01-09 14:00:00"},            # amount == 0
    {"order_id": "1006", "customer_id": "C005", "province": "HaiPhong", "amount": 1200000.0, "status": "Shipping", "order_date": "2025-01-10", "updated_at": "2025-01-10 15:30:00"},
    {"order_id": "1007", "customer_id": "C002", "province": "HoChiMinh", "amount": -50000.0, "status": "SUCCESS", "order_date": "2025-01-11", "updated_at": "2025-01-11 16:45:00"},    # amount < 0
    {"order_id": "1008", "customer_id": "C006", "province": "DaNang", "amount": 3000000.0, "status": "COMPLETED", "order_date": "31/02/2025", "updated_at": "2025-01-12 17:00:00"},    # date sai format
    {"order_id": "1009", "customer_id": "C007", "province": "CanTho", "amount": 600000.0, "status": "SUCCESS", "order_date": "2025-01-13", "updated_at": "2025-01-13 18:10:00"},
    "INVALID_RAW_STRING_MESSAGE_1_NOT_A_JSON"                                                                                                                                # Invalid JSON
]

# Đợt 2: 10 message (7 valid, 3 invalid: 2 amount null, 1 date sai format; 1 duplicate 1001, 1 customer C999)
BATCH_2 = [
    {"order_id": "1001", "customer_id": "C001", "province": "HaNoi", "amount": 1500000.0, "status": "COMPLETED", "order_date": "2025-01-15", "updated_at": "2025-01-15 10:00:00"},  # duplicate 1001
    {"order_id": "1011", "customer_id": "C008", "province": "HoChiMinh", "amount": 4000000.0, "status": "SUCCESS", "order_date": "2025-01-16", "updated_at": "2025-01-16 11:00:00"},
    {"order_id": "1012", "customer_id": "C009", "province": "DaNang", "amount": None, "status": "COMPLETED", "order_date": "2025-01-17", "updated_at": "2025-01-17 12:30:00"},        # amount null
    {"order_id": "1013", "customer_id": "C004", "province": "CanTho", "amount": 1100000.0, "status": "pending", "order_date": "2025-01-18", "updated_at": "2025-01-18 13:45:00"},
    {"order_id": "1014", "customer_id": "C010", "province": "HaiPhong", "amount": 900000.0, "status": "SUCCESS", "order_date": "2025-01-19", "updated_at": "2025-01-19 14:15:00"},
    {"order_id": "1015", "customer_id": "C005", "province": "HaNoi", "amount": 1800000.0, "status": "COMPLETED", "order_date": "2025-13-40", "updated_at": "2025-01-20 15:00:00"},  # date sai format
    {"order_id": "1016", "customer_id": "C002", "province": "HoChiMinh", "amount": 3200000.0, "status": "Shipping", "order_date": "2025-01-21", "updated_at": "2025-01-21 16:20:00"},
    {"order_id": "1017", "customer_id": "C999", "province": "DaNang", "amount": 2200000.0, "status": "SUCCESS", "order_date": "2025-01-22", "updated_at": "2025-01-22 17:35:00"},    # customer C999
    {"order_id": "1018", "customer_id": "C007", "province": "CanTho", "amount": None, "status": "COMPLETED", "order_date": "2025-01-23", "updated_at": "2025-01-23 18:40:00"},        # amount null
    {"order_id": "1019", "customer_id": "C001", "province": "HaNoi", "amount": 2700000.0, "status": "SUCCESS", "order_date": "2025-01-24", "updated_at": "2025-01-24 19:50:00"},
]

# Đợt 3: 10 message (6 valid, 4 invalid: 1 amount<0, 1 date empty, 1 amount=0, 1 invalid JSON; 1 duplicate 1002)
BATCH_3 = [
    {"order_id": "1002", "customer_id": "C002", "province": "HoChiMinh", "amount": 2800000.0, "status": "COMPLETED", "order_date": "2025-01-25", "updated_at": "2025-01-25 09:00:00"}, # duplicate 1002
    {"order_id": "1020", "customer_id": "C003", "province": "DaNang", "amount": 1700000.0, "status": "SUCCESS", "order_date": "2025-01-26", "updated_at": "2025-01-26 10:15:00"},
    {"order_id": "1021", "customer_id": "C008", "province": "CanTho", "amount": 500000.0, "status": "pending", "order_date": "2025-01-27", "updated_at": "2025-01-27 11:30:00"},
    {"order_id": "1022", "customer_id": "C009", "province": "HaiPhong", "amount": -100000.0, "status": "FAILED", "order_date": "2025-01-28", "updated_at": "2025-01-28 12:45:00"},   # amount < 0
    {"order_id": "1023", "customer_id": "C010", "province": "HaNoi", "amount": 3500000.0, "status": "COMPLETED", "order_date": "", "updated_at": "2025-01-29 13:50:00"},             # date empty
    {"order_id": "1024", "customer_id": "C004", "province": "HoChiMinh", "amount": 1900000.0, "status": "SUCCESS", "order_date": "2025-01-30", "updated_at": "2025-01-30 14:20:00"},
    {"order_id": "1025", "customer_id": "C005", "province": "DaNang", "amount": 0.0, "status": "CANCELLED", "order_date": "2025-01-31", "updated_at": "2025-01-31 15:10:00"},          # amount == 0
    {"order_id": "1026", "customer_id": "C006", "province": "CanTho", "amount": 1300000.0, "status": "Shipping", "order_date": "2025-02-01", "updated_at": "2025-02-01 16:00:00"},
    {"order_id": "1027", "customer_id": "C007", "province": "HaiPhong", "amount": 2100000.0, "status": "SUCCESS", "order_date": "2025-02-02", "updated_at": "2025-02-02 17:15:00"},
    '{"order_id": 9999, "broken_json": unclosed_string_error'                                                                                                              # Invalid JSON
]

ALL_BATCHES = [BATCH_1, BATCH_2, BATCH_3]


def is_valid_date(date_str):
    """Kiểm tra format date YYYY-MM-DD hợp lệ."""
    if not date_str or not isinstance(date_str, str):
        return False
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return False
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def validate_order(item):
    """
    Kiểm tra một message có phải là đơn hàng hợp lệ:
    1. Phải là dict (JSON hợp lệ).
    2. amount phải tồn tại, là số và > 0.
    3. order_date đúng format YYYY-MM-DD.
    """
    if not isinstance(item, dict):
        return False, "Not valid JSON dict"
    amount = item.get("amount")
    if amount is None or not isinstance(amount, (int, float)) or amount <= 0:
        return False, f"Invalid amount: {amount}"
    order_date = item.get("order_date")
    if not is_valid_date(order_date):
        return False, f"Invalid order_date: {order_date}"
    return True, "Valid"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3
    )


def send_all_batches(producer, sleep_between_batches=2):
    print("=" * 75)
    print(f"BẮT ĐẦU GỬI MESSAGE VÀO KAFKA TOPIC: {TOPIC_NAME}")
    print("=" * 75)

    total_sent = 0
    valid_orders_sent = []
    latest_orders_map = {}  # order_id -> latest valid order

    for batch_idx, batch in enumerate(ALL_BATCHES, 1):
        print(f"\n--- ĐANG GỬI ĐỢT {batch_idx}/3 (Số message: {len(batch)}) ---")
        for item in batch:
            total_sent += 1
            is_valid, reason = validate_order(item)

            if isinstance(item, dict):
                key = str(item.get("order_id", ""))
                val = item
                order_id = key
            else:
                key = None
                val = item
                order_id = "RAW_INVALID"

            future = producer.send(TOPIC_NAME, key=key, value=val)
            metadata = future.get(timeout=10)

            valid_str = "HỢP LỆ" if is_valid else f"LỖI ({reason})"
            print(f"[{total_sent:02d}] Key={str(key):<5} | Partition={metadata.partition} | Offset={metadata.offset:<3} | {valid_str}")

            if is_valid:
                valid_orders_sent.append(item)
                # Cập nhật dedup theo updated_at
                current_best = latest_orders_map.get(order_id)
                if not current_best or item["updated_at"] > current_best["updated_at"]:
                    latest_orders_map[order_id] = item

        producer.flush()
        if batch_idx < len(ALL_BATCHES):
            print(f"Đã gửi xong đợt {batch_idx}. Chờ {sleep_between_batches}s trước đợt tiếp theo...")
            time.sleep(sleep_between_batches)

    # Thống kê tổng hợp
    gross_valid_count = len(valid_orders_sent)
    gross_valid_amount = sum(item["amount"] for item in valid_orders_sent)

    dedup_valid_count = len(latest_orders_map)
    dedup_valid_amount = sum(item["amount"] for item in latest_orders_map.values())

    print("\n" + "=" * 75)
    print("BÁO CÁO TỔNG KẾT DỮ LIỆU ĐÃ GỬI (PRODUCER SUMMARY)")
    print("=" * 75)
    print(f"1. Tổng số message đã gửi                  : {total_sent}")
    print(f"2. Số message không phải JSON hợp lệ       : {total_sent - 28} (2 message chuỗi thô/lỗi cú pháp)")
    print(f"3. Số message đơn hàng hợp lệ (gốc đã gửi) : {gross_valid_count}")
    print(f"   -> TỔNG AMOUNT CỦA CÁC ORDER HỢP LỆ     : {gross_valid_amount:,.1f} VND")
    print(f"4. Số đơn hàng hợp lệ sau dedup order_id   : {dedup_valid_count} đơn hàng duy nhất")
    print(f"   -> TỔNG AMOUNT SAU DEDUP (LATEST)       : {dedup_valid_amount:,.1f} VND")
    print("=" * 75)

    return {
        "total_sent": total_sent,
        "gross_valid_count": gross_valid_count,
        "gross_valid_amount": gross_valid_amount,
        "dedup_valid_count": dedup_valid_count,
        "dedup_valid_amount": dedup_valid_amount,
    }


def main():
    try:
        producer = create_producer()
        try:
            send_all_batches(producer, sleep_between_batches=2)
        finally:
            producer.close()
            print("\nKafka Producer đã đóng kết nối.")
    except NoBrokersAvailable:
        print("\n" + "!" * 75)
        print("[LỖI KẾT NỐI] Không tìm thấy Kafka Broker tại 127.0.0.1:9092!")
        print("Nguyên nhân: Docker Desktop hoặc container Kafka chưa hoạt động.")
        print("Cách xử lý: Khởi động Docker Desktop và chạy container Kafka trên cổng 9092.")
        print("!" * 75)
        sys.exit(1)


if __name__ == "__main__":
    main()
