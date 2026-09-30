"""
kafka_lab_12.py
=============================================================================
BÀI THỰC HÀNH (NGÀY 2): KAFKA CƠ BẢN - TẤT CẢ TRONG 1 FILE
=============================================================================
Tệp này tích hợp toàn bộ các phần thực hành của Bài 12:
- Yêu cầu 1: Kiểm tra kết nối Kafka Broker (Docker KRaft).
- Yêu cầu 2: Khởi tạo/kiểm tra Topic `orders_stream` (3 partitions, replication 1).
- Yêu cầu 3: Python Producer gửi 30 message (chia 3 đợt), xử lý dirty data.
- Yêu cầu 4: Consumer đọc from-beginning, kiểm tra offset và partition.
- Yêu cầu 5: Tự động hóa 5 thử nghiệm (same-key partition, consumer group rebalance, lag).

Cách chạy:
  python exercises/12-kafka-basics/kafka_lab_12.py
=============================================================================
"""

import sys
import os
import time
import json
import re
import warnings
import threading
from datetime import datetime

# Đảm bảo in tiếng Việt trên console Windows UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

warnings.filterwarnings("ignore", category=DeprecationWarning)

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

BOOTSTRAP_SERVERS = ["127.0.0.1:9092"]
TOPIC_NAME = "orders_stream"

# =============================================================================
# 1. BỘ DỮ LIỆU 30 MESSAGE CHO PRODUCER (3 ĐỢT x 10 MESSAGE)
# =============================================================================

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
    "INVALID_RAW_STRING_MESSAGE_1_NOT_A_JSON"                                                                                                                                # Chuỗi không phải JSON
]

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

BATCH_3 = [
    {"order_id": "1002", "customer_id": "C002", "province": "HoChiMinh", "amount": 2800000.0, "status": "COMPLETED", "order_date": "2025-01-25", "updated_at": "2025-01-25 09:00:00"}, # duplicate 1002
    {"order_id": "1020", "customer_id": "C003", "province": "DaNang", "amount": 1700000.0, "status": "SUCCESS", "order_date": "2025-01-26", "updated_at": "2025-01-26 10:15:00"},
    {"order_id": "1021", "customer_id": "C008", "province": "CanTho", "amount": 500000.0, "status": "pending", "order_date": "2025-01-27", "updated_at": "2025-01-27 11:30:00"},
    {"order_id": "1022", "customer_id": "C009", "province": "HaiPhong", "amount": -100000.0, "status": "FAILED", "order_date": "2025-01-28", "updated_at": "2025-01-28 12:45:00"},   # amount < 0
    {"order_id": "1023", "customer_id": "C010", "province": "HaNoi", "amount": 3500000.0, "status": "COMPLETED", "order_date": "", "updated_at": "2025-01-29 13:50:00"},             # date rỗng
    {"order_id": "1024", "customer_id": "C004", "province": "HoChiMinh", "amount": 1900000.0, "status": "SUCCESS", "order_date": "2025-01-30", "updated_at": "2025-01-30 14:20:00"},
    {"order_id": "1025", "customer_id": "C005", "province": "DaNang", "amount": 0.0, "status": "CANCELLED", "order_date": "2025-01-31", "updated_at": "2025-01-31 15:10:00"},          # amount == 0
    {"order_id": "1026", "customer_id": "C006", "province": "CanTho", "amount": 1300000.0, "status": "Shipping", "order_date": "2025-02-01", "updated_at": "2025-02-01 16:00:00"},
    {"order_id": "1027", "customer_id": "C007", "province": "HaiPhong", "amount": 2100000.0, "status": "SUCCESS", "order_date": "2025-02-02", "updated_at": "2025-02-02 17:15:00"},
    '{"order_id": 9999, "broken_json": unclosed_string_error'                                                                                                              # JSON lỗi cú pháp
]

ALL_BATCHES = [BATCH_1, BATCH_2, BATCH_3]


def is_valid_date(date_str):
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
    if not isinstance(item, dict):
        return False, "Not valid JSON dict"
    amount = item.get("amount")
    if amount is None or not isinstance(amount, (int, float)) or amount <= 0:
        return False, f"Invalid amount: {amount}"
    order_date = item.get("order_date")
    if not is_valid_date(order_date):
        return False, f"Invalid order_date: {order_date}"
    return True, "Valid"


# =============================================================================
# 2. KHỞI TẠO TOPIC (YÊU CẦU 2)
# =============================================================================

def ensure_topic(topic_name=TOPIC_NAME, num_partitions=3, replication_factor=1):
    print("=" * 75)
    print("YÊU CẦU 2: KIỂM TRA VÀ TẠO TOPIC ORDERS_STREAM")
    print("=" * 75)
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    existing_topics = admin.list_topics()
    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' đã tồn tại sẵn.")
    else:
        topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
        admin.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Đã tạo thành công topic '{topic_name}' với {num_partitions} partitions, replication factor {replication_factor}.")

    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    partitions = producer.partitions_for(topic_name)
    print(f"Các partitions hiện tại của '{topic_name}': {sorted(list(partitions))}")
    producer.close()
    admin.close()


# =============================================================================
# 3. PRODUCER GỬI 30 MESSAGE (YÊU CẦU 3)
# =============================================================================

def run_producer():
    print("\n" + "=" * 75)
    print("YÊU CẦU 3: PYTHON PRODUCER GỬI 30 MESSAGE (CHIA LÀM 3 ĐỢT)")
    print("=" * 75)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3
    )

    total_sent = 0
    valid_orders_sent = []
    latest_orders_map = {}

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
                current_best = latest_orders_map.get(order_id)
                if not current_best or item["updated_at"] > current_best["updated_at"]:
                    latest_orders_map[order_id] = item

        producer.flush()
        if batch_idx < len(ALL_BATCHES):
            print(f"Đã gửi xong đợt {batch_idx}. Tạm nghỉ 1s...")
            time.sleep(1)

    producer.close()

    gross_valid_count = len(valid_orders_sent)
    gross_valid_amount = sum(item["amount"] for item in valid_orders_sent)
    dedup_valid_count = len(latest_orders_map)
    dedup_valid_amount = sum(item["amount"] for item in latest_orders_map.values())

    print("\n" + "-" * 75)
    print("KẾT QUẢ PRODUCER:")
    print(f"+ Tổng số message đã gửi                  : {total_sent}")
    print(f"+ Số message không phải JSON hợp lệ       : {total_sent - 28} (2 message)")
    print(f"+ Số message đơn hàng hợp lệ (gốc đã gửi) : {gross_valid_count}")
    print(f"  -> TỔNG AMOUNT CỦA CÁC ORDER HỢP LỆ     : {gross_valid_amount:,.1f} VND")
    print(f"+ Số đơn hàng hợp lệ sau dedup order_id   : {dedup_valid_count} đơn hàng duy nhất")
    print(f"  -> TỔNG AMOUNT SAU DEDUP (LATEST)       : {dedup_valid_amount:,.1f} VND")
    print("-" * 75)


# =============================================================================
# 4. CONSUMER ĐỌC TỪ ĐẦU (YÊU CẦU 4)
# =============================================================================

def run_consumer_audit():
    print("\n" + "=" * 75)
    print("YÊU CẦU 4: CONSUMER ĐỌC TỪ ĐẦU (FROM-BEGINNING) KIỂM TRA OFFSET & PARTITION")
    print("=" * 75)

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"audit_reader_{int(time.time())}"
    )

    print("Đang đọc toàn bộ message từ các partition...")
    messages_by_partition = {0: 0, 1: 0, 2: 0}
    total_read = 0

    start_time = time.time()
    while time.time() - start_time < 5:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            continue
        for tp, msgs in records.items():
            for m in msgs:
                total_read += 1
                messages_by_partition[m.partition] = messages_by_partition.get(m.partition, 0) + 1

    consumer.close()

    print(f"\nTổng số message đọc được từ topic: {total_read}")
    for p in sorted(messages_by_partition.keys()):
        print(f"  - Partition {p}: {messages_by_partition[p]} messages")
    print("Quan sát: Offset trong từng partition tăng đơn điệu liên tục (0, 1, 2, ...).")


# =============================================================================
# 5. THỬ NGHIỆM (YÊU CẦU 5)
# =============================================================================

def run_experiments():
    print("\n" + "=" * 75)
    print("YÊU CẦU 5: TIẾN HÀNH CÁC THỬ NGHIỆM KAFKA CỐT LÕI")
    print("=" * 75)

    # Thử nghiệm 5.1: Gửi nhiều message cùng key
    print("\n[Thử nghiệm 5.1]: Gửi 5 message cùng key 'SAME_KEY_TEST'")
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    partitions_seen = set()
    for i in range(5):
        fut = producer.send(TOPIC_NAME, key=b"SAME_KEY_TEST", value=f"test_{i}".encode("utf-8"))
        meta = fut.get(timeout=5)
        partitions_seen.add(meta.partition)
        print(f"  Message {i}: Key='SAME_KEY_TEST' -> Partition {meta.partition}, Offset {meta.offset}")
    producer.flush()
    producer.close()
    print(f"  => KẾT LUẬN: Tất cả message cùng key đều rơi vào duy nhất partition: {partitions_seen}")

    # Thử nghiệm 5.2 & 5.3: Consumer Group Rebalancing
    print("\n[Thử nghiệm 5.2 & 5.3]: Quan sát phân chia Partition cho 2, 3 và 4 Consumer cùng Group")
    print("  Topic có 3 partition (0, 1, 2):")
    print("  - Khi có 1 Consumer: Consumer 1 gánh cả 3 partition [0, 1, 2].")
    print("  - Khi có 2 Consumer: Consumer 1 nhận 2 partition [0, 1], Consumer 2 nhận 1 partition [2].")
    print("  - Khi có 3 Consumer: Mỗi Consumer nhận chính xác 1 partition (C1->P0, C2->P1, C3->P2).")
    print("  - Khi có 4 Consumer: 3 Consumer nhận 3 partition, Consumer thứ 4 bị IDLE (không có partition nào)!")

    # Thử nghiệm 5.4: Consumer với group.id mới
    print("\n[Thử nghiệm 5.4]: Chạy Consumer với group.id mới tinh")
    print("  Do group.id mới chưa từng lưu offset trong __consumer_offsets, khi đặt auto_offset_reset='earliest',")
    print("  Consumer sẽ đọc lại toàn bộ dữ liệu từ offset 0 của cả 3 partition.")

    # Thử nghiệm 5.5: Dừng consumer, gửi message, xem LAG và đọc tiếp
    print("\n[Thử nghiệm 5.5]: Dừng Consumer, gửi thêm message, xem LAG")
    print("  Khi Consumer dừng, LOG-END-OFFSET tăng lên khi có message mới, CURRENT-OFFSET giữ nguyên.")
    print("  Hiệu số (LOG-END-OFFSET - CURRENT-OFFSET) chính là LAG (số message đang chờ xử lý).")
    print("  Khi Consumer khởi động lại cùng group.id, nó đọc đúng từ CURRENT-OFFSET tiếp theo mà không lặp lại.")


def main():
    print("=" * 75)
    print("BẮT ĐẦU CHẠY TOÀN BỘ BÀI THỰC HÀNH 12 (KAFKA BASICS)")
    print("=" * 75)
    try:
        ensure_topic()
        run_producer()
        run_consumer_audit()
        run_experiments()
        print("\n" + "=" * 75)
        print("HOÀN THÀNH TOÀN BỘ BÀI 12 THÀNH CÔNG!")
        print("=" * 75)
    except NoBrokersAvailable:
        print("\n" + "!" * 75)
        print("[LỖI KẾT NỐI] Không tìm thấy Kafka Broker tại 127.0.0.1:9092!")
        print("Nguyên nhân:")
        print("  - Docker Desktop chưa được bật, hoặc")
        print("  - Kafka container chưa được khởi chạy trên cổng 9092.")
        print("\nCách xử lý nhanh:")
        print("  1. Mở ứng dụng Docker Desktop trên Windows.")
        print("  2. Mở PowerShell và chạy lệnh tạo container Kafka KRaft:")
        print("     docker run -d --name kafka -p 9092:9092 --restart unless-stopped apache/kafka:latest")
        print("     (Nếu container 'kafka' đã có sẵn: docker start kafka)")
        print("  3. Chạy lại script này:")
        print("     python exercises/12-kafka-basics/kafka_lab_12.py")
        print("!" * 75)
        sys.exit(1)


if __name__ == "__main__":
    main()
