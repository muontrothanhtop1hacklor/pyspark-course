"""
generate_data.py
-----------------
Sinh 2 file customers.csv / transactions.csv, cố tình cấy các trường hợp lỗi
theo đúng yêu cầu bài tập:
  - customer_id không tồn tại trong customers (orphan)
  - transaction thiếu customer_id
  - amount <= 0
  - transaction_id trùng lặp với nhiều bản ghi updated_at khác nhau (late-arriving update)
  - status có SUCCESS / FAILED / PENDING
  - 1 khách hàng có nhiều transaction
"""
import csv
import random
from datetime import datetime, timedelta

random.seed(42)

PROVINCES = ["HaNoi", "HCM", "DaNang", "HaiPhong", "CanTho"]
N_CUSTOMERS = 20
N_BASE_TXN = 200  # số transaction_id "gốc", sau đó ta sẽ cấy thêm bản duplicate/lỗi

# ---------------------------------------------------------------------------
# 1. customers.csv
# ---------------------------------------------------------------------------
customers = []
for i in range(1, N_CUSTOMERS + 1):
    cid = f"C{i:04d}"
    created = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 300))
    customers.append(
        {
            "customer_id": cid,
            "customer_name": f"Customer_{i}",
            "province": random.choice(PROVINCES),
            "created_at": created.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )

with open("data/customers.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f, fieldnames=["customer_id", "customer_name", "province", "created_at"]
    )
    writer.writeheader()
    writer.writerows(customers)

valid_customer_ids = [c["customer_id"] for c in customers]

# ---------------------------------------------------------------------------
# 2. transactions.csv
# ---------------------------------------------------------------------------
STATUSES = ["SUCCESS", "FAILED", "PENDING"]
rows = []
base_time = datetime(2024, 3, 1)


def txn_row(tid, cid, amount, status, ttime, updated_at):
    return {
        "transaction_id": tid,
        "customer_id": cid,
        "amount": amount,
        "status": status,
        "transaction_time": ttime.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


tid_counter = 1
for i in range(N_BASE_TXN):
    tid = f"T{tid_counter:05d}"
    tid_counter += 1

    cid = random.choice(valid_customer_ids)  # nhiều txn / 1 customer là bình thường
    amount = round(random.uniform(10_000, 5_000_000), 2)
    status = random.choices(STATUSES, weights=[0.7, 0.15, 0.15])[0]
    ttime = base_time + timedelta(
        days=random.randint(0, 60), hours=random.randint(0, 23)
    )
    updated_at = ttime + timedelta(minutes=random.randint(0, 30))

    rows.append(txn_row(tid, cid, amount, status, ttime, updated_at))

    # ---- cấy lỗi có chủ đích, xác suất thấp để không phá vỡ phần dữ liệu "sạch" ----

    # (a) duplicate transaction_id: cùng 1 tid xuất hiện lại với updated_at MỚI HƠN
    #     và có thể status/amount khác (mô phỏng update trạng thái đơn hàng)
    if random.random() < 0.12:
        later_update = updated_at + timedelta(hours=random.randint(1, 48))
        new_status = random.choice(STATUSES)
        rows.append(
            txn_row(tid, cid, amount, new_status, ttime, later_update)
        )
        # thỉnh thoảng cấy thêm bản thứ 3 để test "giữ đúng bản mới nhất" chắc chắn hơn
        if random.random() < 0.3:
            latest_update = later_update + timedelta(hours=random.randint(1, 24))
            rows.append(
                txn_row(tid, cid, amount, random.choice(STATUSES), ttime, latest_update)
            )

    # (b) thiếu customer_id
    if random.random() < 0.05:
        tid2 = f"T{tid_counter:05d}"
        tid_counter += 1
        rows.append(
            txn_row(tid2, "", round(random.uniform(10_000, 500_000), 2),
                     random.choice(STATUSES), ttime, updated_at)
        )

    # (c) amount <= 0 (0 hoặc âm - dữ liệu bẩn / hoàn tiền ghi sai)
    if random.random() < 0.05:
        tid3 = f"T{tid_counter:05d}"
        tid_counter += 1
        bad_amount = random.choice([0, -1, -random.uniform(1000, 50000)])
        rows.append(
            txn_row(tid3, cid, round(bad_amount, 2),
                     random.choice(STATUSES), ttime, updated_at)
        )

    # (d) customer_id không tồn tại trong customers (orphan / mapping lỗi)
    if random.random() < 0.05:
        tid4 = f"T{tid_counter:05d}"
        tid_counter += 1
        fake_cid = f"C{random.randint(9000, 9999)}"
        rows.append(
            txn_row(tid4, fake_cid, round(random.uniform(10_000, 500_000), 2),
                     random.choice(STATUSES), ttime, updated_at)
        )

with open("data/transactions.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "transaction_id",
            "customer_id",
            "amount",
            "status",
            "transaction_time",
            "updated_at",
        ],
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"customers.csv: {len(customers)} rows")
print(f"transactions.csv: {len(rows)} rows (bao gồm duplicate + lỗi cố tình)")
