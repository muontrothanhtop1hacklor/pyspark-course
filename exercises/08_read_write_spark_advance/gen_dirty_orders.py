import csv
import random
from datetime import date, timedelta

random.seed(11)

provinces = ["HaNoi", "HoChiMinh", "DaNang", "CanTho", "HaiPhong", "NgheAn"]
status_variants = {
    "PENDING":   ["PENDING", "pending", "Pending"],
    "COMPLETED": ["COMPLETED", "completed", "Completed"],
    "CANCELLED": ["CANCELLED", "cancelled", "Cancelled"],
    "SHIPPING":  ["SHIPPING", "shipping", "Shipping"],
}
start = date(2025, 1, 1)

N = 2000
rows = []

for i in range(1, N + 1):
    customer_id = random.randint(1, 500)
    province = random.choice(provinces)
    status = random.choice(random.choice(list(status_variants.values())))
    d = start + timedelta(days=random.randint(0, 300))
    order_date = d.isoformat()
    amount = round(random.uniform(50_000, 5_000_000), 0)

    rows.append([i, customer_id, province, amount, status, order_date])

# --- Chủ động chèn dữ liệu bẩn ---

def pick_indices(k):
    return random.sample(range(N), k)

# 1) amount sai kiểu hoặc null (~5%)
for idx in pick_indices(100):
    choice = random.choice(["text", "empty", "negative"])
    if choice == "text":
        rows[idx][3] = "N/A"           # sai kiểu -> Spark parse ra null với schema DoubleType
    elif choice == "empty":
        rows[idx][3] = ""              # null
    else:
        rows[idx][3] = -random.randint(10_000, 500_000)  # amount âm -> invalid theo business rule

# 2) order_date sai format (~5%)
for idx in pick_indices(100):
    choice = random.choice(["bad_format", "empty", "invalid_date"])
    if choice == "bad_format":
        rows[idx][5] = "31/02/2025"        # sai format dd/mm/yyyy thay vì yyyy-MM-dd
    elif choice == "empty":
        rows[idx][5] = ""
    else:
        rows[idx][5] = "2025-13-40"        # tháng/ngày không tồn tại

# 3) province null (~4%)
for idx in pick_indices(80):
    rows[idx][2] = ""

# 4) status hoa/thường không đồng nhất đã có sẵn qua status_variants ở trên,
#    thêm một số biến thể chữ hoa/thường bất thường khác cho chắc
for idx in pick_indices(60):
    s = rows[idx][4]
    rows[idx][4] = random.choice([s.lower(), s.upper(), s.capitalize()])

with open("orders_dirty.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["order_id", "customer_id", "province", "amount", "status", "order_date"])
    w.writerows(rows)

print(f"Generated orders_dirty.csv with {N} rows (đã chèn dữ liệu bẩn)")
