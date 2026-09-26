import csv
import random
from datetime import date, timedelta, datetime

random.seed(23)

provinces = ["HaNoi", "HoChiMinh", "DaNang", "CanTho", "HaiPhong", "NgheAn"]
customer_types = ["REGULAR", "VIP", "NEW"]
statuses_canon = ["PENDING", "COMPLETED", "CANCELLED", "SHIPPING"]

# ---------- customers.csv ----------
N_CUSTOMERS = 200
with open("customers.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["customer_id", "customer_name", "customer_type"])
    first = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Dang", "Bui", "Do", "Ngo"]
    last = ["An", "Binh", "Cuong", "Dung", "Em", "Phuong", "Giang", "Hoa", "Ich", "Khanh"]
    for cid in range(1, N_CUSTOMERS + 1):
        name = f"{random.choice(first)} {random.choice(last)}"
        ctype = random.choice(customer_types)
        w.writerow([cid, name, ctype])

# ---------- orders.csv ----------
N_ORDERS = 3000
start = date(2025, 1, 1)
rows = []

for i in range(1, N_ORDERS + 1):
    customer_id = random.randint(1, N_CUSTOMERS)
    province = random.choice(provinces)
    status = random.choice(statuses_canon)
    d = start + timedelta(days=random.randint(0, 300))
    order_date = d.isoformat()
    amount = round(random.uniform(50_000, 20_000_000), 0)
    updated_at = datetime(d.year, d.month, d.day, random.randint(0, 23), random.randint(0, 59)).isoformat(sep=" ")
    rows.append([i, customer_id, province, amount, status, order_date, updated_at])

def pick_indices(k, exclude=set()):
    pool = [i for i in range(N_ORDERS) if i not in exclude]
    return random.sample(pool, k)

used = set()

# 1) order_id duplicate (~5%): tạo bản ghi update sau với updated_at mới hơn, có thể khác amount/status
dup_idx = pick_indices(150)
used |= set(dup_idx)
dup_rows = []
for idx in dup_idx:
    orig = rows[idx]
    order_id, customer_id, province, amount, status, order_date, updated_at = orig
    new_amount = round(random.uniform(50_000, 20_000_000), 0)
    new_status = random.choice(statuses_canon)
    old_dt = datetime.fromisoformat(updated_at)
    new_dt = old_dt + timedelta(hours=random.randint(1, 48))
    dup_rows.append([order_id, customer_id, province, new_amount, new_status, order_date, new_dt.isoformat(sep=" ")])

# 2) amount null hoặc <=0 (~5%)
for idx in pick_indices(150, used):
    used.add(idx)
    choice = random.choice(["null", "zero", "negative"])
    if choice == "null":
        rows[idx][3] = ""
    elif choice == "zero":
        rows[idx][3] = 0
    else:
        rows[idx][3] = -random.randint(10_000, 500_000)

# 3) status hoa/thường không đồng nhất (~5%, không tính status vốn đã lộn xộn theo lựa chọn ngẫu nhiên)
for idx in pick_indices(150, used):
    used.add(idx)
    s = rows[idx][4]
    rows[idx][4] = random.choice([s.lower(), s.capitalize(), s.lower().capitalize()])

# 4) order_date sai format (~5%)
for idx in pick_indices(150, used):
    used.add(idx)
    choice = random.choice(["bad_format", "empty", "invalid_date"])
    if choice == "bad_format":
        rows[idx][5] = "31/02/2025"
    elif choice == "empty":
        rows[idx][5] = ""
    else:
        rows[idx][5] = "2025-13-40"

# 5) customer_id không tồn tại trong customers (~4%)
for idx in pick_indices(120, used):
    used.add(idx)
    rows[idx][1] = random.randint(N_CUSTOMERS + 1, N_CUSTOMERS + 500)

all_rows = rows + dup_rows
random.shuffle(all_rows)

with open("orders.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["order_id", "customer_id", "province", "amount", "status", "order_date", "updated_at"])
    w.writerows(all_rows)

print(f"customers.csv: {N_CUSTOMERS} dòng")
print(f"orders.csv: {len(all_rows)} dòng (gồm {len(dup_rows)} bản ghi duplicate order_id)")
