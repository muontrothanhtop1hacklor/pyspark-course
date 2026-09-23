import csv
import random
from datetime import date, timedelta

random.seed(42)

provinces = ["HaNoi", "HoChiMinh", "DaNang", "CanTho", "HaiPhong",
             "NgheAn", "ThanhHoa", "BinhDuong", "DongNai", "KhanhHoa"]
statuses = ["PENDING", "COMPLETED", "CANCELLED", "SHIPPING"]

start = date(2025, 1, 1)
N = 200_000  # đủ lớn để thấy khác biệt về file/partition

with open("orders.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["order_id", "customer_id", "province", "amount", "status", "order_date"])
    for i in range(1, N + 1):
        pid = random.randint(1, 50000)
        prov = random.choices(
            provinces,
            weights=[25, 20, 8, 6, 8, 6, 6, 8, 7, 6],  # phân bố lệch để thấy skew khi repartition("province")
        )[0]
        amount = round(random.uniform(10_000, 5_000_000), 0)
        status = random.choice(statuses)
        d = start + timedelta(days=random.randint(0, 300))
        w.writerow([i, pid, prov, amount, status, d.isoformat()])

print(f"Generated orders.csv with {N} rows")
