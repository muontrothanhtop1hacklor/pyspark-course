"""
Centralized Data Generator Utility
==================================
Tien ich sinh du lieu mau Orders va Customers dung chung cho cac bai tap.
Dam bao su thong nhat ve ten cot, ten tinh thanh va nguong nghiep vu.
"""

import csv
import os
import random
from datetime import date, timedelta, datetime
from .provinces import CANONICAL_PROVINCES
from .rules import VALID_STATUSES


def generate_customers_dataset(
    n_customers: int = 200,
    output_path: str = None,
    id_prefix: str = "C",
    seed: int = 42,
) -> list:
    """Sinh danh sach khach hang chuan."""
    random.seed(seed)
    customer_types = ["REGULAR", "VIP", "NEW"]
    first = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Dang", "Bui", "Do", "Ngo"]
    last = ["An", "Binh", "Cuong", "Dung", "Em", "Phuong", "Giang", "Hoa", "Ich", "Khanh"]

    customers = []
    for cid in range(1, n_customers + 1):
        c_id_str = f"{id_prefix}{cid:04d}" if id_prefix else str(cid)
        name = f"{random.choice(first)} {random.choice(last)}"
        prov = random.choice(CANONICAL_PROVINCES)
        ctype = random.choice(customer_types)
        created = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 300))
        created_str = created.strftime("%Y-%m-%d %H:%M:%S")

        customers.append({
            "customer_id": c_id_str,
            "customer_name": name,
            "province": prov,
            "customer_type": ctype,
            "created_at": created_str,
        })

    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["customer_id", "customer_name", "province", "customer_type", "created_at"]
            )
            writer.writeheader()
            writer.writerows(customers)
        print(f"Da sinh {len(customers)} khach hang vao: {output_path}")

    return customers


def generate_orders_dataset(
    n_orders: int = 2000,
    output_path: str = None,
    n_customers: int = 200,
    id_prefix: str = "C",
    include_updated_at: bool = True,
    dirty_ratio: float = 0.05,
    duplicate_ratio: float = 0.05,
    seed: int = 42,
) -> list:
    """Sinh danh sach don hang chuan, ho tro cay loi va du lieu trung lap."""
    random.seed(seed)
    start = date(2025, 1, 1)
    base_rows = []

    for i in range(1, n_orders + 1):
        cid_num = random.randint(1, n_customers)
        cid_str = f"{id_prefix}{cid_num:04d}" if id_prefix else str(cid_num)
        prov = random.choice(CANONICAL_PROVINCES)
        status = random.choice(VALID_STATUSES)
        d = start + timedelta(days=random.randint(0, 300))
        order_date = d.isoformat()
        amount = round(random.uniform(50_000, 15_000_000), 2)
        
        row = {
            "order_id": i,
            "customer_id": cid_str,
            "province": prov,
            "amount": amount,
            "status": status,
            "order_date": order_date,
        }
        if include_updated_at:
            h = random.randint(0, 23)
            m = random.randint(0, 59)
            row["updated_at"] = f"{order_date} {h:02d}:{m:02d}:00"
        
        base_rows.append(row)

    # Cay duplicates neu can
    dup_rows = []
    if duplicate_ratio > 0 and include_updated_at:
        n_dup = int(n_orders * duplicate_ratio)
        dup_indices = random.sample(range(n_orders), n_dup)
        for idx in dup_indices:
            orig = base_rows[idx].copy()
            orig["amount"] = round(random.uniform(50_000, 15_000_000), 2)
            orig["status"] = random.choice(VALID_STATUSES)
            old_dt = datetime.strptime(orig["updated_at"], "%Y-%m-%d %H:%M:%S")
            new_dt = old_dt + timedelta(hours=random.randint(1, 48))
            orig["updated_at"] = new_dt.strftime("%Y-%m-%d %H:%M:%S")
            dup_rows.append(orig)

    # Cay loi ban neu can
    if dirty_ratio > 0:
        n_dirty = int(n_orders * dirty_ratio)
        dirty_indices = random.sample(range(n_orders), n_dirty)
        for idx in dirty_indices:
            choice = random.choice(["bad_amount", "bad_date", "missing_province", "bad_status"])
            if choice == "bad_amount":
                base_rows[idx]["amount"] = "N/A"
            elif choice == "bad_date":
                base_rows[idx]["order_date"] = "2025-13-40"
            elif choice == "missing_province":
                base_rows[idx]["province"] = ""
            elif choice == "bad_status":
                base_rows[idx]["status"] = "SUCCES"

    all_rows = base_rows + dup_rows
    random.shuffle(all_rows)

    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        fieldnames = ["order_id", "customer_id", "province", "amount", "status", "order_date"]
        if include_updated_at:
            fieldnames.append("updated_at")

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"Da sinh {len(all_rows)} don hang vao: {output_path}")

    return all_rows
