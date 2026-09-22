"""Sinh customers.csv - bang khach hang cho bai Join (00d-joins).

orders.csv trong thu muc nay LA FILE THAT cua bai 01-orders-aggregation
(khong sinh lai), gom cac customer_id: C001, C002, C003, C004, C005, C006, C007.

customers.csv o day CO CHU Y bo bot mot vai customer_id so voi orders.csv
(khong tao them C001, C005, C007) de co san truong hop "order khong tim
thay customer" ma KHONG can sua orders.csv that:

    - order 1006 (customer_id=C005) -> khong co C005 trong customers.csv
    - order 1008 (customer_id=C007) -> khong co C007 trong customers.csv
"""

import csv
from pathlib import Path

HEADER = ["customer_id", "customer_name", "email", "customer_since"]

ROWS = [
    ["C001", "Nguyen Van A", "a.nguyen@example.com", "2025-01-15"],
    ["C002", "Tran Thi B", "b.tran@example.com", "2025-02-20"],
    ["C003", "Le Van C", "c.le@example.com", "2025-03-10"],
    ["C004", "Pham Thi D", "d.pham@example.com", "2025-04-05"],
    ["C006", "Vu Thi F", "f.vu@example.com", "2025-06-18"],
    # KHONG tao C005, C007 - de lai 2 don "mo coi" trong orders.csv that
]


def main() -> None:
    out_path = Path(__file__).parent / "customers.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        writer.writerows(ROWS)
    print(f"Da sinh {len(ROWS)} khach hang vao {out_path}")
    print("customer_id CO trong orders.csv nhung KHONG co trong customers.csv: C005, C007")


if __name__ == "__main__":
    main()
