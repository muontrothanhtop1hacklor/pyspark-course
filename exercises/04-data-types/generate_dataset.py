"""Sinh raw_orders_types.csv - dataset tho cho bai Chapter 2 (Data Types).

Dataset khop voi bai 01-orders-aggregation: 6 cot dau giong orders.csv
(order_id, customer_id, province, amount, status, order_date), them cac cot
de luyen kieu Integer / Timestamp / Array / Struct.

Tat ca cac cot deu duoc ghi ra duoi dang chuoi (String), giong voi du lieu
CSV that ngoai doi - ke ca number/date/timestamp cung chi la text cho toi khi
duoc cast tuong minh. Dataset gom:

- 6 dong "sach": dung dinh dang, cast se thanh cong.
- 5 dong "ban": co tinh sai dinh dang o amount / quantity / order_date /
  created_at de quan sat hanh vi cast khi gap loi.

Kieu du lieu dich sau khi cast:
    order_id                  -> Integer
    customer_id, province,
    status                    -> String
    amount                    -> Decimal(12,2)   (giong ORDER_SCHEMA bai 01)
    quantity                  -> Integer
    order_date                -> Date
    created_at                -> Timestamp
    tags                      -> Array<String>
    street, district, zip_code-> Struct (shipping_address)
"""

import csv
from pathlib import Path

HEADER = [
    "order_id",
    "customer_id",
    "province",
    "amount",
    "status",
    "order_date",
    "quantity",
    "created_at",
    "tags",
    "street",
    "district",
    "zip_code",
]

ROWS = [
    # ---- dong sach: cast phai thanh cong het ----
    ["2001", "C001", "Hanoi", "125000.50", "SUCCESS", "2026-09-01",
     "2", "2026-09-01 08:30:00", "vip,urgent", "123 Le Loi", "Hoan Kiem", "100000"],
    ["2002", "C002", "Da Nang", "89000.00", "SUCCESS", "2026-09-01",
     "1", "2026-09-01 10:15:00", "regular", "45 Bach Dang", "Hai Chau", "550000"],
    ["2003", "C003", "Ho Chi Minh", "450000.75", "FAILED", "2026-09-02",
     "5", "2026-09-02 14:05:30", "vip", "9 Tran Hung Dao", "Quan 1", "700000"],
    ["2004", "C004", "Ho Chi Minh", "275000.00", "SUCCESS", "2026-09-02",
     "1", "2026-09-02 09:00:00", "new_customer", "12 Hai Ba Trung", "Quan 3", "700000"],
    ["2005", "C005", "Can Tho", "180000.25", "SUCCESS", "2026-09-03",
     "3", "2026-09-03 16:45:00", "regular,discount", "78 Nguyen Van Cu", "Ninh Kieu", "900000"],
    ["2006", "C006", "Hai Phong", "999999.99", "SUCCESS", "2026-09-03",
     "10", "2026-09-03 23:59:59", "vip,urgent,discount", "3 Le Hong Phong", "Ngo Quyen", "180000"],
    # ---- dong ban: co tinh sai de xem cast ra sao ----
    # amount khong phai so -> cast ra NULL
    ["2007", "C007", "Hanoi", "abc", "SUCCESS", "2026-09-04",
     "1", "2026-09-04 07:00:00", "regular", "5 Le Duan", "Dong Da", "100000"],
    # order_date sai dinh dang / khong ton tai (ngay 32, thang 13) -> NULL
    ["2008", "C008", "Da Nang", "76000.00", "FAILED", "32/13/2026",
     "2", "2026-09-04 12:00:00", "new_customer", "8 Phan Chau Trinh", "Hai Chau", "550000"],
    # created_at rong -> CSV reader doc thanh NULL ngay tu dau (khong phai loi cast)
    ["2009", "C001", "Hanoi", "125000.00", "SUCCESS", "2026-09-05",
     "1", "", "vip", "21 Kim Ma", "Ba Dinh", "100000"],
    # amount am -> cast THANH CONG (dung kieu) nhung sai nghiep vu
    ["2010", "C009", "Can Tho", "-50000.00", "SUCCESS", "2026-09-05",
     "1", "2026-09-05 18:20:00", "regular", "60 Hoa Binh", "Ninh Kieu", "900000"],
    # quantity la chu thay vi so -> cast Integer ra NULL
    ["2011", "C010", "Ho Chi Minh", "210000.00", "FAILED", "2026-09-06",
     "five", "2026-09-06 11:11:11", "regular,urgent", "17 Nguyen Hue", "Quan 1", "700000"],
]


def main() -> None:
    out_path = Path(__file__).parent / "raw_orders_types.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        writer.writerows(ROWS)
    print(f"Da sinh {len(ROWS)} dong du lieu vao {out_path}")


if __name__ == "__main__":
    main()