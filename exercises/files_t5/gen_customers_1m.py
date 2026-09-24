import csv
import random

random.seed(7)

provinces = ["HaNoi", "HoChiMinh", "DaNang", "CanTho", "HaiPhong",
             "NgheAn", "ThanhHoa", "BinhDuong", "DongNai", "KhanhHoa"]

first_names = ["nguyen", "tran", "le", "pham", "hoang", "vu", "dang", "bui",
               "do", "ngo", "duong", "ly", "trinh", "cao", "ta", "luu", "phan", "vo"]
middle = ["van", "thi", "huu", "ngoc", "minh", "duc", "thanh", "quoc"]
last_names = ["an", "binh", "cuong", "dung", "em", "phuong", "giang", "hoa",
              "ich", "khanh", "long", "mai", "nam", "oanh", "phuc", "quyen",
              "son", "tam", "uyen", "viet", "xuan", "yen"]

notes_pool = ["", "khach quen", "moi dang ky", "can cham soc", "VIP tiem nang", ""]

N = 1_000_000

with open("customers_1m.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["customer_id", "customer_name", "province", "amount", "note"])
    for i in range(1, N + 1):
        fn = random.choice(first_names)
        mn = random.choice(middle)
        ln = random.choice(last_names)
        # cố ý tạo khoảng trắng thừa + hoa/thường lộn xộn ngẫu nhiên để UDF chuẩn hóa có ý nghĩa
        raw = f"{fn} {mn} {ln}"
        style = random.randint(0, 4)
        if style == 0:
            name = f"  {raw} "
        elif style == 1:
            name = raw.upper()
        elif style == 2:
            name = f"{fn}  {mn}   {ln}"  # nhiều khoảng trắng giữa
        elif style == 3:
            name = raw.title() + " "
        else:
            name = raw

        prov = random.choice(provinces)
        bucket = random.random()
        if bucket < 0.5:
            amount = random.randint(200_000, 900_000)       # BASIC
        elif bucket < 0.85:
            amount = random.randint(1_000_000, 4_500_000)   # STANDARD
        else:
            amount = random.randint(5_000_000, 20_000_000)  # VIP
        note = random.choice(notes_pool)
        w.writerow([i, name, prov, amount, note])

print(f"Generated customers_1m.csv with {N} rows")
