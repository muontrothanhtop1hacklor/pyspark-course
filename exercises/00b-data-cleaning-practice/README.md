# 00b – Thực hành làm sạch dữ liệu cơ bản (Data Cleaning Practice)

Bài thực hành nhanh về các phương thức làm sạch dữ liệu cơ bản trên DataFrame API của PySpark: xử lý giá trị khuyết (`null`, `NaN`), lọc theo điều kiện số học và loại bỏ các bản ghi không hợp lệ.

---

## 1. Mục tiêu

- Làm quen với các hàm xử lý dữ liệu null/khuyết: `df.na.drop(subset=...)`, `df.na.fill(...)`.
- Lọc điều kiện với `.where()` và `.between()`.
- Nhận biết sự khác biệt giữa `None` (SQL NULL) và `float('nan')` (Not a Number trong chuẩn IEEE floating-point).

---

## 2. Cấu trúc thư mục

```text
00b-data-cleaning-practice/
├── README.md               ← Tài liệu hướng dẫn này
└── prac.py                 ← Script thực hành làm sạch dữ liệu
```

---

## 3. Nội dung thực hành (`prac.py`)

Tạo một DataFrame nhỏ có sẵn các trường hợp lỗi dữ liệu thường gặp:
- `name` bị `None`
- `height` bị `NaN` hoặc `None`
- `height` bị ngoại lai (`1802.3` - bất thường)
- `age` bị `None`

Quy trình làm sạch:
1. `df.na.drop(subset="name")`: Loại bỏ mọi bản ghi có cột `name` bị null/rỗng.
2. `df2.where(df2.height.between(65, 85))`: Lọc các bản ghi có chiều cao trong khoảng hợp lý từ 65 đến 85.

---

## 4. Cách chạy

```powershell
python exercises/00b-data-cleaning-practice/prac.py
```

---

## 5. Kết quả mong đợi

Chỉ các bản ghi có tên hợp lệ và chiều cao trong khoảng `[65, 85]` được giữ lại:

```text
+-----+------+---+
| name|height|age|
+-----+------+---+
|Alice|  80.1|  5|
| josh|  78.9|  9|
|jerry|  75.3|  7|
+-----+------+---+
```
