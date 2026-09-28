# Chapter 2 - Data Types

## 1. Mục tiêu

Hiểu các kiểu dữ liệu cơ bản (String, Integer, Double/Float/Decimal) và kiểu
phức hợp (Array, Struct) trong PySpark; hiểu vì sao ETL cần kiểm soát schema
thủ công thay vì dùng `inferSchema`; quan sát hành vi cast khi dữ liệu sai
định dạng.

## 2. Bối cảnh / khái niệm liên quan

- Basic Data Types, Double/Float/Decimal, Complex Data Types, Casting
  Columns, Semi-Structured Data.
- Dataset `raw_orders_types.csv` gồm dòng sạch và dòng cố tình sai định dạng
  ở amount / order_date / created_at.

## 3. Phương pháp / kỹ thuật áp dụng

- Khai báo `StructType` thủ công (`schema_manual.py`) thay vì `inferSchema`.
- Cast bằng `.cast()`, `to_date()`, `to_timestamp()`, `split()`, `struct()`.
- Đếm `null` sau cast để phát hiện dòng lỗi (`cast_demo.py`).

## 4. Code đã viết

Xem `exercises/00c-data-types/` (`generate_dataset.py`, `schema_manual.py`,
`cast_demo.py`)

## 5. Kết quả chạy thử



## 6. Lỗi gặp phải và cách xử lý



## 7. Kết luận / ghi chú



## 8. Plan cho buổi sau

Chapter 3 - (dự kiến) Transformations & Aggregations nâng cao
