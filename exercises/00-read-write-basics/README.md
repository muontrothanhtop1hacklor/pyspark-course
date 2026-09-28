# 00 – Đọc và ghi dữ liệu cơ bản (Read & Write Basics)

Bài thực hành nhập môn về **đọc và ghi dữ liệu trong PySpark**: làm quen với SparkSession, đọc file CSV và JSON, hiểu schema suy diễn (`inferSchema`), và quan sát cơ chế ghi dữ liệu phân tán dạng thư mục part-file của Spark.

---

## 1. Mục tiêu

- Khởi tạo `SparkSession` trong môi trường local.
- Đọc file CSV (`employee.csv`) với các option `header=true`, `inferSchema=true`.
- Đọc file JSON (`employees.json`) với option `multiline=true`.
- Hiểu vì sao Spark ghi dữ liệu ra **thư mục** chứa các file `part-*` và `_SUCCESS` thay vì 1 file đơn lẻ.
- Thực hành ghi dữ liệu ra định dạng **Parquet** và **CSV**.

---

## 2. Cấu trúc thư mục

```text
00-read-write-basics/
├── README.md               ← Tài liệu hướng dẫn này
├── employee.csv            ← File CSV mẫu (thông tin nhân viên)
├── employees.json          ← File JSON mẫu (dạng array of objects)
├── read_write_demo.py      ← Script PySpark thực hành đọc & ghi
└── output/                 ← Thư mục chứa kết quả do Spark ghi ra (tự sinh)
```

---

## 3. Dữ liệu mẫu

### `employee.csv`
Gồm 7 dòng nhân viên:
```csv
Employee ID,Role,Location
19238,Data Analyst,"Seattle, WA"
19239,Software Engineer,"Seattle, WA"
19240,IT Specialist,"Seattle, WA"
...
```

### `employees.json`
Định dạng JSON mảng (cần `option("multiline", "true")` khi đọc bằng PySpark):
```json
[
  {
    "employee_id": 19238,
    "role": "Data Analyst",
    "location": "Seattle, WA"
  },
  ...
]
```

---

## 4. Cách chạy

Kích hoạt môi trường conda PySpark và chạy:

```powershell
python exercises/00-read-write-basics/read_write_demo.py
```

---

## 5. Kiến thức cốt lõi cần nhớ

1. **Vì sao Spark ghi ra thư mục chứa nhiều file `part-*`?**
   - Spark là hệ thống tính toán phân tán. Dữ liệu được chia thành nhiều partition.
   - Mỗi task (trên mỗi core/worker) ghi partition của mình ra đĩa một cách độc lập và đồng thời, tạo ra file `part-00000-...`.
   - File `_SUCCESS` (0 bytes) là tín hiệu cam kết (commit flag): nếu có file này nghĩa là toàn bộ job đã hoàn tất an toàn. Nếu job bị crash giữa chừng, `_SUCCESS` sẽ không được tạo.

2. **So sánh CSV và Parquet:**
   - **CSV:** Dạng text, không lưu schema (kiểu dữ liệu), không nén hiệu quả, tốn chi phí parse chuỗi khi đọc.
   - **Parquet:** Dạng cột (columnar format), lưu sẵn schema và metadata thống kê (min/max), nén cực tốt (Snappy), hỗ trợ predicate pushdown (chỉ đọc các cột và dòng cần thiết).
