# Join – Orders x Customers (PySpark)

Bài thực hành về **join trong PySpark**: kết hợp `orders` với `customers`, so sánh inner join và left join, và tách riêng các bản ghi không mapping được để kiểm tra. `orders.csv` **không có bản sao** trong thư mục này — script đọc thẳng file thật của [bài 01-orders-aggregation](../01-orders-aggregation/README.md) qua đường dẫn tương đối; chỉ `customers.csv` là dữ liệu mới của bài này.

> Mục tiêu chính: hiểu join và biết vì sao ETL thường phải giữ lại dữ liệu không mapping được để kiểm tra.

## 1. Mục tiêu

- Tạo hai DataFrame `orders` và `customers`.
- Thực hành `inner join` và `left join` theo `customer_id`.
- Tạo trường hợp `order` không tìm thấy `customer` tương ứng.
- Tách `mapped`/`unmapped` records từ kết quả left join.
- So sánh kết quả inner join và left join.

## 2. Cấu trúc

```text
exercises/
├── 00d-joins/
│   ├── README.md              ← file này
│   ├── generate_customers.py  ← sinh customers.csv
│   ├── customers.csv          ← 5 khách hàng (dữ liệu mới)
│   ├── schema_manual.py       ← schema thủ công cho 2 bảng
│   └── join_demo.py           ← đọc orders.csv từ bài 01 qua đường dẫn tương đối
└── 01-orders-aggregation/
    └── orders.csv              ← không đổi, dùng chung cho cả 2 bài
```

`join_demo.py` trỏ tới file gốc bằng:

```python
ORDERS_PATH = PROJECT_DIR.parent / "01-orders-aggregation" / "orders.csv"
```

Vì vậy bài này **bắt buộc phải nằm đúng vị trí** `exercises/00d-joins/`, ngang hàng với `exercises/01-orders-aggregation/` — nếu di chuyển thư mục đi nơi khác, đường dẫn tương đối sẽ sai.

## 3. Dữ liệu

**`orders.csv`** (8 dòng, đọc trực tiếp từ bài 01, không copy): `order_id`, `customer_id`, `province`, `amount`, `status`, `order_date`. Các `customer_id` xuất hiện: `C001`–`C007` (`C001` xuất hiện 2 lần).

**`customers.csv`** (5 dòng, dữ liệu mới): `customer_id`, `customer_name`, `email`, `customer_since`. **Cố tình không tạo** `C005` và `C007` — hai mã này có thật trong `orders.csv` nhưng không có trong `customers.csv`, tạo ra tình huống "order không tìm thấy customer" **bằng dữ liệu thật**, không cần chèn dòng giả:

| `order_id` | `customer_id` | Vì sao không mapping được |
|---|---|---|
| `1006` | `C005` | `customers.csv` không có `C005` |
| `1008` | `C007` | `customers.csv` không có `C007` |

> Lưu ý: `ORDER_SCHEMA` trong `spark_orders_exercise.py` (bài 01) chỉ khai báo 5 field, thiếu `order_date` — Spark đọc thiếu cột mà không báo lỗi. Ở bài này, `ORDERS_SCHEMA` khai báo đủ 6 cột để không làm mất `order_date` khi đọc.

## 4. Script chính: `join_demo.py`

1. Đọc `customers.csv` và `orders.csv` (file thật của bài 01) với schema thủ công (`schema_manual.py`).
2. `orders.join(customers, on="customer_id", how="inner")` — chỉ giữ đơn có `customer_id` khớp.
3. `orders.join(customers, on="customer_id", how="left")` — giữ **toàn bộ** `orders`; cột của `customers` là `NULL` nếu không khớp.
4. Từ kết quả left join: lọc `customer_name IS NOT NULL` → `mapped_df`; `customer_name IS NULL` → `unmapped_df`.
5. So sánh số dòng: `orders` gốc, inner join, left join, mapped, unmapped — kèm `assert` để tự kiểm tra logic.
6. Spark SQL đối chiếu: liệt kê các `order_id` chỉ xuất hiện ở left join mà không có ở inner join.

## 5. Yêu cầu môi trường

- Python 3.11 (khuyến nghị), Java/JDK, PySpark

```powershell
pip install pyspark
```

## 6. Cách chạy

```powershell
cd .\exercises\00d-joins
python .\generate_customers.py
python .\join_demo.py
```

Không cần copy `orders.csv` — script tự đọc từ `..\01-orders-aggregation\orders.csv`. Chỉ cần thư mục `01-orders-aggregation` vẫn còn tồn tại cạnh `00d-joins`.

## 7. Kết quả mong đợi

```text
orders goc       : 8
inner join       : 6
left join        : 8
mapped (trong left)   : 6
unmapped (trong left) : 2
```

Inner join **mất 2 dòng** (`1006`, `1008`) vì không tìm thấy `customer_id` khớp. Left join **giữ đủ 8 dòng**, 2 dòng unmapped có các cột `customer_name`/`email`/`customer_since` là `NULL`.

## 8. Bài học rút ra

- Inner join âm thầm **loại bỏ** những dòng không khớp — nếu chỉ dùng inner join để báo cáo, 2 đơn hàng thật sẽ biến mất khỏi kết quả mà không có cảnh báo nào.
- Left join giữ nguyên số dòng của bảng gốc (bên trái), nên là lựa chọn an toàn hơn khi cần **không được làm mất dữ liệu nguồn**.
- Tách riêng `unmapped_df` biến "dữ liệu bị mất" thành "danh sách cần kiểm tra": có thể log lại, báo cho đội vận hành, hoặc đối chiếu thủ công — ở đây có thể là do `C005`/`C007` chưa được đồng bộ sang hệ thống CRM.
- `orders gốc = left join`, `inner join = mapped` là 2 bất biến (invariant) hữu ích để tự kiểm tra logic join có đúng không.

## 9. Troubleshooting

| Vấn đề | Cách xử lý |
|---|---|
| Thiếu Java / `JAVA_HOME` | Cài JDK và set `JAVA_HOME` |
| Spark không tìm thấy `python3` | Đã xử lý sẵn qua `PYSPARK_PYTHON`; kiểm tra `sys.executable` đúng môi trường ảo |
| Lỗi không tìm thấy `orders.csv` | Kiểm tra bài `01-orders-aggregation/orders.csv` còn tồn tại và đúng vị trí tương đối (`../01-orders-aggregation/`) so với `00d-joins/` |
| Join ra nhiều cột `customer_id` trùng tên | Dùng `on="customer_id"` (chuỗi) thay vì điều kiện `==` để Spark tự gộp cột join, tránh cột trùng |
