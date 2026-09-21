# Chapter 2 – Data Types & Schema Control (PySpark)

Bài thực hành về **kiểu dữ liệu trong PySpark**: khai báo schema thủ công, cast từ `String` sang đúng kiểu, và quan sát chuyện gì xảy ra khi dữ liệu sai định dạng.

> Mục tiêu chính: hiểu datatype và vì sao ETL cần kiểm soát schema. Dataset dùng cùng kiểu dữ liệu đơn hàng với [bài 01-orders-aggregation](../01-orders-aggregation/README.md) (6 cột đầu giống `orders.csv`).

## 1. Mục tiêu

- Tạo dataset có đủ `String`, `Integer`, `Decimal`, `Date`, `Timestamp`, `Array`, `Struct`.
- Khai báo schema **thủ công** (`StructType`) thay vì `inferSchema`.
- Cast `order_id`, `amount`, `quantity`, `order_date`, `created_at` từ `String` sang đúng kiểu.
- Cố tình tạo dữ liệu sai để xem kết quả khi cast.
- So sánh `Double`/`Float`/`Decimal` và hành vi cast khi bật/tắt ANSI mode.

## 2. Cấu trúc

```text
00c-data-types/
├── README.md              ← file này
├── generate_dataset.py    ← sinh raw_orders_types.csv
├── raw_orders_types.csv   ← dữ liệu thô (11 dòng, mọi cột là text)
├── schema_manual.py       ← schema thủ công (toàn String)
└── cast_demo.py           ← script chính: cast + báo cáo lỗi
```

## 3. Dữ liệu: `raw_orders_types.csv`

11 dòng: 6 dòng sạch (`2001`–`2006`) và 5 dòng lỗi (`2007`–`2011`).

| Cột | Kiểu sau cast |
|---|---|
| `order_id` | `Integer` |
| `customer_id`, `province`, `status` | `String` |
| `amount` | `Decimal(12,2)` (giống `ORDER_SCHEMA` bài 01) |
| `order_date` | `Date` (`yyyy-MM-dd`) |
| `quantity` | `Integer` |
| `created_at` | `Timestamp` (`yyyy-MM-dd HH:mm:ss`) |
| `tags` | `Array<String>` (tách theo dấu `,`) |
| `street`, `district`, `zip_code` | gộp thành `Struct` `shipping_address` |

Các dòng lỗi:

| `order_id` | Lỗi | Kết quả |
|---|---|---|
| `2007` | `amount = "abc"` | cast ra `NULL` |
| `2008` | `order_date = "32/13/2026"` | cast ra `NULL` |
| `2009` | `created_at` rỗng | `NULL` ngay từ lúc đọc CSV (**không phải lỗi cast**) |
| `2010` | `amount = -50000.00` | cast **thành công** nhưng sai nghiệp vụ |
| `2011` | `quantity = "five"` | cast ra `NULL` |

## 4. Schema thủ công thay vì `inferSchema`

`schema_manual.py` khai báo toàn bộ cột là `String` (tầng raw giữ nguyên dữ liệu gốc), việc ép kiểu làm tường minh ở bước sau. Nếu dùng `inferSchema` trên file này, Spark đoán: `zip_code` → `integer` (mất số 0 đầu nếu có), `created_at` → `timestamp`, còn `amount`/`quantity`/`order_date` → `string` vì dính dòng lỗi. Kết quả **phụ thuộc vào nội dung dữ liệu**, không ổn định giữa các lần chạy.

> Lưu ý: `nullable=False` trong schema bị Spark bỏ qua khi đọc file (`printSchema()` vẫn hiện `nullable = true`).

## 5. Script chính: `cast_demo.py`

1. Tạo `SparkSession` (`getOrCreate()`), đọc CSV với `RAW_ORDERS_SCHEMA`, in schema trước cast.
2. Cast `order_id` → `int`, `amount` → `decimal(12,2)`, `quantity` → `int`, `order_date` → `to_date`, `created_at` → `to_timestamp`.
3. `tags` → `split()` thành `Array`; `street/district/zip_code` → `struct()`.
4. In schema và dữ liệu sau cast.
5. Báo cáo lỗi: đếm `NULL` sau cast, và đếm **lỗi cast thật sự** (giá trị gốc có nhưng cast ra `NULL`) bằng cách so sánh raw với casted.
6. Liệt kê dòng `NULL` từ nguồn và dòng cast đúng nhưng `amount < 0`.
7. Demo `Double` vs `Decimal`, và ANSI mode `true` (ném exception) vs `false` + `try_cast` (trả `NULL`).
8. `spark.stop()` trong `finally`.

Script đặt `spark.sql.ansi.enabled=false` để cast lỗi trả `NULL` thay vì làm sập job.

## 6. Yêu cầu môi trường

- Python 3.11 (khuyến nghị), Java/JDK, PySpark

```powershell
pip install pyspark
```

## 7. Cách chạy

```powershell
cd .\exercises\00c-data-types
python .\generate_dataset.py
python .\cast_demo.py
```

## 8. Kết quả mong đợi

Schema sau cast:

```text
 |-- order_id: integer
 |-- customer_id: string
 |-- province: string
 |-- amount: decimal(12,2)
 |-- status: string
 |-- order_date: date
 |-- quantity: integer
 |-- created_at: timestamp
 |-- tags: array<string>
 |-- shipping_address: struct<street, district, zip_code>
```

Số lỗi cast thật sự (`2009` không tính vì đã `NULL` từ nguồn):

```text
+--------+------+--------+----------+----------+
|order_id|amount|quantity|order_date|created_at|
+--------+------+--------+----------+----------+
|       0|     1|       1|         1|         0|
+--------+------+--------+----------+----------+
```

Double vs Decimal:

```text
double_sum = 0.30000000000000004    decimal_sum = 0.30
```

## 9. Bài học rút ra

- Cast lỗi **không báo lỗi**, chỉ âm thầm ra `NULL` (khi ANSI tắt) → phải chủ động kiểm tra.
- `NULL` sau cast có 2 nguồn: `NULL` sẵn từ dữ liệu gốc, hoặc cast thất bại. So sánh raw với casted mới phân biệt được.
- Đúng kiểu ≠ đúng nghiệp vụ (`2010`): cần thêm bước validate riêng.
- Tiền tệ dùng `Decimal`, không dùng `Double`/`Float` vì sai số dấu phẩy động.
- Phạm vi bài này chưa gồm `Map` và JSON (`from_json`).

## 10. Troubleshooting

| Vấn đề | Cách xử lý |
|---|---|
| Thiếu Java / `JAVA_HOME` | Cài JDK và set `JAVA_HOME` |
| Spark không tìm thấy `python3` | Đã xử lý sẵn qua `PYSPARK_PYTHON`; kiểm tra `sys.executable` đúng môi trường ảo |
| CSV không tồn tại | Chạy `generate_dataset.py` trước |
| Cast lỗi ném exception thay vì `NULL` | ANSI đang bật; đặt `spark.sql.ansi.enabled=false` hoặc dùng `try_cast` |