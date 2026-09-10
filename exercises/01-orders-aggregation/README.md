# 01 – Orders Aggregation (DataFrame API & Spark SQL)

Bài thực hành trọng tâm về **DataFrame API** và **Spark SQL**: đọc dữ liệu đơn hàng, kết hợp với dữ liệu mock, lọc, group và tính toán aggregation theo tỉnh/thành (`province`).

> Bài này chỉ tập trung vào DataFrame API và Spark SQL cơ bản. Nếu bạn muốn xem flow Bronze/Silver/Gold và upload MinIO, xem [README của bài 02-orders-lakehouse](../02-orders-lakehouse/README.md).

## 1. Mục tiêu

- Tạo DataFrame từ dữ liệu mock trong code và từ file CSV, sau đó gộp lại bằng `unionByName`.
- Xem schema và dữ liệu bằng `printSchema()` / `show()`.
- Chọn cột với `select()`, lọc dữ liệu với điều kiện `status = SUCCESS`.
- Group theo `province`, tính `order_count` và `total_amount`.
- Tạo temporary view và chạy truy vấn tương đương bằng Spark SQL.
- Ghi kết quả ra thư mục output.

## 2. Cấu trúc

```text
01-orders-aggregation/
├── .gitignore
├── README.md                     ← file này
├── dev-requirements.txt
├── orders.csv
├── pyproject.toml
├── spark_orders_exercise.py
├── pyspark_test/
│   ├── __init__.py
│   └── __main__.py
└── tests/
    ├── .gitignore
    └── test_iceburg_test.py
```

## 3. Dữ liệu: `orders.csv`

`orders.csv` hiện là **dữ liệu mẫu đơn giản và hợp lệ** (8 dòng), không chứa record lỗi hay giá trị cần làm sạch. Các cột:

| Cột | Mô tả |
|---|---|
| `order_id` | Mã đơn hàng |
| `customer_id` | Mã khách hàng |
| `province` | Tỉnh/thành của đơn hàng |
| `amount` | Giá trị đơn hàng |
| `status` | Trạng thái đơn hàng (ví dụ `SUCCESS`) |
| `order_date` | Ngày đặt hàng |

Ngoài dữ liệu đọc từ file, script còn tạo thêm một DataFrame mock ngay trong code với cùng schema, rồi gộp hai DataFrame bằng `unionByName` để có bộ dữ liệu đầy đủ hơn trước khi xử lý.

## 4. Script chính: `spark_orders_exercise.py`

Script thực hiện tuần tự các bước sau:

1. Tạo `SparkSession`.
2. Tạo một DataFrame mock từ dữ liệu khai báo trong code.
3. Đọc `orders.csv`.
4. Gộp hai DataFrame bằng `unionByName`.
5. In schema bằng `printSchema()`.
6. Hiển thị dữ liệu bằng `show()`.
7. Chọn một số cột bằng `select()`.
8. Lọc các đơn có `status = SUCCESS`.
9. Group theo `province`.
10. Tính `order_count` và `total_amount` cho mỗi province.
11. Tạo temporary view tên `orders`.
12. Chạy Spark SQL để tính tổng `amount` theo `province` (tương đương với bước DataFrame API ở trên, dùng để đối chiếu hai cách viết).
13. Ghi kết quả ra `output/orders_by_province/`.

Script tự cấu hình Python interpreter cho Spark bằng:

```python
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
```

Nhờ vậy Spark sẽ dùng đúng Python đang chạy trong môi trường ảo hiện tại, tránh lỗi Spark tìm `python3` không thấy trên Windows.

## 5. Yêu cầu môi trường

- Python 3.11 (khuyến nghị, cùng môi trường Conda/venv của toàn project)
- Java/JDK
- PySpark
- pytest (để chạy test trong thư mục `tests/`)

Cài đặt (nếu chưa cài ở root):

```powershell
pip install pyspark pytest
```

Hoặc dùng `dev-requirements.txt` có sẵn trong thư mục này:

```powershell
pip install -r dev-requirements.txt
```

## 6. Cách chạy

Chạy đúng từ thư mục của bài này:

```powershell
cd .\exercises\01-orders-aggregation
python .\spark_orders_exercise.py
```

Chạy test (nếu có):

```powershell
pytest
```

## 7. Kết quả mong đợi

Sau khi chạy, bạn sẽ thấy:

- Schema và dữ liệu được in ra console qua `printSchema()`/`show()`.
- Kết quả group theo `province` (từ cả DataFrame API và Spark SQL) hiển thị trên console.
- Một thư mục output mới:

```text
output/orders_by_province/
├── _SUCCESS
├── part-00000-....csv
└── .part-*.csv.crc
```

Đây là cách Spark ghi dữ liệu: kết quả nằm trong **một thư mục**, không phải một file CSV duy nhất — số lượng `part-*.csv` phụ thuộc vào số partition lúc ghi.

`output/`, `__pycache__/` và các file `.pyc` không nên commit (đã có trong `.gitignore`).

## 8. Troubleshooting

| Vấn đề | Cách xử lý |
|---|---|
| Thiếu Java / `JAVA_HOME` | Cài JDK và set `JAVA_HOME` |
| Test không chạy được | `pip install pytest` |
| Lỗi Hadoop khi ghi file trên Windows | Cài `winutils.exe`, set `HADOOP_HOME` và thêm vào `Path` (xem README ở root) |
| Spark không tìm thấy `python3` | Đã được xử lý sẵn qua `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON`; nếu vẫn lỗi, kiểm tra `sys.executable` có trỏ đúng môi trường ảo đang active |
