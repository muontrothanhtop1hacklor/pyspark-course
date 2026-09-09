# PySpark Course — Orders Exercise

Bài thực hành PySpark: tạo/đọc dữ liệu đơn hàng, xử lý bằng DataFrame API & Spark SQL, ghi kết quả ra CSV.

Tài liệu tham khảo chính: [PySpark User Guide (Spark 4.0.1)](https://spark.apache.org/docs/4.0.1/api/python/user_guide/index.html)
- Chapter 1 — DataFrames: Create, View, Manipulation
- Chapter 3 — Function Junction: Clean / Transform / Summarize data
- Chapter 6 — Old SQL, New Tricks: Running SQL with PySpark
- Chapter 7 — Load and Behold: Reading & Writing Data

## Mục tiêu

Hiểu flow cơ bản: **đọc/tạo dữ liệu → xử lý bằng Spark → ghi kết quả**.

## Cấu trúc project

```
pyspark-course/
├── spark_orders_exercise.py   # Script chính
├── orders.csv                 # Dữ liệu đầu vào
├── output/
│   └── orders_by_province/    # Kết quả ghi ra (Spark tự tạo nhiều file, xem mục "Lỗi 2")
├── pyspark_test/
├── tests/
├── pyproject.toml
├── dev-requirements.txt
└── README.md
```

## Yêu cầu môi trường

- Python (khuyến nghị dùng qua **conda/venv riêng**, không dùng Python hệ thống)
- Java 8/11/17 (Spark cần JDK, không chạy được nếu chỉ có JRE hoặc thiếu `JAVA_HOME`)
- PySpark (cài trong `dev-requirements.txt`)

Cài đặt nhanh:

```bash
conda create -n pyspark_env python=3.10 -y
conda activate pyspark_env
pip install -r dev-requirements.txt
```

## Cách chạy

```bash
python spark_orders_exercise.py
```

Script sẽ:
1. Tạo `SparkSession`
2. Tạo DataFrame mock + đọc `orders.csv`, gộp lại bằng `unionByName`
3. In schema, show dữ liệu, select cột, filter `status = SUCCESS`
4. Group by `province`, tính `count` order và `sum amount`
5. Tạo temp view, chạy SQL tương đương
6. Ghi kết quả ra `output/orders_by_province` (CSV)

---

## Các lỗi đã gặp & cách khắc phục

### Lỗi 1 — Môi trường: driver và worker dùng Python khác nhau

**Triệu chứng:** Lỗi kiểu `Python in worker has different version than that in driver`, hoặc PySpark báo không tìm thấy `python`/`python3`, hoặc treo/crash khi gọi các action như `.show()`, `.collect()`.

**Nguyên nhân:** Trên máy có nhiều Python (Python hệ thống, conda base, conda env riêng...). Khi không chỉ định rõ, Spark driver và các worker process có thể dùng 2 bản Python khác nhau (khác version, khác vị trí cài), dẫn đến xung đột khi serialize/deserialize dữ liệu giữa các tiến trình.

**Giải pháp:** Set biến môi trường `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON` trỏ **cùng một** interpreter (chính là Python trong conda env đang dùng để chạy PySpark), **trước khi** tạo `SparkSession`:

```python
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Administrator\miniconda3\envs\pyspark_env\python.exe"
```

Lưu ý:
- Phải set **trước** dòng `SparkSession.builder...getOrCreate()`, vì Spark đọc các biến này ngay lúc khởi tạo session.
- Đường dẫn phải trỏ đúng tới `python.exe` bên trong `envs/pyspark_env` (không phải Python hệ thống hay conda base) — dùng `where python` (Windows) hoặc `which python` (macOS/Linux) sau khi `conda activate pyspark_env` để lấy đường dẫn chính xác.
- Nếu chuyển máy/chuyển user, đường dẫn hardcode này sẽ sai — nên cân nhắc dùng biến động thay vì hardcode tuyệt đối (xem phần "Cải tiến đề xuất" bên dưới).

### Lỗi 2 — Chỗ lưu file: tưởng ghi ra 1 file CSV nhưng lại ra cả thư mục

**Triệu chứng:** Sau khi chạy `df.write.csv(path)`, thư mục `output/orders_by_province/` không phải là 1 file `.csv` mà là một **folder** chứa nhiều file lạ:

```
output/orders_by_province/
├── _SUCCESS
├── _SUCCESS.crc
├── part-00000-xxxxxxxx.csv
├── .part-00000-xxxxxxxx.csv.crc
```

**Nguyên nhân:** Đây là hành vi bình thường của Spark, không phải lỗi:
- Spark xử lý dữ liệu phân tán theo **partition**, mỗi partition ghi ra một file `part-XXXXX...`.
- File `_SUCCESS` (rỗng) là marker báo job ghi thành công.
- Các file `.crc` là checksum ẩn do Hadoop's local filesystem client tạo ra để kiểm tra tính toàn vẹn dữ liệu.
- Vì dữ liệu bài tập chỉ có ít partition/dữ liệu nhỏ nên nhìn có vẻ "thừa file", nhưng với dữ liệu lớn thật sự đây chính là cách Spark ghi song song hiệu quả.

**Giải pháp / lưu ý:**
- Nếu muốn output là **đúng 1 file CSV** để dễ xem/gửi đi, gọi `.coalesce(1)` (hoặc `.repartition(1)`) trước khi `.write`:
  ```python
  province_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_path))
  ```
  rồi tự đổi tên file `part-00000-*.csv` thành tên mong muốn (Spark không cho đặt tên file output trực tiếp).
- Không nên commit các file `_SUCCESS`, `.crc`, `part-*` vào Git — nên thêm `output/` vào `.gitignore`.
- Tương tự, thư mục `__pycache__/` (chứa file `.pyc` biên dịch sẵn) cũng **không nên commit** — hiện đang bị đẩy nhầm lên repo. Thêm vào `.gitignore`:
  ```
  __pycache__/
  *.pyc
  output/
  .venv/
  ```

### Lỗi 3 — Vị trí đặt `import os` và set biến môi trường

**Triệu chứng:** Set `os.environ["PYSPARK_PYTHON"]` nhưng vẫn bị lỗi Python mismatch như Lỗi 1.

**Nguyên nhân:** Thứ tự code sai — nếu `SparkSession.builder.getOrCreate()` được gọi **trước** khi set `os.environ`, hoặc nếu Spark context đã được khởi tạo từ trước đó trong cùng tiến trình (ví dụ chạy trong notebook, chạy lại cell nhiều lần), thì việc set biến môi trường sau đó sẽ **không có tác dụng** vì Spark JVM/worker process đã được fork với biến môi trường cũ.

**Giải pháp:** Đảm bảo thứ tự trong file luôn là:
1. `import os`
2. Set toàn bộ `os.environ[...]` cần thiết
3. Sau đó mới `import pyspark` / tạo `SparkSession`

Đây cũng là lý do trong `spark_orders_exercise.py`, khối set `PYSPARK_PYTHON` được đặt ngay sau phần `import`, ở đầu file, trước khi `main()` chạy và tạo `SparkSession`.

---

## Cải tiến đề xuất (chưa bắt buộc, để tham khảo thêm)

- Thay vì hardcode đường dẫn Python tuyệt đối, có thể lấy tự động bằng `sys.executable`:
  ```python
  import sys
  os.environ["PYSPARK_PYTHON"] = sys.executable
  os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
  ```
  Cách này tự động đúng theo interpreter đang chạy script, không phụ thuộc máy/user cụ thể.
- Thêm file `.gitignore` để tránh commit nhầm `__pycache__/`, `output/`, file `.crc`.
- Có thể thử ghi output ra Parquet (`.parquet` thay vì `.csv`) để so sánh, vì Parquet là định dạng cột (columnar), giữ nguyên kiểu dữ liệu (ví dụ `DecimalType`), nén tốt hơn — đúng với phần Chapter 7 (Reading & Writing Data) của tài liệu.

