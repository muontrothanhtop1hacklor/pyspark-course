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

## Các lỗi gặp phải khi chạy Spark và kinh nghiệm rút ra

**1. `Python worker failed to connect back` / Windows gợi ý tải Python từ Microsoft Store**
Windows chặn lệnh `python` bằng "App Execution Alias" giả. → Tắt ở Settings → Apps → Advanced app settings → App execution aliases, và khai báo rõ interpreter trong code, **trước khi** tạo `SparkSession`:
```python
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

**2. `Cannot run program "python3": ... cannot find the file specified`**
Windows/Conda chỉ có `python.exe`, không có `python3.exe`, nhưng Spark mặc định tìm `python3` kiểu Linux/Mac. → Set `PYSPARK_PYTHON` như trên (không phải do App Execution Alias nên tắt nó không giải quyết được lỗi này).

**3. `HADOOP_HOME and hadoop.home.dir are unset` khi `.write().csv()`**
Đọc/`.show()` thì không sao, nhưng ghi file ra ổ đĩa Windows thì Spark cần `winutils.exe` để giả lập filesystem kiểu Hadoop. → Tải `winutils.exe` + `hadoop.dll` từ `github.com/cdarlint/winutils`, bỏ vào `C:\...\hadoop\bin\`, set `HADOOP_HOME` và thêm `%HADOOP_HOME%\bin` vào `Path`, rồi mở lại terminal.

**4. `ModuleNotFoundError: No module named 'pyspark'`**
Terminal đang ở env `base` chứ chưa activate đúng env cài pyspark. → `conda activate pyspark_env` rồi `pip install pyspark`.

---

## Lưu ý về thư mục output

Khi ghi CSV, Spark không tạo 1 file duy nhất mà tạo cả folder gồm `_SUCCESS`, các file `.crc` (checksum) và `part-00000-*.csv` (mỗi partition ghi 1 file) — đây là hành vi bình thường, không phải lỗi. Nếu muốn ra đúng 1 file CSV, gọi `.coalesce(1)` trước khi `.write`. Nên thêm `.gitignore` để không commit nhầm các file này và `__pycache__/`:

```
__pycache__/
*.pyc
output/
.venv/
```

