# PySpark orders exercise

Bài thực hành này minh họa flow:

1. Đọc `orders.csv` thành DataFrame với schema tường minh.
2. Xem schema và dữ liệu bằng `printSchema()` và `show()`.
3. Chọn cột, lọc đơn `status = SUCCESS`, rồi `groupBy` theo `province`.
4. Tạo temporary view và chạy SQL tính tổng `amount` theo tỉnh.
5. Ghi bảng tổng hợp đơn thành công ra thư mục CSV dạng Spark output.

## Chạy

Yêu cầu PySpark 4.0.1 (hoặc tương thích), Java và Hadoop native helper
`winutils.exe` trên Windows. Đặt `HADOOP_HOME` trỏ tới thư mục Hadoop có
`bin\winutils.exe` trước khi chạy:

```powershell
cd C:\Users\Administrator\minio-nessie\iceburg_test_project
python -m pip install pyspark==4.0.1
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
python .\spark_orders_exercise.py
```

Kết quả được ghi vào `output\orders_by_province`. Đây là một thư mục chứa
các part-file CSV do Spark tạo ra, không phải một file CSV đơn duy nhất.

Nếu chỉ muốn chạy trên Linux/macOS thì không cần bước `HADOOP_HOME`; chạy
script sau khi cài PySpark là đủ.
