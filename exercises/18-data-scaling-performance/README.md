# Bài 18: Đánh giá hiệu năng mở rộng với tài nguyên giới hạn (Low Memory)

Mục tiêu của bài thực hành này là chạy hệ thống ETL (Bronze -> Silver -> Gold) trên 3 quy mô dữ liệu: **1 Triệu (1M)**, **10 Triệu (10M)**, và **100 Triệu (100M)** dòng. 

Tuy nhiên, thay vì tối ưu hóa tài nguyên RAM để chạy nhanh nhất có thể (như ở Bài 17), chúng ta đã **giới hạn RAM nghiêm ngặt** để nhường tài nguyên cho máy tính thực hiện đa tác vụ (Multitasking).

## 1. Cấu hình Tài Nguyên Giới Hạn

Code được thiết kế trong file `etl_orders_low_mem.py` nhằm mục đích "ép" PySpark phải liên tục đọc/ghi ổ cứng (Spill to Disk) thay vì dùng RAM:

| Quy mô dữ liệu | Kích thước file | RAM cho phép | Số lượng Partitions |
|---|---|---|---|
| **1m** | ~ 40MB | `1g` (Giảm từ 4g) | 10 |
| **10m** | ~ 400MB | `1g` (Giảm từ 4g) | 50 |
| **100m** | ~ 4GB | `2g` (Giảm từ 8g) | 200 |

*Cấu hình Spark được sử dụng:*
```python
spark = SparkSession.builder \
    .appName(f"Medallion_ETL_ORDERS_{scale}_LOW_MEM") \
    .config("spark.driver.memory", memory) \
    .config("spark.executor.memory", memory) \
    .config("spark.sql.shuffle.partitions", partitions) \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.3") \
    .getOrCreate()
```

## 2. Kết quả Thực Thi

- **Tập 1M:**
  - **Thời gian chạy:** ~ 31.68 giây.
  - **Nhận xét:** Với dữ liệu 1 triệu dòng, việc cấp phát RAM thấp (`1g`) gần như không ảnh hưởng. Dữ liệu vẫn được load vào RAM một cách thoải mái.

- **Tập 10M & 100M:** *(Đang thực thi ngầm)*
  - Khi chạy tới 10M và 100M, với bộ nhớ cực kỳ eo hẹp, Spark sẽ liên tục báo log cấu hình lại kích thước file nhóm (`Scaling row group sizes...`) hoặc cảnh báo thiếu bộ nhớ.
  - Bù lại, hệ điều hành (Windows) vẫn hoạt động trơn tru. Bạn vẫn có thể lướt web, làm việc trên các công cụ khác một cách bình thường!

## 3. Cách chạy lại (Reproduction)
Nếu bạn muốn tự chạy kiểm tra độc lập, mở terminal trong thư mục `18-data-scaling-performance` và chạy lệnh:

```bash
# Định dạng log Unicode cho Tiếng Việt
$env:PYTHONIOENCODING="utf-8"

# Chạy cho tập 1 Triệu
python etl_orders_low_mem.py --scale 1m

# Chạy cho tập 10 Triệu
python etl_orders_low_mem.py --scale 10m

# Chạy cho tập 100 Triệu
python etl_orders_low_mem.py --scale 100m
```
