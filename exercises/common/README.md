# Common Data Contract & Unified Schemas

Thư mục dùng chung (shared module) chuẩn hóa toàn bộ cấu trúc dữ liệu, quy ước cột, tên tỉnh thành và quy tắc nghiệp vụ cho tất cả các bài tập PySpark trong dự án.

---

## 1. Cấu trúc thư mục `common/`

```text
exercises/common/
├── __init__.py           ← Export các schema, hàm chuẩn hóa và rules
├── schemas.py            ← Định nghĩa PySpark StructType (Orders, Customers)
├── provinces.py          ← 10 tỉnh thành chuẩn (PascalCase) + từ điển chuẩn hóa biến thể
├── rules.py              ← Các ngưỡng nghiệp vụ (VIP, STANDARD, BASIC) và trạng thái chuẩn
├── data_generator.py     ← Tiện ích sinh dữ liệu Orders/Customers thống nhất
├── data/
│   └── customers_master.csv  ← Dataset 200 khách hàng chuẩn dùng chung
└── README.md             ← Tài liệu hướng dẫn này
```

---

## 2. Chuẩn hóa Schema cốt lõi

### 2.1. Orders (Fact Table)
Gồm 6 cột nền tảng (và 1 cột mở rộng `updated_at` cho các bài nâng cao):

| Cột | Kiểu Raw (Bronze) | Kiểu Valid (Silver/Gold) | Ghi chú |
|---|---|---|---|
| `order_id` | `StringType` | `IntegerType` | Khóa chính đơn hàng |
| `customer_id` | `StringType` | `StringType` | Khóa ngoại trỏ sang khách hàng (`C0001`...) |
| `province` | `StringType` | `StringType` | Tỉnh/thành giao dịch (chỉ nhận 10 tỉnh chuẩn) |
| `amount` | `StringType` | `DecimalType(12, 2)` / `Double` | Số tiền đơn hàng |
| `status` | `StringType` | `StringType` | `SUCCESS`, `COMPLETED`, `PENDING`, `SHIPPING`, `FAILED`, `CANCELLED` |
| `order_date` | `StringType` | `DateType` | Ngày đặt hàng (`yyyy-MM-dd`) |
| `updated_at` | `StringType` | `TimestampType` | Mốc cập nhật (phục vụ dedup Window trong bài 06, 09) |

### 2.2. Customers (Dimension Table)

| Cột | Kiểu dữ liệu | Ý nghĩa |
|---|---|---|
| `customer_id` | `StringType` | Mã định danh khách hàng |
| `customer_name` | `StringType` | Tên khách hàng (chuẩn hóa Title Case) |
| `province` | `StringType` | Tỉnh/thành phố cư trú |
| `customer_type` | `StringType` | Phân hạng: `REGULAR`, `VIP`, `NEW` |
| `created_at` | `TimestampType` | Ngày giờ tạo tài khoản |

---

## 3. Bản đồ dùng chung dữ liệu giữa các bài tập

```text
[common/data/customers_master.csv]
       │
       ├──► 05-joins/ (đọc subset khách hàng để test inner/left join)
       ├──► 06-customer-transactions/ (join với transactions)
       └──► 09-full-etl-pipeline/ (enrich thông tin khách hàng trong ETL flow)

[01-orders-aggregation/orders.csv]
       │
       └──► 05-joins/ (dùng chung trực tiếp orders.csv qua relative path)

[common/provinces.py & rules.py]
       │
       ├──► 02-orders-lakehouse (chuẩn hóa tỉnh thành và status ở tầng Silver)
       ├──► 07-partitioning-performance (phân bổ partition theo province)
       ├──► 08-read-write-advance (quarantine các record sai province/amount)
       └──► 10-udf-pandas-udtf (phân loại VIP / STANDARD / BASIC theo rules)
```

---

## 4. Cách sử dụng trong các script bài tập

```python
import sys
from pathlib import Path

# Thêm đường dẫn tới exercises vào sys.path nếu cần
COMMON_DIR = Path(__file__).resolve().parent.parent / "common"
sys.path.append(str(COMMON_DIR.parent))

from common.schemas import VALID_ORDER_SCHEMA, CUSTOMER_SCHEMA
from common.provinces import normalize_province, CANONICAL_PROVINCES
from common.rules import classify_segment
```
