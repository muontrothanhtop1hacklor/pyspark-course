# Lab: Flow PySpark hoàn chỉnh — read → clean → validate → dedup → join → transform → aggregate → write → check

**Dữ liệu:** `orders.csv` (3.150 dòng: 3.000 order gốc + 150 bản ghi duplicate mô phỏng update) và `customers.csv` (200 dòng). Dữ liệu bẩn chủ động chèn vào (xem `gen_data.py`):
- 150 `order_id` bị duplicate (bản ghi cập nhật sau có `updated_at` mới hơn, có thể khác `amount`/`status`).
- 150 dòng `amount` null/0/âm.
- 150 dòng `status` viết hoa/thường lộn xộn.
- 150 dòng `order_date` sai format hoặc rỗng.
- 120 dòng `customer_id` không tồn tại trong `customers.csv`.

Toàn bộ code nằm trong `full_etl_lab.py`. Báo cáo này chỉ nêu **kết quả thực tế** và **giải thích**, không lặp lại code.

---

## Yêu cầu 1 — Read + Clean

Khai báo `StructType` riêng cho `orders` (7 cột, `amount: DoubleType`, `order_date`/`updated_at` để `StringType` vì cần tự validate) và cho `customers` (3 cột). Chuẩn hóa `status` bằng `upper(trim(...))`, parse `order_date`/`updated_at` bằng `try_to_date`/`try_to_timestamp` (bắt buộc dùng bản `try_` vì Spark bật ANSI mode mặc định, hàm gốc `to_date`/`to_timestamp` sẽ ném lỗi thay vì trả `null` khi gặp chuỗi sai format).

**Kết quả kiểm tra lỗi trên 3.150 dòng gốc:**

| Tiêu chí | Số dòng vi phạm |
|---|---|
| `amount` null hoặc ≤ 0 | 150 |
| `order_date` không parse được | 150 |
| `updated_at` không parse được | 0 |

---

## Yêu cầu 2 — Deduplicate (Window) + Validate

`Window.partitionBy("order_id").orderBy(updated_at_parsed.desc_nulls_last())` + `row_number() == 1` để giữ đúng bản ghi có `updated_at` mới nhất cho mỗi `order_id` (xem `full_etl_lab.py`, mục "YÊU CẦU 2").

**Kết quả dedup:** 3.150 dòng gốc → **3.000 dòng** sau dedup (đúng bằng số `order_id` phân biệt, 150 bản ghi duplicate thừa đã bị loại).

**Validate + gắn `error_reason`** (thứ tự ưu tiên: `INVALID_AMOUNT` → `INVALID_DATE` → `CUSTOMER_NOT_FOUND`, left-join tạm với `customers` để biết `customer_id` có tồn tại không):

| | Số dòng |
|---|---|
| `valid_orders` (trước join/transform) | 2.580 |
| `invalid_orders` | 420 |
| — INVALID_DATE | 150 |
| — INVALID_AMOUNT | 150 |
| — CUSTOMER_NOT_FOUND | 120 |
| **Tổng** | **3.000** (khớp số dòng sau dedup) |

---

## Yêu cầu 3 — Join + Transform

**Left join** `valid_orders` với `customers` theo `customer_id` để lấy `customer_name`/`customer_type`. Vì `CUSTOMER_NOT_FOUND` đã bị lọc sang `invalid_orders` ở bước trước, **0 dòng** trong `valid_orders` bị null sau join — đúng như kỳ vọng.

**`order_level` theo `amount`** — đề bài không cho ngưỡng cụ thể, mình **tự chọn** cùng mức đã dùng ở các lab trước cho nhất quán: `HIGH ≥ 5.000.000`, `MEDIUM ≥ 1.000.000`, còn lại là `LOW` (nêu rõ đây là giả định, không phải số cố định trong đề).

| order_level | Số dòng |
|---|---|
| HIGH | 1.927 |
| MEDIUM | 523 |
| LOW | 130 |

---

## Yêu cầu 4 — Aggregate: `province_report`

**Giả định thêm:** đề không định nghĩa rõ "success/failed order" — mình chọn `success_orders` = đếm `status = COMPLETED`, `failed_orders` = đếm `status = CANCELLED` (2 trạng thái kết thúc rõ ràng; `PENDING`/`SHIPPING` coi là đang xử lý, không tính vào 2 cột này).

| province | total_orders | total_customers | total_amount | avg_amount | success_orders | failed_orders |
|---|---|---|---|---|---|---|
| CanTho | 421 | 179 | 4,194,462,192 | 9,963,093 | 95 | 97 |
| DaNang | 409 | 177 | 4,122,456,075 | 10,079,355 | 117 | 95 |
| HaNoi | 441 | 178 | 4,545,018,599 | 10,306,165 | 118 | 96 |
| HaiPhong | 461 | 177 | 4,698,132,991 | 10,191,178 | 115 | 114 |
| HoChiMinh | 424 | 173 | 4,196,577,252 | 9,897,588 | 103 | 99 |
| NgheAn | 424 | 177 | 4,236,063,121 | 9,990,715 | 106 | 104 |

---

## Yêu cầu 5 — Write + Check

| Cách ghi `valid_orders` | Số file part-* | Cấu trúc |
|---|---|---|
| Bình thường | 1 | phẳng |
| `partitionBy("province")` | 6 | `province=<value>/part-...parquet`, mỗi tỉnh 1 thư mục |

**Đọc lại & đối chiếu:**

| Kiểm tra | Trước ghi | Sau đọc lại | Khớp? |
|---|---|---|---|
| Count `valid_orders` | 2.580 | 2.580 | ✅ |
| Schema | 10 cột (bao gồm `order_date: date` sau parse, `order_level`, `customer_name`, `customer_type`) | giống hệt | ✅ |
| Tổng `amount` | 25,992,710,230.00 | 25,992,710,230.00 | ✅ |
| Count `province_report` | 6 dòng | 6 dòng | ✅ |

---

## Phần cuối: trả lời tổng kết

**a) Vì sao dùng `Window` thay vì `dropDuplicates()`?**
`dropDuplicates()` chỉ giữ **một bản ghi bất kỳ** trong các bản ghi trùng — không có cách nào chỉ định "giữ bản mới nhất theo `updated_at`" bằng riêng nó. `Window.partitionBy("order_id").orderBy(updated_at.desc())` cho phép **xếp hạng** các bản ghi trùng theo tiêu chí mong muốn (ở đây là thời gian cập nhật), rồi chỉ giữ hạng 1 (`row_number() == 1`) — đây là cách duy nhất để dedup **có chọn lọc** theo một quy tắc nghiệp vụ cụ thể, thay vì dedup ngẫu nhiên.

**b) Vì sao dùng left join?**
Vì mục tiêu là **giữ lại toàn bộ order**, kể cả những order không tìm được customer tương ứng — để còn có thể phát hiện và xử lý (`CUSTOMER_NOT_FOUND`). Nếu dùng `inner join`, các order có `customer_id` không tồn tại sẽ **biến mất âm thầm** khỏi kết quả mà không có cách nào biết được, sai với mục tiêu validate dữ liệu của bài toán.

**c) `partitionBy` dùng để làm gì?**
Tổ chức lại output thành cấu trúc thư mục theo giá trị cột (`province=<value>/`), giúp khi đọc lại với điều kiện lọc theo cột đó, engine chỉ cần đọc đúng thư mục liên quan (partition pruning) thay vì quét toàn bộ dữ liệu — hữu ích khi các job sau thường xuyên lọc/xử lý riêng theo tỉnh.

**d) Nếu `amount`/`order_date` sai thì nên xử lý như thế nào trong ETL?**
Không nên âm thầm loại bỏ hay sửa liều dữ liệu sai. Cách làm chuẩn (đã áp dụng trong lab): **tách riêng** các bản ghi lỗi sang vùng `invalid_orders`/quarantine kèm `error_reason` rõ ràng, để đội vận hành/nghiệp vụ có thể tra cứu, đối chiếu lại nguồn, hoặc sửa tay — vẫn giữ nguyên **toàn vẹn số lượng** dữ liệu (không có dòng nào "mất tích" trong quá trình xử lý), thay vì `filter` bỏ luôn không dấu vết.

**e) Trong bài này bước nào có khả năng gây shuffle?**
- `Window.partitionBy("order_id").orderBy(...)` ở bước dedup — Spark phải **shuffle dữ liệu theo `order_id`** để gom các bản ghi cùng `order_id` vào cùng partition trước khi xếp hạng.
- `join` (left join `orders` với `customers`, và join tạm để check `customer_id` tồn tại) — join thường kéo theo shuffle cả 2 bên theo khóa join, trừ khi 1 bên đủ nhỏ để Spark tự động dùng **broadcast join** (ở đây `customers` chỉ 200 dòng, nhiều khả năng Spark đã tự broadcast thay vì shuffle).
- `groupBy("province")` ở bước aggregate — luôn shuffle theo khóa `province` để gom nhóm trước khi tính `count`/`sum`/`avg`.
- `countDistinct("customer_id")` bên trong `groupBy` — cũng cần shuffle thêm để loại trùng trước khi đếm.

---

## Output

```
output/
  valid_orders/                 # Parquet, ghi bình thường
  valid_orders_partitioned/     # Parquet, partitionBy("province")
  invalid_orders/               # Parquet, có cột error_reason
  province_report/              # Parquet, report tổng hợp theo tỉnh
```

## Phụ lục: cách chạy lại

```bash
pip install pyspark --break-system-packages
python3 gen_data.py        # sinh orders.csv (3,150 dòng) + customers.csv (200 dòng)
python3 full_etl_lab.py    # chạy toàn bộ pipeline 5 yêu cầu, in log + ghi output/
```
