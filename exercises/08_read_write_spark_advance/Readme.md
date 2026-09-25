# Lab: Schema tự khai báo — Validate — Write (overwrite/append/partitionBy) — Đọc lại

**Dữ liệu:** `orders_dirty.csv` — 2.000 dòng, cột `order_id, customer_id, province, amount, status, order_date`. Chủ động chèn dữ liệu bẩn (xem `gen_dirty_orders.py`):
- `amount`: sai kiểu (`"N/A"`), rỗng, hoặc âm.
- `order_date`: sai format (`31/02/2025`), rỗng, hoặc ngày không tồn tại (`2025-13-40`).
- `province`: rỗng.
- `status`: viết hoa/thường không đồng nhất (`PENDING`/`pending`/`Pending`...).

Toàn bộ code nằm trong `schema_validate_lab.py`; báo cáo này chỉ trình bày **kết quả thực tế** và **giải thích**.

---

## Yêu cầu 1 — Đọc dữ liệu với schema tự khai báo

Khai báo `StructType` với 6 `StructField` tương ứng 6 cột (`amount: DoubleType`, `order_date` để `StringType` vì cần tự validate format ở bước sau, không để Spark tự parse thành DateType).

**Schema sau khi đọc (không dùng inferSchema):**
```
order_id: integer, customer_id: integer, province: string,
amount: double, status: string, order_date: string
```

**Count:** 2.000 dòng.

**Số dòng null theo cột:**

| order_id | customer_id | province | amount | status | order_date |
|---|---|---|---|---|---|
| 0 | 0 | 80 | 66 | 0 | 37 |

→ `amount` bị null ở 66 dòng: đây chính là các dòng có giá trị gốc là `"N/A"` hoặc rỗng trong CSV — vì cột được khai báo `DoubleType`, Spark **tự động chuyển thành `null`** thay vì raise lỗi hoặc để nguyên chuỗi.

**So sánh với `inferSchema=True`:** Spark đoán `amount` là **`string`** (không phải `double`) — vì trong cột có lẫn giá trị text `"N/A"`, Spark chọn kiểu rộng nhất bao trùm được mọi giá trị (string), khiến toàn bộ cột `amount` mất khả năng tính toán số học nếu không ép kiểu lại thủ công sau đó.

### Giải thích

**`inferSchema` khác schema tự khai báo như thế nào:** `inferSchema=True` khiến Spark phải **đọc qua dữ liệu một lượt** chỉ để đoán kiểu cho từng cột trước khi đọc thật — tốn thêm chi phí I/O, và kiểu đoán được **phụ thuộc vào chính nội dung dữ liệu** (nếu dữ liệu có lẫn giá trị bẩn, kiểu bị đoán sai như ví dụ `amount` ở trên). Khai báo `StructType` thủ công thì không cần pass dò-kiểu, và **ép chặt kiểu mong muốn** — giá trị nào không đúng kiểu sẽ tự thành `null` thay vì làm sai lệch kiểu cả cột.

**Tại sao ETL thực tế thường cần kiểm soát schema:** dữ liệu nguồn hiếm khi sạch 100% và cấu trúc có thể đổi bất ngờ (thêm/bớt cột, đổi kiểu). Khai báo schema tường minh giúp: phát hiện lỗi **sớm** ngay lúc đọc thay vì lỗi ngầm ở bước tính toán sau; pipeline **ổn định**, không phụ thuộc Spark đoán đúng mỗi lần chạy; và **tài liệu hoá** rõ cấu trúc dữ liệu kỳ vọng cho người review/bảo trì sau này.

---

## Yêu cầu 2 — Chuẩn hóa & validate, tách valid/invalid

Chuẩn hóa `status` về uppercase (`F.upper(F.trim(...))`), thử parse `order_date` bằng `try_to_date(order_date, 'yyyy-MM-dd')` (dùng `try_to_date` thay vì `to_date` vì Spark bật ANSI mode mặc định — `to_date` với chuỗi sai format sẽ **ném lỗi** thay vì trả `null`), rồi gắn `error_reason` theo thứ tự ưu tiên: `INVALID_AMOUNT` → `INVALID_DATE` → `MISSING_PROVINCE` (xem hàm tạo `df_flagged` trong `schema_validate_lab.py`, mục "YÊU CẦU 2").

**Kết quả:**

| | Số dòng |
|---|---|
| `valid_orders` | 1.734 |
| `invalid_orders` | 266 |
| **Tổng** | **2.000** (khớp với count gốc) |

**Phân bố lỗi trong `invalid_orders`:**

| error_reason | Số dòng |
|---|---|
| INVALID_AMOUNT | 100 |
| INVALID_DATE | 95 |
| MISSING_PROVINCE | 71 |

(Tổng 266 = 100 + 95 + 71 — không có dòng nào bị đếm 2 lần vì dùng `when/otherwise` theo thứ tự ưu tiên, mỗi dòng chỉ nhận đúng 1 lý do lỗi kể cả khi vi phạm nhiều điều kiện cùng lúc.)

---

## Yêu cầu 3 — Ghi Parquet: bình thường vs `partitionBy("province")`

| Cách ghi | Số file part-* | Cấu trúc |
|---|---|---|
| Bình thường | **1** | phẳng — `valid_orders/part-00000...parquet` |
| `partitionBy("province")` | **6** | `valid_orders_partitioned/province=<value>/part-...parquet`, mỗi tỉnh 1 thư mục riêng |

*(Ghi bình thường chỉ ra 1 file vì `valid_orders` sau các phép `filter`/`withColumn` gói gọn trong 1 partition ở quy mô 1.734 dòng — không liên quan đến `partitionBy`.)*

### Giải thích khác biệt

Ghi bình thường dồn hết dữ liệu vào các file phẳng ngang hàng, các tỉnh có thể lẫn lộn trong cùng 1 file tuỳ theo cách chia partition trong bộ nhớ lúc ghi — muốn lọc theo 1 tỉnh, engine đọc phải quét gần như toàn bộ file rồi mới lọc.

`partitionBy("province")` tạo **cấu trúc thư mục Hive-style**: mỗi tỉnh có thư mục riêng `province=<value>/`. Khi query có `WHERE province = 'HaNoi'`, Spark/Hive/Presto áp dụng được **partition pruning** — chỉ đọc đúng thư mục đó, không cần quét các tỉnh khác, nhanh hơn nhiều trên dữ liệu lớn.

---

## Yêu cầu 4 — Append vs Overwrite

| Bước | Thao tác | Count sau đó |
|---|---|---|
| [1] | Ghi lần đầu bằng `overwrite` | 1.734 |
| [2] | Ghi thêm 3 order mới bằng `append` | **1.737** (= 1.734 + 3, đúng như kỳ vọng) |
| [3] | Ghi lại `valid_orders` (dữ liệu gốc, KHÔNG có 3 order mới) bằng `overwrite` | **1.734** |

Sau bước [3], kiểm tra riêng 3 order mới (`order_id >= 90001`): **0 dòng còn tồn tại**.

**Kết luận:** `append` cộng dồn dữ liệu mới vào dữ liệu đã có ở đường dẫn đó. `overwrite` **xoá sạch toàn bộ nội dung cũ** của đường dẫn rồi ghi đè bằng dữ liệu mới — 3 order thêm ở bước [2] bị mất hoàn toàn sau bước [3] vì `overwrite` không quan tâm dữ liệu cũ là gì, chỉ ghi đúng những gì DataFrame hiện tại có.

---

## Yêu cầu 5 — Đọc lại & validate output

| Kiểm tra | Trước khi ghi | Sau khi đọc lại Parquet | Khớp? |
|---|---|---|---|
| Schema | 6 cột đúng kiểu đã khai báo | **giống hệt** (Parquet lưu kèm schema, không cần khai báo lại khi đọc) | ✅ |
| Total count | 1.734 | 1.734 | ✅ |
| Count theo tỉnh | (6 tỉnh) | **giống hệt**, 0 dòng lệch khi đối chiếu bằng `exceptAll` 2 chiều | ✅ |
| Tổng `amount` theo tỉnh | vd HaNoi: 731,855,528 | HaNoi: 731,855,528 | ✅ — lệch lớn nhất đo được: **0.0** |

Toàn bộ số liệu khớp tuyệt đối giữa trước khi ghi và sau khi đọc lại — đúng như kỳ vọng vì Parquet là định dạng **lưu kèm schema và không mất mát dữ liệu** (columnar, có nén nhưng không suy hao số liệu).

---

## Cuối bài: trả lời tổng kết

**a) `append` khác `overwrite` như thế nào?**
`append` **cộng thêm** dữ liệu mới vào những gì đã có sẵn ở đường dẫn ghi — dữ liệu cũ được giữ nguyên. `overwrite` **xoá toàn bộ nội dung cũ** của đường dẫn đó rồi ghi lại từ đầu bằng đúng nội dung của DataFrame hiện tại — bất kỳ dữ liệu nào không nằm trong DataFrame đang ghi sẽ **biến mất vĩnh viễn**, kể cả dữ liệu được thêm bằng `append` trước đó (đã thấy rõ ở Yêu cầu 4).

**b) `partitionBy` khi write dùng để làm gì?**
Dùng để **tổ chức lại cấu trúc thư mục output** theo giá trị của (các) cột chỉ định — mỗi giá trị riêng biệt được lưu trong 1 thư mục con dạng `col=value/`. Mục đích chính là phục vụ **partition pruning** khi đọc lại: engine chỉ cần đọc đúng thư mục ứng với điều kiện lọc, không phải quét toàn bộ dữ liệu, giúp truy vấn theo cột đó nhanh hơn đáng kể trên dữ liệu lớn.

**c) Schema tự khai báo có lợi gì?**
Kiểm soát chặt kiểu dữ liệu ngay từ lúc đọc (giá trị sai kiểu tự thành `null` thay vì làm hỏng kiểu cả cột hoặc gây lỗi ngầm về sau), không tốn thêm 1 lượt đọc để dò kiểu như `inferSchema`, giúp pipeline ổn định và dễ kiểm soát hơn khi dữ liệu nguồn có thể thay đổi hoặc không sạch — đây là lý do các pipeline ETL thực tế gần như luôn khai báo schema tường minh thay vì dựa vào suy đoán tự động.

**d) Nếu `partitionBy` một cột có quá nhiều giá trị khác nhau (high cardinality) thì có vấn đề gì?**
Sẽ sinh ra **quá nhiều thư mục con**, mỗi thư mục có thể chỉ chứa vài dòng dữ liệu → tạo ra rất nhiều file nhỏ (vấn đề "small files" đã thấy ở lab partition trước) — gây tốn kém quản lý metadata, chậm khi liệt kê thư mục, và làm giảm hiệu quả đọc do overhead mở/đóng nhiều file nhỏ. Nguyên tắc chung: chỉ nên `partitionBy` những cột có **số lượng giá trị phân biệt vừa phải** (vài chục đến vài trăm, như `province`, `year`, `month`), tránh các cột như `customer_id` hay timestamp chi tiết đến từng giây.

**e) Tại sao sau khi Spark write thường thấy nhiều file part-* thay vì một file duy nhất?**
Vì Spark xử lý dữ liệu phân tán theo nhiều **partition song song** — mỗi partition trong bộ nhớ được một task ghi ra **đúng 1 file** độc lập, không có bước "gộp lại thành 1 file" mặc định (gộp sẽ mất khả năng ghi song song, chậm hơn với dữ liệu lớn). Số file part-* sinh ra vì vậy luôn phản ánh đúng số partition tại thời điểm ghi — muốn giảm số file phải chủ động `coalesce()`/`repartition()` trước khi `write`.

---

## Output

```
output/
  valid_orders/                # Parquet, ghi bình thường
  valid_orders_partitioned/    # Parquet, partitionBy("province")
  invalid_orders/              # Parquet, có thêm cột error_reason
```

## Phụ lục: cách chạy lại

```bash
pip install pyspark --break-system-packages
python3 gen_dirty_orders.py       # sinh orders_dirty.csv (2,000 dòng, có dữ liệu bẩn)
python3 schema_validate_lab.py    # chạy toàn bộ 5 yêu cầu, in log + ghi output/
```
