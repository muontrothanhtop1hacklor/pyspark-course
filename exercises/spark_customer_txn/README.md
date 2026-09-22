# PySpark Pipeline: Customer × Transaction

Flow: **raw data → validate → deduplicate → join → window → aggregate → partitioned output**

## 1. Cấu trúc project

```
spark_customer_txn/
├── generate_data.py     # sinh customers.csv / transactions.csv (có cấy lỗi cố tình)
├── pipeline.py           # pipeline chính, chạy: python3 pipeline.py
├── data/
│   ├── customers.csv
│   └── transactions.csv
├── output/
│   ├── valid_transactions_parquet/        (partition theo province)
│   ├── customer_summary_parquet/          (partition theo province)
│   ├── top3_by_province_parquet/          (partition theo province)
│   ├── invalid_transactions_parquet/      (không partition)
│   └── unmapped_transactions_parquet/     (không partition)
└── logs/
    ├── run_output.txt          # log console đầy đủ khi chạy
    └── execution_plans.txt     # explain() của các bước quan trọng
```

## 2. Quy trình đã chạy thực tế 

| Bước | Việc làm | Kết quả thực tế |
|---|---|---|
| 1. Read | Đọc CSV bằng `StructType` khai báo sẵn, **không** `inferSchema` | customers=20, transactions=256 |
| 2. Validate | Tách record thiếu `customer_id` hoặc `amount <= 0` | invalid=15, valid_raw=241 |
| 3. Dedup | `Window` + `row_number()` theo `updated_at` DESC, giữ `rn=1` | 241 → 208 (loại 33 bản cũ) |
| 4. Join | `left join` transactions (đã dedup) với customers | mapped=200, unmapped=8 |
| 5. Window/customer | Tìm transaction gần nhất theo `transaction_time` | dùng cho `customer_summary` |
| 6. Aggregate | `groupBy customer_id` → total_transactions, total_amount, success_transactions | 20 dòng (1 dòng/customer) |
| 7. Top-N | `Window.partitionBy(province).orderBy(total_amount desc)` + `rank() <= 3` | 13 dòng (5 tỉnh × top3, có tỉnh chỉ có 2 customer) |
| 8. Write valid | Ghi Parquet, `partitionBy("province")` | 3 dataset valid |
| 9. Write error | Ghi Parquet riêng, không partition | invalid=15, unmapped=8 |
| 10. Round-trip check | Đọc lại từng output, so `count()` với DataFrame gốc | **TẤT CẢ KHỚP** |

**Reconciliation toàn cục**:
```
transactions_raw (đọc từ CSV)          : 256
  - invalid (missing_cid / amount<=0)  : 15
  - loại bỏ do trùng transaction_id    : 33
= còn lại sau validate + dedup         : 208
    -> mapped (join thành công)        : 200
    -> unmapped (customer_id lạ)       : 8
Reconciliation OK? True
```
256 = 15 (invalid) + 33 (duplicate cũ bị loại) + 200 (mapped) + 8 (unmapped) → khớp tuyệt đối,
chứng minh pipeline không làm rơi hay nhân đôi bản ghi nào ở bất kỳ bước nào.

## 3. Vì sao dùng Window thay vì `dropDuplicates()`

`dropDuplicates(["transaction_id"])` chỉ **giữ lại một bản ghi bất kỳ** trong các bản trùng —
Spark không đảm bảo (và không cho cấu hình) giữ bản nào theo tiêu chí thời gian. Với dữ liệu
transaction có `updated_at` (trạng thái được cập nhật nhiều lần: SUCCESS → FAILED → PENDING
chẳng hạn), nếu `dropDuplicates()` chọn nhầm bản **cũ**, ta sẽ báo cáo sai trạng thái/amount
thật của giao dịch đó — đây là lỗi nghiệp vụ nghiêm trọng (silent wrong data), rất khó phát hiện
vì code chạy không lỗi, chỉ ra kết quả sai.

`Window.partitionBy("transaction_id").orderBy(col("updated_at").desc())` + `row_number() == 1`
cho phép ta **chỉ định rõ ràng tiêu chí "mới nhất"**, và có thể mở rộng tiêu chí tie-break
(ví dụ thêm `.orderBy(desc("updated_at"), desc("transaction_time"))` nếu hai bản có cùng
`updated_at`). Đây là pattern chuẩn cho bài toán **"lấy bản ghi mới nhất của mỗi nhóm"**
(latest-record-per-group / SCD-type xử lý).

Ngoài ra Window row_number còn cho phép audit: ta biết chính xác có bao nhiêu bản bị loại
(`n_before_dedup - n_after_dedup = 33`), điều mà `dropDuplicates()` không tự báo cáo được.

## 4. Vì sao join ở đây dùng `left join` thay vì `inner join`

Yêu cầu bài toán là: *"Tách các transaction không mapping được customer ra một DataFrame
riêng"*. Nếu dùng `inner join`, các transaction có `customer_id` không tồn tại trong
`customers.csv` sẽ **bị Spark âm thầm loại bỏ ngay trong lúc join** — ta sẽ không bao giờ nhìn
thấy chúng để tách ra file lỗi, và tổng số dòng sẽ "biến mất" mà không có cách nào audit lại
(vi phạm nguyên tắc reconciliation ở bước 10).

`left join` giữ **toàn bộ transaction** ở vế trái; với transaction không khớp, các cột bên
customers (`customer_name`, `province`, `created_at`) sẽ là `NULL`. Nhờ vậy ta lọc
`customer_name IS NULL` để tách chính xác dataset "unmapped", đúng yêu cầu đề bài, đồng thời
vẫn đảm bảo được reconciliation 256 = 15 + 33 + 200 + 8.

## 5. Bước nào tốn tài nguyên nhất khi transactions tăng lên vài triệu record?

Xếp theo mức độ rủi ro giảm dần:

1. **Bước 3 — Dedup bằng Window (`partitionBy("transaction_id")`)**: đây là bước nặng nhất.
   Window function bắt buộc Spark phải **shuffle toàn bộ dữ liệu** theo key
   `transaction_id` (thấy rõ trong plan: `Exchange hashpartitioning(transaction_id, ...)`
   ngay trước `Window`), sau đó **sort trong từng partition** theo `updated_at`. Với vài
   triệu record, đây là shuffle + sort trên toàn bộ tập transactions — tốn network I/O,
   disk spill nếu partition lệch (data skew: một `transaction_id` bị update quá nhiều lần
   so với các key khác), và là điểm dễ bị OOM nhất nếu `spark.sql.shuffle.partitions`
   không được tune hợp lý.

2. **Bước 4 — Join transactions × customers**: ở quy mô nhỏ (20 customers) Spark tự động
   chọn `BroadcastHashJoin` (thấy trong plan), rất rẻ vì customers được broadcast toàn bộ
   tới các executor, không cần shuffle transactions. Nhưng nếu customers cũng lớn
   (hàng triệu dòng, vượt ngưỡng `spark.sql.autoBroadcastJoinThreshold`, mặc định 10MB),
   Spark sẽ tự chuyển sang `SortMergeJoin` — lúc đó **cả hai** bên đều phải shuffle + sort
   theo `customer_id`, tốn kém hơn nhiều. Với transactions ở quy mô triệu dòng nhưng
   customers vẫn nhỏ (trường hợp thực tế phổ biến), bước này vẫn rẻ nhờ broadcast.

3. **Bước 5/7 — Window tìm transaction gần nhất / rank top-3 theo province**: cũng shuffle
   (`partitionBy customer_id` / `partitionBy province`), nhưng cardinality của
   `customer_id`/`province` thường nhỏ hơn nhiều so với `transaction_id`, nên chi phí thấp
   hơn bước 3 dù cùng cơ chế.

4. **Bước 8/9 — Ghi Parquet `partitionBy("province")`**: nếu số lượng province ít nhưng data
   nghiêng mạnh (skew) — ví dụ 90% giao dịch ở 1 tỉnh — sẽ sinh ra file Parquet rất to ở
   partition đó và rất nhỏ ở các partition khác (small-file problem ngược), ảnh hưởng hiệu
   năng đọc lại sau này. Cũng cần chú ý số partition Spark trước khi ghi (dùng
   `repartition("province")` hoặc `coalesce` để tránh sinh quá nhiều file nhỏ mỗi partition).

**Tóm lại**: bước 3 (dedup qua Window trên khóa cardinality cao `transaction_id`) là bước
đáng lo nhất khi scale, vì nó bắt buộc full shuffle trên toàn bộ transactions — không có cách
tránh shuffle ở bước này (bản chất bài toán "tìm bản mới nhất mỗi nhóm" luôn cần group theo
key rồi so sánh trong nhóm). Cách giảm nhẹ: tăng `spark.sql.shuffle.partitions` phù hợp với
cluster, cân nhắc `salting` key nếu phát hiện `transaction_id` bị skew nặng do một số ID có
quá nhiều lần update.

## 6. `explain()` — join / shuffle / sort nằm ở đâu (trích từ `logs/execution_plans.txt`)

Ví dụ plan thật của **Bước 4 (Left Join)**, đã bao gồm cả dedup ở nhánh trái:

```
BroadcastHashJoin LeftOuter BuildRight (15)          <-- JOIN (broadcast vì customers nhỏ)
:- Project (11)
:  +- Filter (10)                                     <-- rn == 1 (kết quả dedup)
:     +- Window (9)                                   <-- WINDOW row_number()
:        +- WindowGroupLimit (8)
:           +- Sort (7)                                <-- SORT theo transaction_id, updated_at desc
:              +- Exchange (6)                          <-- SHUFFLE: hashpartitioning(transaction_id, 4)
:                 +- WindowGroupLimit (5)
:                    +- Sort (4)                          <-- Sort cục bộ trước Exchange (partial optimization)
:                       +- Project (3)
:                          +- Filter (2)                    <-- validate: missing_customer_id / amount<=0
:                             +- Scan csv (1)                  <-- đọc transactions.csv theo schema
+- BroadcastExchange (14)                              <-- customers được broadcast (không shuffle)
   +- Filter (13)
      +- Scan csv (12)                                    <-- đọc customers.csv
```

Quan sát chính:
- **`Exchange (6)`** = shuffle thật sự duy nhất trên nhánh transactions, bắt buộc bởi
  `Window.partitionBy("transaction_id")` ở bước dedup.
- **`Sort (4)` và `Sort (7)`** = sort để phục vụ window function (`orderBy(updated_at desc)`),
  xảy ra cả trước và sau Exchange (Spark tối ưu `WindowGroupLimit` thành 2 pha: Partial rồi
  Final, giảm dữ liệu cần shuffle bằng cách giữ top-1 cục bộ trước khi shuffle).
- **`BroadcastHashJoin` + `BroadcastExchange (14)`** = ở bước join, vì customers rất nhỏ
  (20 dòng, dưới ngưỡng broadcast mặc định), Spark Catalyst optimizer **không** chọn
  `SortMergeJoin` (vốn cần shuffle + sort cả 2 bên) mà broadcast toàn bộ customers tới mọi
  executor — join local, không shuffle transactions thêm lần nào nữa.
- Ở **Bước 7** (top-3 theo province), plan cho thấy thêm một `Exchange` +
  `Window`/`WindowGroupLimit` mới trên key `province`, độc lập với shuffle ở bước dedup.

Toàn bộ output `explain(True)` / `explain("formatted")` cho tất cả các bước (3, 4, 5, 6, 7)
nằm trong `logs/execution_plans.txt` (731 dòng) để đối chiếu chi tiết từng operator.

## 7. Cách chạy lại

```bash
cd spark_customer_txn
python3 generate_data.py   # sinh lại data/*.csv (seed cố định = kết quả tái lập được)
python3 pipeline.py        # chạy full pipeline, in log ra console + logs/
```
