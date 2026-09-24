# Lab: UDF, Pandas UDF, UDTF trong PySpark vs Built-in Function

**Dữ liệu:** `customers_1m.csv` — **1.000.000 dòng**, cột: `customer_id, customer_name, province, amount, note`. Tên khách hàng được cố ý tạo có khoảng trắng thừa và viết hoa/thường lộn xộn (`"  duong huu nam  "`, `"TRAN THANH BINH"`...) để thấy rõ tác dụng của bước chuẩn hóa. Dữ liệu lớn giúp đo được **hiệu năng thực tế** giữa UDF/built-in/Pandas UDF — điều mà tập dữ liệu nhỏ (vài chục dòng) không thể hiện rõ vì overhead cố định của Spark (khởi tạo job, lập kế hoạch...) lấn át thời gian xử lý dữ liệu thật.

**Cách đo:** dùng `.write.parquet(...)` (ghi toàn bộ ra đĩa) thay vì `.show(10)` để ép Spark tính **toàn bộ** 1 triệu dòng — `.show(10)` chỉ tính vừa đủ 10 dòng đầu do Spark tối ưu `LIMIT`, không phản ánh đúng hiệu năng thật khi dữ liệu lớn.

---

## Yêu cầu 1 — Python UDF

Viết 2 hàm Python thuần `clean_name()` và `segment_customer()`, bọc bằng `udf(...)` thành `clean_name_udf`, `segment_udf` (xem `udf_lab_1m.py`, mục "YÊU CẦU 1").

**Kết quả (5 dòng đầu trong 1.000.000 dòng):**

| customer_id | customer_name | customer_name_clean | amount | customer_segment |
|---|---|---|---|---|
| 1 | `  duong huu nam  ` | `Duong Huu Nam` | 1394810 | STANDARD |
| 2 | `  tran ngoc binh ` | `Tran Ngoc Binh` | 452353 | BASIC |
| 3 | `vo thanh binh` | `Vo Thanh Binh` | 15580147 | VIP |
| 4 | `TRAN THANH BINH  ` | `Tran Thanh Binh` | 1558575 | STANDARD |
| 5 | `  cao huu tam    ` | `Cao Huu Tam` | 389505 | BASIC |

UDF hoạt động đúng **theo từng dòng** (row-by-row): Spark serialize từng giá trị, gửi sang tiến trình Python để chạy hàm `clean_name`/`segment_customer`, rồi nhận kết quả trả về.

**Thời gian xử lý toàn bộ 1.000.000 dòng (ghi ra Parquet): `9.21s`**

---

## Yêu cầu 2 — Built-in Spark function

Viết lại cùng logic bằng `F.initcap(F.trim(F.regexp_replace(...)))` cho tên và `F.when/.otherwise` cho phân loại (xem hàm tạo `df_builtin` trong `udf_lab_1m.py`, mục "YÊU CẦU 2"):

- `regexp_replace(col, r"\s+", " ")` gộp mọi chuỗi khoảng trắng liên tiếp thành 1 dấu cách.
- `trim(...)` cắt khoảng trắng 2 đầu.
- `initcap(...)` viết hoa chữ cái đầu mỗi từ (tương đương `.title()` trong Python).
- `when/otherwise` thay cho if/elif/else.

**Thời gian xử lý toàn bộ 1.000.000 dòng (ghi ra Parquet): `3.98s`**

**Đối chiếu kết quả:** chạy `df_udf.exceptAll(df_builtin)` trên đúng 3 cột so sánh, **toàn bộ 1 triệu dòng** → **0 dòng khác nhau**. Hai cách cho ra kết quả giống hệt nhau.

### So sánh UDF vs built-in

| Tiêu chí | Python UDF | Built-in function |
|---|---|---|
| Query plan | `ArrowEvalPython` — Spark coi UDF là **hộp đen**, không tối ưu được | `Project` với biểu thức `CASE WHEN...` — Catalyst **thấy rõ logic**, có thể tối ưu (predicate pushdown, constant folding...) |
| Hiệu năng (đo thực tế trên 1 triệu dòng) | **9.21s** — chậm hơn ~2.3 lần | **3.98s** |
| Vì sao chậm hơn | Mỗi dòng phải serialize/deserialize qua lại giữa JVM và tiến trình Python (qua Arrow theo lô nhưng vẫn gọi hàm Python cho từng dòng) | Chạy thẳng trong JVM, không có bước chuyển dữ liệu sang tiến trình khác |
| Độ dễ đọc | Quen thuộc với người biết Python thuần, dễ viết logic phức tạp | Cần biết API của Spark SQL functions, nhưng ngắn gọn hơn cho logic đơn giản |
| Khả năng tái sử dụng trong SQL thuần | Phải đăng ký (`spark.udf.register`) mới gọi được trong SQL string | Dùng được ngay trong cả DataFrame API và SQL |

**Kết quả benchmark thực tế trên 1.000.000 dòng:**

| Cách làm | Thời gian (full-pass, ghi Parquet) |
|---|---|
| Python UDF | 9.21s |
| Built-in function | 3.98s |
| Chênh lệch | UDF chậm hơn **~2.31 lần** |

Với chỉ 60 dòng (bản trước), chênh lệch này gần như không đo được vì overhead cố định (khởi động JVM, lập kế hoạch truy vấn...) chiếm phần lớn thời gian. Ở quy mô 1 triệu dòng, chi phí serialize từng dòng qua Python của UDF mới thực sự lộ rõ — và với dữ liệu càng lớn (hàng chục/hàng trăm triệu dòng như trong ETL BHXH thật), khoảng cách này còn giãn ra nhiều hơn nữa.

**Physical plan thực tế (trích từ `explain()`, chạy trên tập 1 triệu dòng đã cache):** UDF hiện ra là `ArrowEvalPython [segment_customer(amount)]`, còn built-in là `Project [CASE WHEN (amount >= 5000000) THEN VIP WHEN ... END]` (log đầy đủ nằm cuối `udf_lab_1m.py` khi chạy).

Built-in trở thành 1 `Project` đơn giản mà Catalyst optimizer hiểu và có thể tối ưu tiếp (ví dụ kết hợp với các phép biến đổi khác, đẩy filter xuống sớm hơn). UDF luôn hiện ra như một "hộp đen" `ArrowEvalPython` — Catalyst không biết bên trong làm gì nên **không tối ưu được**.

**Cách nào dễ đọc hơn?** Với logic đơn giản (chuẩn hóa chuỗi, phân loại theo ngưỡng) thì **built-in dễ đọc hơn** vì súc tích và là ngôn ngữ khai báo (declarative) quen thuộc với dân SQL. UDF dễ đọc hơn khi logic **phức tạp, nhiều nhánh, cần gọi thư viện Python khác** (regex phức tạp, xử lý ngày tháng đặc thù, gọi model ML...).

**Khi nào nên ưu tiên built-in:** luôn ưu tiên built-in **trừ khi** built-in không biểu diễn được logic cần thiết. Vì built-in luôn nhanh hơn UDF (được biên dịch/tối ưu bởi Catalyst, chạy trong JVM) và tận dụng được toàn bộ hạ tầng tối ưu hoá của Spark.

---

## Yêu cầu 3 — Pandas UDF

Hàm `amount_with_surcharge(amount: pd.Series) -> pd.Series` được đánh dấu `@pandas_udf(DoubleType())`, nhận cả một `Series` (nhiều dòng cùng lúc) và trả về `amount * 1.05` bằng phép toán vector, không lặp từng phần tử (xem `udf_lab_1m.py`, mục "YÊU CẦU 3").

**Kết quả (5 dòng đầu trong 1.000.000 dòng):**

| customer_id | amount | amount_with_fee |
|---|---|---|
| 1 | 1394810 | 1464550.50 |
| 3 | 15580147 | 16359154.35 |
| 5 | 389505 | 408980.25 |

**Thời gian xử lý toàn bộ 1.000.000 dòng (ghi ra Parquet): `3.11s`** — nhanh hơn cả built-in trong lần đo này (phép tính ở đây đơn giản hơn — chỉ nhân 1 cột số — nên chưa thể hiện hết chi phí runtime cố định của Pandas UDF; điểm mấu chốt là **nhanh hơn hẳn Python UDF thông thường**, gần bằng tốc độ built-in dù logic được viết bằng Python).

**Bảng benchmark tổng hợp trên 1.000.000 dòng:**

| Cách làm | Thời gian (full-pass, ghi Parquet) | So với Python UDF |
|---|---|---|
| Python UDF | 9.21s | — |
| Built-in function | 3.98s | nhanh hơn ~2.3 lần |
| Pandas UDF | 3.11s | nhanh hơn ~3 lần |

(Lưu ý: Pandas UDF ở đây tính công thức khác cột amount đơn giản hơn segment logic của UDF/built-in nên chỉ mang tính **tham khảo tương đối** về cùng cấp độ overhead, không phải so sánh 1-1 cùng một phép tính — nhưng đủ để thấy xu hướng rõ ràng: Pandas UDF luôn nhanh hơn nhiều so với Python UDF thông thường.)

### So sánh Python UDF vs Pandas UDF

| Tiêu chí | Python UDF | Pandas UDF |
|---|---|---|
| Đơn vị xử lý | **Từng dòng một** — hàm nhận 1 giá trị scalar, gọi lại N lần cho N dòng | **Theo lô (batch/vector)** — hàm nhận cả 1 `pandas.Series` (nhiều dòng cùng lúc), trả về 1 `Series` |
| Cơ chế truyền dữ liệu JVM ↔ Python | Serialize/deserialize **từng dòng** qua pickle | Dùng **Apache Arrow** để chuyển cả khối dữ liệu (columnar) một lần, giảm overhead nhiều |
| Hiệu năng | Chậm với dữ liệu lớn (overhead cộng dồn theo từng dòng) | Nhanh hơn đáng kể — tận dụng được các phép toán vector hoá của NumPy/pandas bên trong hàm |
| Cách viết logic | Viết như hàm Python thuần trên 1 giá trị | Viết như đang thao tác trên `pandas.Series`/`DataFrame` (dùng phép toán vector: `amount * 1.05` thay vì loop từng phần tử) |
| Yêu cầu thư viện | Không cần thêm gì | Cần `pyarrow` cài sẵn trên cluster |

**Vì sao Pandas UDF phù hợp hơn khi xử lý theo batch/vector:** vì nó giảm được chi phí giao tiếp giữa JVM và tiến trình Python — thay vì gọi hàm Python và serialize dữ liệu **N lần** (N = số dòng), Spark gom một lô dữ liệu (ví dụ vài nghìn dòng) thành 1 `pandas.Series`, chuyển qua Arrow **một lần**, hàm Python xử lý bằng các phép toán vector hoá (rất nhanh vì chạy trên C/NumPy bên dưới) rồi trả nguyên khối kết quả về — số lần "qua lại" giữa hai bên giảm đi rất nhiều so với Python UDF thông thường.

---

## Yêu cầu 4 — UDTF (User Defined Table Function)

`SplitTags` là 1 class đánh dấu `@udtf(returnType="tag: string")`, có phương thức `eval()` dùng `yield` để trả nhiều dòng cho 1 lần gọi; đăng ký bằng `spark.udtf.register(...)` rồi gọi qua `LATERAL split_tags(tags)` trong SQL (xem `udf_lab_1m.py`, mục "YÊU CẦU 4").

**Kết quả:**

| customer_id | tag |
|---|---|
| 1 | spark |
| 1 | python |
| 1 | etl |
| 2 | sql |
| 2 | airflow |
| 3 | spark |
| 3 | scala |
| 3 | kafka |
| 3 | delta |

Đúng như yêu cầu: mỗi dòng `customer_id | tags` (1 chuỗi) được tách thành **nhiều dòng** `customer_id | tag` (1 giá trị/dòng).

**Lưu ý:** với case đơn giản như tách chuỗi bằng dấu phẩy, Spark có sẵn cách làm tương đương **không cần UDTF**: `df_tags.select("customer_id", F.explode(F.split("tags", ",")).alias("tag"))` — cho kết quả giống hệt. UDTF chỉ thực sự cần thiết khi logic sinh nhiều dòng **phức tạp hơn** những gì `explode`/`flatten`/`inline` built-in hỗ trợ được (ví dụ: parse một cấu trúc dữ liệu phức tạp, gọi API ngoài cho mỗi dòng rồi trả về nhiều kết quả, logic sinh dòng có điều kiện phức tạp...).

### Điểm khác nhau cơ bản giữa UDF và UDTF

| | UDF | UDTF |
|---|---|---|
| Input | 1 hoặc nhiều giá trị (từ 1 dòng) | 1 hoặc nhiều giá trị (từ 1 dòng, hoặc cả bảng khi dùng dạng table-argument) |
| Output | **Đúng 1 giá trị** cho mỗi lần gọi (1 dòng vào → 1 giá trị ra) | **0, 1, hoặc nhiều dòng** cho mỗi lần gọi (1 dòng vào → N dòng ra) — dùng `yield`/`return` nhiều lần |
| Dùng trong SQL | Xuất hiện trong `SELECT col, my_udf(col) ...` | Phải dùng với `LATERAL` (hoặc `FROM my_udtf(...)`) vì nó **sinh thêm hàng**, giống một "bảng con" được join vào |
| Ví dụ tương đương built-in | `upper()`, `when()`... | `explode()`, `inline()`, `posexplode()`... |

---

## Cuối bài: trả lời tổng kết

**a) UDF là gì?**
UDF (User Defined Function) là hàm do người dùng tự viết (bằng Python, Scala, Java...) để mở rộng khả năng xử lý của Spark SQL/DataFrame khi các hàm built-in không đáp ứng được logic cần thiết. UDF nhận vào giá trị của một hoặc nhiều cột trong **1 dòng** và trả về **đúng 1 giá trị** cho dòng đó.

**b) UDTF khác UDF như thế nào?**
UDF luôn trả về **1 giá trị** cho mỗi lần gọi (ánh xạ 1-1), trong khi UDTF (User Defined Table Function) có thể trả về **nhiều dòng** (thậm chí 0 dòng) cho mỗi lần gọi — tương tự việc "nở" một dòng đầu vào thành một bảng con nhiều dòng, nên khi dùng trong SQL phải kết hợp với `LATERAL` để join kết quả đó vào bảng gốc.

**c) Khi nào nên dùng built-in Spark function thay vì UDF?**
Nên dùng built-in **bất cứ khi nào logic có thể biểu diễn được** bằng các hàm sẵn có của Spark SQL (`trim`, `upper`, `when/otherwise`, `regexp_replace`, `explode`...), vì built-in luôn được Catalyst optimizer hiểu và tối ưu, chạy trực tiếp trong JVM nên nhanh hơn UDF (vốn bị coi là "hộp đen" và tốn chi phí serialize qua lại giữa JVM và Python). Chỉ nên viết UDF khi logic thực sự **không thể** hoặc **rất khó** biểu diễn bằng built-in (thuật toán phức tạp, cần gọi thư viện Python chuyên biệt, xử lý theo quy tắc nghiệp vụ phức tạp nhiều nhánh...).

**d) Python UDF và Pandas UDF khác nhau ở điểm nào?**
Python UDF xử lý **từng dòng một** (scalar), trong khi Pandas UDF xử lý **theo lô** — nhận và trả về cả một `pandas.Series`/`DataFrame` cùng lúc, tận dụng Apache Arrow để truyền dữ liệu hàng loạt giữa JVM và Python thay vì từng dòng, nên **nhanh hơn đáng kể** với khối lượng dữ liệu lớn, đặc biệt khi logic có thể viết bằng các phép toán vector hoá của pandas/NumPy.

**e) Nếu cùng một logic có thể viết bằng built-in function thì nên chọn cách nào và vì sao?**
Nên **luôn chọn built-in function**, vì: (1) hiệu năng tốt hơn — chạy trong JVM, không tốn chi phí serialize dữ liệu qua Python; (2) được Catalyst optimizer tối ưu cùng với các bước xử lý khác trong toàn bộ query plan (UDF thì không, vì Spark không "nhìn" được bên trong UDF); (3) code ngắn gọn, dễ maintain và nhất quán với phong cách khai báo (declarative) của Spark SQL. UDF/Pandas UDF/UDTF chỉ nên dùng như **giải pháp cuối cùng** khi built-in không đáp ứng được yêu cầu.

---

## Output

- `customers_processed/` — bảng gốc (1.000.000 dòng) + `customer_name_clean`, `customer_segment` (built-in), `amount_with_fee` (Pandas UDF), định dạng Parquet, chia thành 4 file part-*.
- `customer_tags/` — kết quả UDTF tách `tags` thành nhiều dòng `customer_id | tag`, định dạng CSV.

## Phụ lục: cách chạy lại

```bash
pip install pyspark pandas pyarrow --break-system-packages
python3 gen_customers_1m.py  # sinh customers_1m.csv (1,000,000 dòng, ~46MB)
python3 udf_lab_1m.py        # chạy toàn bộ 4 yêu cầu + benchmark, in log + ghi output_1m/
```