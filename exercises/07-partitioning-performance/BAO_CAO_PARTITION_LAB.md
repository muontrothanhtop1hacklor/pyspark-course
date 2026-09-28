# Lab: Partition trong Spark — repartition vs coalesce vs partitionBy

**Dữ liệu:** `orders.csv` — 200.000 dòng, cột: `order_id, customer_id, province, amount, status, order_date` (10 tỉnh, phân bố lệch có chủ đích để thấy rõ skew).

**Môi trường:** `local[4]` (giả lập 4 core), `spark.sql.shuffle.partitions = 8`, tắt AQE (`spark.sql.adaptive.enabled = false`) để số partition sau shuffle không bị Spark tự động gộp lại — nếu bật AQE mặc định, mọi trường hợp groupBy bên dưới sẽ tự co về 1 file vì dữ liệu sau `groupBy` quá nhỏ (chỉ 10 dòng), khiến ta không quan sát được đúng ảnh hưởng của cấu hình.

---

## 1. Đọc dữ liệu & số partition ban đầu

```python
df_raw = spark.read.option("header", True).option("inferSchema", True).csv("orders.csv")
print(df_raw.rdd.getNumPartitions())
```

**Kết quả:** `3` partition.

Đây không phải con số cố định — Spark tính dựa trên kích thước file (~9.8 MB) chia cho `spark.sql.files.maxPartitionBytes` (mặc định 128MB) và số block. Với file nhỏ hơn 128MB, Spark vẫn có thể tách thành vài partition dựa trên số core khả dụng và cấu hình đọc — ở đây ra 3.

---

## 2. Kết quả từng trường hợp repartition / coalesce

Với mỗi trường hợp, lab đo **hai lượt ghi khác nhau** để tách bạch hai hiệu ứng:

- **(A) Ghi trực tiếp** dữ liệu đã repartition/coalesce ra Parquet, **không** qua `groupBy` → cho thấy ảnh hưởng **trực tiếp** của repartition/coalesce lên số file.
- **(B) Ghi sau khi `groupBy("province")`** → vì `groupBy` luôn kích hoạt **shuffle riêng của nó**, số file ở bước này phụ thuộc vào `spark.sql.shuffle.partitions`, gần như **không liên quan** đến số partition trước đó.

| Trường hợp | Số partition | File ghi trực tiếp (A) | File sau groupBy (B) | Thời gian |
|---|---|---|---|---|
| `repartition(2)` | 2 | **2** | 6 | 2.30s |
| `repartition(4)` | 4 | **4** | 6 | 1.15s |
| `repartition(8)` | 8 | **8** | 6 | 1.19s |
| `repartition("province")` | 8 | **6** * | 6 | 1.07s |
| `coalesce(2)` | 2 | **2** | 6 | 0.80s |
| `repartition(200)` (nâng cao) | 200 | **200** | 6 | 3.02s |

\* `repartition("province")` báo `getNumPartitions()=8` (bằng `spark.sql.shuffle.partitions`), nhưng khi ghi thực tế chỉ ra **6 file có dữ liệu** — vì cột `province` được băm (hash) vào 8 bucket, nhưng chỉ 6/8 bucket có ít nhất 1 tỉnh rơi vào (10 tỉnh dồn vào 8 bucket theo hash, có bucket trống, có bucket chứa nhiều hơn 1 tỉnh). Đây chính là **hiện tượng "hash collision"** khi repartition theo cột có ít giá trị phân biệt.

### Nhận xét cột (A) — ghi trực tiếp, không groupBy

- Số **file output = số partition** một cách chính xác, gần như 1-1 (trừ cột dùng key có ít giá trị phân biệt).
- `repartition(2/4/8)` dùng **round-robin hoặc hash ngẫu nhiên toàn bộ dữ liệu lại** (shuffle toàn bộ) rồi chia đều — file tương đối đồng đều về kích thước (~3MB / n file).
- `coalesce(2)` **không shuffle** — chỉ gộp cơ học các partition hiện có (3 partition gốc → gộp còn 2), nên nhanh hơn hẳn (0.8s so với 2.3s của repartition(2) dù cùng ra 2 partition).
- `repartition("province")` phân phối dữ liệu theo **giá trị cột**, các tỉnh có tỷ trọng đơn hàng cao hơn (HaNoi, HoChiMinh — vì tạo dữ liệu có tỉ trọng cao hơn) sẽ rơi vào partition lớn hơn → **data skew**, không đồng đều.

### Nhận xét cột (B) — sau groupBy

- **Không đổi (6 file) dù trước đó là repartition(2), (4), (8), (200) hay coalesce(2).** Đây là điểm quan trọng nhất của bài: `groupBy` là một **transformation gây shuffle**, nó tạo ra shuffle stage mới và **partition lại toàn bộ theo `spark.sql.shuffle.partitions`** (ở đây = 8, nhưng chỉ 6/8 bucket có dữ liệu vì chỉ có 10 nhóm `province`) — **partition đầu vào trước đó bị "xoá bỏ", không kế thừa**.
- Nói cách khác: **repartition/coalesce trước một phép toán có shuffle (groupBy, join, orderBy...) chỉ ảnh hưởng đến hiệu năng tính toán của phép toán đó (cân bằng tải giữa các task), chứ không quyết định số partition/số file ở output cuối cùng** — số đó do `spark.sql.shuffle.partitions` (hoặc AQE nếu bật) quyết định.

---

## 3. So sánh `repartition` và `coalesce`

| Tiêu chí | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Có shuffle không? | **Có** — luôn shuffle toàn bộ dữ liệu qua mạng/đĩa | **Không** (khi giảm số partition) — chỉ gộp cơ học các partition hiện có, dữ liệu không di chuyển giữa executor nếu không cần |
| Tăng số partition | Được | **Không** — coalesce chỉ dùng để **giảm**, gọi `coalesce(n)` với `n` lớn hơn số partition hiện tại sẽ **không có tác dụng** (Spark giữ nguyên số partition cũ) |
| Giảm số partition | Được, nhưng tốn shuffle không cần thiết | Được, **rẻ hơn** vì không shuffle |
| Cân bằng dữ liệu (data balance) | Tốt — dữ liệu chia đều ngẫu nhiên (round-robin) hoặc theo hash | Có thể **lệch tải** — vì chỉ gộp các partition liền kề, nếu các partition gốc vốn đã lệch kích thước thì sau coalesce vẫn lệch |
| Thời gian thực thi (trong lab) | Chậm hơn (2.30s cho n=2) | Nhanh hơn (0.80s cho n=2) |
| Khi nào dùng | Cần **tăng** song song hoá, cần **cân bằng lại** dữ liệu trước join/groupBy tốn kém, hoặc cần chia đều theo key | Cần **giảm** số file output cuối cùng (trước khi ghi) mà **không cần** cân bằng lại, ưu tiên tốc độ |

**Khi nào gây shuffle:** `repartition()` **luôn luôn** shuffle (dù tăng hay giảm số partition). `coalesce()` chỉ shuffle nếu bạn cố tình truyền `shuffle=True`, còn mặc định là **không** shuffle khi giảm.

**Output tạo bao nhiêu file:** cả hai đều tuân theo quy tắc **1 partition → 1 file** khi ghi trực tiếp (không qua thêm transformation gây shuffle nào khác sau đó).

---

## 4. `partitionBy` khi ghi vs không dùng

Cả hai cùng ghi bảng đã `groupBy("province")` (10 dòng kết quả), từ `df_raw.repartition(4)`:

**Không dùng `partitionBy`:**
```
case_no_partitionBy/
  _SUCCESS
  part-00000-...snappy.parquet
  part-00001-...snappy.parquet
  part-00002-...snappy.parquet
  part-00004-...snappy.parquet
  part-00005-...snappy.parquet
  part-00007-...snappy.parquet
```
→ **6 file phẳng**, tất cả 10 tỉnh trộn lẫn trong 6 file dựa theo hash-partition của shuffle, không theo cấu trúc thư mục nào.

**Dùng `partitionBy("province")`:**
```
case_with_partitionBy/
  _SUCCESS
  province=ThanhHoa/part-00001-...c000.snappy.parquet
  province=HaiPhong/part-00004-...c000.snappy.parquet
  province=DongNai/part-00000-...c000.snappy.parquet
  province=HoChiMinh/part-00002-...c000.snappy.parquet
  province=DaNang/part-00001-...c000.snappy.parquet
  province=CanTho/part-00002-...c000.snappy.parquet
  province=NgheAn/part-00005-...c000.snappy.parquet
  province=BinhDuong/part-00007-...c000.snappy.parquet
  province=KhanhHoa/part-00004-...c000.snappy.parquet
  province=HaNoi/part-00004-...c000.snappy.parquet
```
→ **10 file, mỗi tỉnh 1 thư mục con `province=<value>/`** — đây là cấu trúc **Hive-style partitioning**. Mỗi thư mục tương ứng đúng 1 giá trị của cột `province`.

### Giải thích khác biệt

- `partitionBy` khi ghi **không phải** một phép biến đổi trên DataFrame (không gây shuffle, không đổi `getNumPartitions()` của DataFrame) — nó chỉ là **chỉ thị cho writer**: "khi ghi ra đĩa, hãy **tách file theo thư mục** dựa trên giá trị của (các) cột này". Mỗi in-memory partition khi ghi sẽ tự chia nhỏ ra tương ứng theo giá trị cột.
- `repartition(col)` xảy ra **trong bộ nhớ, trước khi ghi**, quyết định dữ liệu được **shuffle** và gom nhóm vào **bao nhiêu task song song** ra sao (dựa trên hash của cột) — không tạo thư mục con.
- Kết hợp cả hai (`df.repartition("province").write.partitionBy("province")...`) là pattern phổ biến: `repartition("province")` giúp mỗi partition trong bộ nhớ chỉ chứa dữ liệu của (gần như) đúng 1 tỉnh, để khi `partitionBy` ghi ra, **mỗi thư mục `province=X/` chỉ có 1 file** thay vì nhiều file nhỏ rải rác do nhiều task cùng ghi vào cùng 1 thư mục.
- Lợi ích của Hive-style `partitionBy` khi đọc lại: engine (Spark, Hive, Presto...) có thể **partition pruning** — chỉ đọc thư mục `province=HaNoi/` nếu query có `WHERE province = 'HaNoi'`, không cần quét toàn bộ dữ liệu.

---

## 5. Phần nâng cao: `repartition(200)` — over-partition

```
[repartition(200)] partitions=200 | file ghi trực tiếp = 200 | file sau groupBy = 6
Tổng dung lượng: 3656.5 KB, trung bình mỗi file: 18.28 KB/file
```

- Ghi trực tiếp (không groupBy) với 200 partition trên dữ liệu chỉ ~3.5MB (sau khi giải nén thành Parquet có nén) → sinh **200 file, mỗi file trung bình chỉ ~18 KB**.
- Đây là ví dụ điển hình của **"small files problem"**.

### Vì sao quá nhiều file nhỏ gây bất lợi

1. **Quá tải Namenode/metadata store**: HDFS/S3/metastore phải lưu & quản lý metadata (tên file, vị trí, kích thước...) cho từng file — hàng triệu file nhỏ làm phình metadata, chậm liệt kê (`list`), chậm lập kế hoạch truy vấn.
2. **Overhead khi đọc lại**: mỗi file nhỏ vẫn tốn 1 "task" khi Spark đọc lại (mở file, đọc header Parquet, đóng file) — chi phí **mở/đóng file** (I/O overhead) có thể lớn hơn cả thời gian đọc dữ liệu thật sự, khiến job đọc chậm dù dữ liệu tổng không lớn.
3. **Không tận dụng được nén & columnar tốt**: Parquet có lợi thế nén và đọc theo cột hiệu quả khi file đủ lớn (row-group đủ lớn); file quá nhỏ làm giảm tỷ lệ nén, tăng overhead của footer/schema lặp lại trong từng file.
4. **Tốn nhiều task hơn mức cần** trên cluster thật (mỗi task có chi phí khởi tạo cố định trên executor) → tổng thời gian có thể lâu hơn dùng ít task, file lớn hơn.
5. Trên các hệ thống tính phí theo **số request** (S3, cloud storage) — nhiều file nhỏ đồng nghĩa nhiều lượt gọi API hơn → **tốn chi phí** hơn.

**Khuyến nghị thực tế:** kích thước file Parquet output nên nhắm khoảng **128MB–1GB/file** tuỳ hệ thống đọc, và số partition khi ghi nên được tính dựa trên: `số partition ≈ tổng kích thước dữ liệu / kích thước file mong muốn`, rồi dùng `coalesce()` hoặc `repartition()` để đạt số đó trước khi `write`.

---

## 6. Trả lời tổng kết

**a) `repartition` khác `coalesce` như thế nào?**
`repartition(n)` luôn **shuffle** toàn bộ dữ liệu để chia lại thành đúng `n` partition có kích thước tương đối đều (dùng round-robin hoặc hash), có thể **tăng hoặc giảm** số partition. `coalesce(n)` **không shuffle** (mặc định), chỉ **gộp cơ học** các partition liền kề lại để **giảm** số partition — nhanh hơn nhưng có thể khiến dữ liệu lệch tải nếu các partition gốc vốn không đều.

**b) `repartition` theo cột khác gì `repartition` theo số?**
`repartition(n)` (theo số) dùng **round-robin** — chia đều số lượng dòng vào `n` partition, không quan tâm giá trị dữ liệu, phù hợp khi chỉ cần tăng song song hoá đơn thuần. `repartition(col)` (theo cột) dùng **hash của giá trị cột** để quyết định dòng nào vào partition nào — các dòng có cùng giá trị cột sẽ nằm cùng 1 partition (hữu ích trước `join`/`groupBy` theo cùng cột để tránh shuffle lặp lại), nhưng dễ gây **data skew** nếu cột đó phân bố không đều (một số giá trị xuất hiện nhiều hơn hẳn), và số partition có dữ liệu thực tế có thể **ít hơn** số partition khai báo nếu số giá trị phân biệt của cột nhỏ hơn hoặc hash bị trùng bucket.

**c) `partitionBy` khi write khác `repartition` trong Spark ở điểm nào?**
`repartition` là phép biến đổi **trong bộ nhớ**, xảy ra **trước** khi ghi, ảnh hưởng đến việc dữ liệu được shuffle/phân chia vào các task xử lý song song thế nào (không liên quan trực tiếp đến cấu trúc file lúc ghi). `partitionBy` là chỉ thị cho **writer**, chỉ có tác dụng **lúc ghi ra đĩa**: tạo cấu trúc thư mục con dạng `col=value/` (Hive-style) theo giá trị của cột được chỉ định, phục vụ mục đích **đọc lại hiệu quả hơn** (partition pruning), không làm thay đổi số partition trong bộ nhớ của DataFrame.

**d) Trường hợp nào nên tăng/giảm số partition?**
- **Tăng** (`repartition` lên số lớn hơn) khi: dữ liệu đầu vào quá lớn nhưng partition quá ít khiến 1 vài task phải xử lý khối lượng khổng lồ (dễ OOM, chạy chậm/không cân bằng tải), hoặc trước khi `join`/`groupBy` trên cột có skew mà cần phân bổ lại.
- **Giảm** (`coalesce` hoặc `repartition` xuống số nhỏ hơn) khi: sau các phép `filter`/`groupBy` làm dữ liệu co lại nhiều nhưng vẫn giữ số partition cũ (quá nhiều partition rỗng hoặc quá nhỏ), hoặc **trước khi ghi output cuối cùng** để tránh sinh quá nhiều file nhỏ.

**e) Vì sao không nên tạo quá nhiều file nhỏ?**
Vì mỗi file nhỏ vẫn tốn chi phí cố định khi lưu trữ (metadata) và khi đọc lại (mở/đóng file, khởi tạo task), khiến hệ thống chậm đi ở khâu liệt kê, lập kế hoạch truy vấn và I/O, đồng thời giảm hiệu quả nén/đọc cột của định dạng Parquet, và có thể tăng chi phí (đặc biệt trên cloud storage tính phí theo số request) — dù tổng dung lượng dữ liệu không đổi.

---

## Phụ lục: cách chạy lại

```bash
pip install pyspark --break-system-packages
python3 gen_data.py          # sinh orders.csv (200,000 dòng)
python3 partition_lab.py     # chạy toàn bộ thí nghiệm, in log + ghi output/
```

File `results_summary.txt` chứa dữ liệu thô (dict Python) của từng trường hợp để tiện đối chiếu hoặc vẽ biểu đồ thêm.
