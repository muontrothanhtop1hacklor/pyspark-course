# BÁO CÁO THỰC HÀNH: SPARK BATCH VS STRUCTURED STREAMING (BÀI 11)

---

## 1. PHẦN CHUẨN BỊ: DỮ LIỆU GỐC & TÍNH TOÁN ĐỐI CHIẾU (BASELINE)

Bộ dữ liệu gồm 30 dòng được chia đều vào 3 file CSV trong thư mục `data/batch_input/`:
- `orders_1.csv` (10 dòng: 7 hợp lệ, 3 lỗi)
- `orders_2.csv` (10 dòng: 7 hợp lệ, 3 lỗi; chứa duplicate `order_id` 1001)
- `orders_3.csv` (10 dòng: 7 hợp lệ, 3 lỗi; chứa duplicate `order_id` 1002)

### Bảng dữ liệu chi tiết và phân loại lỗi:

| File | order_id | province | amount | status | order_date | updated_at | Trạng thái kiểm tra |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **orders_1** | 1001 | HaNoi | 1,000,000 | SUCCESS | 2025-01-05 | 2025-01-05 08:30:00 | **Hợp lệ** (1.0M) |
| | 1002 | HoChiMinh | 2,500,000 | completed | 2025-01-06 | 2025-01-06 09:15:00 | **Hợp lệ** (2.5M) |
| | 1003 | DaNang | 1,500,000 | PENDING | 2025-01-07 | 2025-01-07 10:00:00 | **Hợp lệ** (1.5M) |
| | 1004 | CanTho | 800,000 | COMPLETED | 2025-01-08 | 2025-01-08 11:20:00 | **Hợp lệ** (0.8M) |
| | 1005 | HaNoi | 0.0 | SUCCESS | 2025-01-09 | 2025-01-09 14:00:00 | **LỖI**: amount == 0 |
| | 1006 | HaiPhong | 1,200,000 | Shipping | 2025-01-10 | 2025-01-10 15:30:00 | **Hợp lệ** (1.2M) |
| | 1007 | HoChiMinh | -50,000 | SUCCESS | 2025-01-11 | 2025-01-11 16:45:00 | **LỖI**: amount < 0 |
| | 1008 | DaNang | 3,000,000 | COMPLETED | 31/02/2025 | 2025-01-12 17:00:00 | **LỖI**: Ngày 31/02 không tồn tại |
| | 1009 | CanTho | 600,000 | SUCCESS | 2025-01-13 | 2025-01-13 18:10:00 | **Hợp lệ** (0.6M) |
| | 1010 | HaNoi | 2,000,000 | Pending | 2025-01-14 | 2025-01-14 19:30:00 | **Hợp lệ** (2.0M) |
| **orders_2** | 1001 | HaNoi | 1,500,000 | COMPLETED | 2025-01-15 | 2025-01-15 10:00:00 | **Hợp lệ** (1.5M - duplicate 1001) |
| | 1011 | HoChiMinh | 4,000,000 | SUCCESS | 2025-01-16 | 2025-01-16 11:00:00 | **Hợp lệ** (4.0M) |
| | 1012 | DaNang | N/A | COMPLETED | 2025-01-17 | 2025-01-17 12:30:00 | **LỖI**: amount text 'N/A' |
| | 1013 | CanTho | 1,100,000 | pending | 2025-01-18 | 2025-01-18 13:45:00 | **Hợp lệ** (1.1M) |
| | 1014 | HaiPhong | 900,000 | SUCCESS | 2025-01-19 | 2025-01-19 14:15:00 | **Hợp lệ** (0.9M) |
| | 1015 | HaNoi | 1,800,000 | COMPLETED | 2025-13-40 | 2025-01-20 15:00:00 | **LỖI**: Tháng 13 ngày 40 |
| | 1016 | HoChiMinh | 3,200,000 | Shipping | 2025-01-21 | 2025-01-21 16:20:00 | **Hợp lệ** (3.2M) |
| | 1017 | DaNang | 2,200,000 | SUCCESS | 2025-01-22 | 2025-01-22 17:35:00 | **Hợp lệ** (2.2M) |
| | 1018 | CanTho | *(rỗng)* | COMPLETED | 2025-01-23 | 2025-01-23 18:40:00 | **LỖI**: amount rỗng/null |
| | 1019 | HaNoi | 2,700,000 | SUCCESS | 2025-01-24 | 2025-01-24 19:50:00 | **Hợp lệ** (2.7M) |
| **orders_3** | 1002 | HoChiMinh | 2,800,000 | COMPLETED | 2025-01-25 | 2025-01-25 09:00:00 | **Hợp lệ** (2.8M - duplicate 1002) |
| | 1020 | DaNang | 1,700,000 | SUCCESS | 2025-01-26 | 2025-01-26 10:15:00 | **Hợp lệ** (1.7M) |
| | 1021 | CanTho | 500,000 | pending | 2025-01-27 | 2025-01-27 11:30:00 | **Hợp lệ** (0.5M) |
| | 1022 | HaiPhong | -100,000 | FAILED | 2025-01-28 | 2025-01-28 12:45:00 | **LỖI**: amount < 0 |
| | 1023 | HaNoi | 3,500,000 | COMPLETED | *(rỗng)* | 2025-01-29 13:50:00 | **LỖI**: order_date rỗng |
| | 1024 | HoChiMinh | 1,900,000 | SUCCESS | 2025-01-30 | 2025-01-30 14:20:00 | **Hợp lệ** (1.9M) |
| | 1025 | DaNang | 0.0 | CANCELLED | 2025-01-31 | 2025-01-31 15:10:00 | **LỖI**: amount == 0 |
| | 1026 | CanTho | 1,300,000 | Shipping | 2025-02-01 | 2025-02-01 16:00:00 | **Hợp lệ** (1.3M) |
| | 1027 | HaiPhong | 2,100,000 | SUCCESS | 2025-02-02 | 2025-02-02 17:15:00 | **Hợp lệ** (2.1M) |
| | 1028 | HaNoi | 4,500,000 | SUCCESS | 2025-02-03 | 2025-02-03 18:30:00 | **Hợp lệ** (4.5M) |

### Số liệu đối chiếu chuẩn (Baseline Numbers):
- **Tổng số dòng nạp:** 30 dòng (10 dòng/file).
- **Tổng số dòng lỗi:** 9 dòng (mỗi file 3 dòng lỗi).
- **Tổng số dòng hợp lệ:** **21 dòng**.
- **Tổng amount của các dòng hợp lệ:** **40,000,000 VND**.
- **Số order và doanh số theo từng Province:**
  - `CanTho`: **5 đơn**, **4,300,000 VND** (800k + 600k + 1.1M + 500k + 1.3M)
  - `DaNang`: **3 đơn**, **5,400,000 VND** (1.5M + 2.2M + 1.7M)
  - `HaiPhong`: **3 đơn**, **4,200,000 VND** (1.2M + 900k + 2.1M)
  - `HaNoi`: **5 đơn**, **11,700,000 VND** (1.0M + 2.0M + 1.5M + 2.7M + 4.5M)
  - `HoChiMinh`: **5 đơn**, **14,400,000 VND** (2.5M + 4.0M + 3.2M + 2.8M + 1.9M)

---

## 2. YÊU CẦU 1: KẾT QUẢ BATCH PROCESSING

### Cách chạy:
```bash
python exercises/11-batch-vs-streaming/req1_batch.py
```

### Kết quả trên Console:
```text
+---------+------------+------------+
|province |total_orders|total_amount|
+---------+------------+------------+
|CanTho   |5           |4300000.0   |
|DaNang   |3           |5400000.0   |
|HaNoi    |5           |1.17E7      |
|HaiPhong |3           |4200000.0   |
|HoChiMinh|5           |1.44E7      |
+---------+------------+------------+
```

### Đối chiếu:
- Tổng số đơn: **21** (Khớp 100% với baseline).
- Tổng doanh thu: **40,000,000 VND** (Khớp 100% với baseline).

---

## 3. YÊU CẦU 2: STREAMING CÙNG LOGIC (BẢNG THEO DÕI MICRO-BATCH)

### Cách chạy:
```bash
python exercises/11-batch-vs-streaming/req2_streaming_console.py
```
- Cơ chế: `readStream` với `maxFilesPerTrigger=1`, `trigger(processingTime="5 seconds")`, `outputMode("complete")`.
- Lần lượt thả `orders_1.csv`, `orders_2.csv`, `orders_3.csv` vào `data/stream_input/`.

### Bảng theo dõi từng Micro-batch:

| Micro-batch | File vừa thả | Kết quả trên console | Khác gì so với batch trước đó |
| :--- | :--- | :--- | :--- |
| **Batch 0** | `orders_1.csv` | • HaNoi: 2 đơn, 3.0M<br>• CanTho: 2 đơn, 1.4M<br>• HoChiMinh: 1 đơn, 2.5M<br>• HaiPhong: 1 đơn, 1.2M<br>• DaNang: 1 đơn, 1.5M<br>*(Tổng: 7 đơn, 9.6M)* | Bắt đầu khởi tạo State Store từ con số 0. In ra kết quả tổng hợp của 7 dòng hợp lệ đầu tiên trong `orders_1.csv`. |
| **Batch 1** | `orders_2.csv` | • HaNoi: 4 đơn, 7.2M (+2 đơn, +4.2M)<br>• CanTho: 3 đơn, 2.5M (+1 đơn, +1.1M)<br>• HoChiMinh: 3 đơn, 9.7M (+2 đơn, +7.2M)<br>• HaiPhong: 2 đơn, 2.1M (+1 đơn, +0.9M)<br>• DaNang: 2 đơn, 3.7M (+1 đơn, +2.2M)<br>*(Tổng: 14 đơn, 25.2M)* | State Store cộng dồn (accumulate) dữ liệu mới của `orders_2.csv` vào state cũ. In ra **toàn bộ bảng kết quả tổng hợp mới** (do `complete` mode). Cả 5 tỉnh đều tăng đơn và tăng amount. |
| **Batch 2** | `orders_3.csv` | • HaNoi: 5 đơn, 11.7M (+1 đơn, +4.5M)<br>• CanTho: 5 đơn, 4.3M (+2 đơn, +1.8M)<br>• HoChiMinh: 5 đơn, 14.4M (+2 đơn, +4.7M)<br>• HaiPhong: 3 đơn, 4.2M (+1 đơn, +2.1M)<br>• DaNang: 3 đơn, 5.4M (+1 đơn, +1.7M)<br>*(Tổng: 21 đơn, 40.0M)* | State Store tiếp tục cộng dồn 7 dòng hợp lệ từ `orders_3.csv`. Toàn bộ 21 đơn hợp lệ đã được tổng hợp đầy đủ. Số liệu khớp tuyệt đối với Batch. |

---

## 4. YÊU CẦU 3: SO SÁNH BATCH VÀ STREAMING

### 1. Đối chiếu kết quả:
- Kết quả Streaming sau khi hoàn tất Batch 2 **khớp 100%** với kết quả Batch ở Yêu cầu 1 và số tính tay ở phần Chuẩn bị:
  - Cả hai đều ra **21 đơn hàng hợp lệ** và tổng số tiền **40,000,000 VND**.
  - Tỷ lệ và số lượng từng tỉnh thành (CanTho: 5, DaNang: 3, HaNoi: 5, HaiPhong: 3, HoChiMinh: 5) hoàn toàn đồng nhất.

### 2. Ba (03) điểm khác nhau quan sát được:

| Tiêu chí | Batch Processing | Structured Streaming |
| :--- | :--- | :--- |
| **1. Cách viết code** | Dùng `spark.read`, DataFrame là tĩnh (bounded). Không cần khai báo checkpoint hay trigger, kết thúc bằng các action tức thời (`.show()`, `.write.parquet()`). Schema có thể tự động infer. | Dùng `spark.readStream`, DataFrame đại diện cho bảng luồng (unbounded). Bắt buộc phải khai báo schema tĩnh, phải cấu hình `trigger`, `outputMode` và `checkpointLocation`. Kết thúc bằng `.start()` và `.awaitTermination()`. |
| **2. Cách chạy & Vòng đời** | Job chỉ chạy **1 lần duy nhất** trên tập dữ liệu hiện có (snapshot), chiếm tài nguyên xử lý xong rồi giải phóng và thoát ngay. | Job chạy **liên tục dưới dạng tiến trình nền (daemon)**. Engine Spark định kỳ thức dậy theo chu kỳ trigger, kiểm tra nguồn dữ liệu mới, chạy từng micro-batch và cập nhật State Store. Job không tự tắt trừ khi có lỗi hoặc được gọi lệnh `.stop()`. |
| **3. Cách hiển thị kết quả** | `.show()` in ra kết quả cuối cùng một lần duy nhất tại thời điểm xử lý xong toàn bộ dữ liệu. | Console in ra liên tục qua từng micro-batch (Batch: 0, Batch: 1, Batch: 2...). Với `outputMode("complete")`, mỗi micro-batch đều in lại toàn bộ trạng thái tổng hợp mới nhất của toàn hệ thống từ đầu đến hiện tại. |

---

## 5. YÊU CẦU 4: THỬ NGHIỆM VÀ QUAN SÁT CÁC HIỆN TƯỢNG / THÔNG BÁO LỖI

Tất cả 5 thử nghiệm đã được kiểm chứng thực tế trong script `req4_experiments.py`:

### Thử nghiệm 1: Bỏ `.schema(...)` khỏi `readStream`
- **Hiện tượng**: Spark dừng ngay khi gọi `.load(STREAM_INPUT_DIR)` và ném ra ngoại lệ.
- **Thông báo lỗi**:
  ```text
  java.lang.IllegalArgumentException: Schema must be specified when creating a streaming source DataFrame. 
  If some files already exist in the directory, then depending on the file format you may be able to 
  create a static DataFrame on that directory with 'spark.read.load(directory)' and infer schema from it.
  ```
- **Ý nghĩa**: Trong streaming, dữ liệu là vô hạn và có thể chưa xuất hiện khi job khởi động. Spark không thể scan toàn bộ dữ liệu tương lai để đoán kiểu dữ liệu, nên bắt buộc người dùng phải cung cấp schema tĩnh từ đầu.

### Thử nghiệm 2: Đổi trigger thành 20 giây rồi thả file mới
- **Hiện tượng**: Thả file `orders_1.csv` lúc 15:10:18, nhưng Micro-batch 0 không kích hoạt ngay mà phải đợi đến đúng nhịp tick tiếp theo của trigger clock (15:10:21) mới xử lý xong.
- **Ý nghĩa**: Trigger `processingTime="20s"` quy định đồng hồ nhịp đập (tick interval). Spark chỉ thức dậy thăm dò nguồn dữ liệu ở mỗi chu kỳ 20 giây, dẫn đến độ trễ dữ liệu (data latency) tối đa bằng chu kỳ trigger.

### Thử nghiệm 3: Thả lại cùng một file đã thả trước đó
- **Hiện tượng**: Thả `orders_1.csv`, Spark xử lý Micro-batch 0 (10 dòng). Sau đó ghi đè/thả lại chính file `orders_1.csv` vào `stream_input`. Spark **hoàn toàn bỏ qua**, không kích hoạt thêm bất kỳ micro-batch nào (`batches_processed = [(0, 10)]`).
- **Ý nghĩa**: Structured Streaming có cơ chế quản lý offset bằng nhật ký tệp (`FileStreamSourceLog` lưu trong checkpoint). Spark ghi nhớ tên tệp và đường dẫn đã xử lý; những tệp cũ dù bị ghi đè nội dung cũng sẽ không bị xử lý lại, giúp đảm bảo tính idempotent.

### Thử nghiệm 4: Đổi `outputMode` sang `"append"` trên bản aggregate
- **Hiện tượng**: Spark ném ngoại lệ phân tích câu lệnh ngay khi gọi `.start()`.
- **Thông báo lỗi**:
  ```text
  pyspark.errors.exceptions.captured.AnalysisException: 
  [STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION] Invalid streaming output mode: append. 
  This output mode is not supported for streaming aggregations without watermark on streaming DataFrames/DataSets. 
  SQLSTATE: 42KDE
  ```
- **Ý nghĩa**: Phép tổng hợp (`groupBy`) trên stream liên tục nhận thêm dữ liệu làm thay đổi kết quả tổng. Nếu không có **Watermark** (ngưỡng xác định khi nào dữ liệu của một khoảng thời gian được coi là đã đóng), Spark không bao giờ biết một dòng aggregate đã hoàn tất (finalized) hay chưa để emit ra theo chế độ `append`. Vì vậy, với aggregate không watermark, chỉ hỗ trợ `complete` hoặc `update`.

### Thử nghiệm 5: Thử dedup bằng `Window + row_number()` trên stream
- **Hiện tượng**: Spark từ chối thực thi và báo lỗi cú pháp streaming ngay lập tức.
- **Thông báo lỗi**:
  ```text
  pyspark.errors.exceptions.captured.AnalysisException: 
  [NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING] Window function is not supported in ROW_NUMBER() 
  (as column `rn`) on streaming DataFrames/Datasets. 
  Structured Streaming only supports time-window aggregation using the WINDOW function. 
  (window specification: (PARTITION BY ORDER_ID ORDER BY UPDATED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) 
  SQLSTATE: 42KDE
  ```
- **Ý nghĩa**: Trong batch, Window với `orderBy(updated_at.desc())` có thể gom toàn bộ tập dữ liệu vào các partition bộ nhớ để đánh số thứ tự từ 1 đến N. Nhưng trong streaming, dữ liệu đến vô tận, Spark không thể lưu giữ vô hạn toàn bộ lịch sử các dòng trong bộ nhớ để xếp hạng lại `row_number()`. Structured Streaming yêu cầu dùng phương thức chuyên dụng `dropDuplicates(["order_id"])` kết hợp watermark thay vì dùng window function thông thường.

---

## 6. YÊU CẦU 5: GHI DỮ LIỆU RA PARQUET & KHÁM PHÁ CHECKPOINT

### Cách chạy:
```bash
python exercises/11-batch-vs-streaming/req5_write_and_check.py
```

### 1. Kết quả kiểm tra đối chiếu (Parity Audit):
- **Row count**:
  - `Batch Parquet`: **21 dòng**
  - `Streaming Parquet`: **21 dòng**
  - Đối chiếu tính tay: **21 dòng** $\rightarrow$ **Khớp tuyệt đối (100%)**.
- **Tổng amount**:
  - `Batch Parquet`: **40,000,000 VND**
  - `Streaming Parquet`: **40,000,000 VND**
  - Đối chiếu tính tay: **40,000,000 VND** $\rightarrow$ **Khớp tuyệt đối (100%)**.
- **Schema kiểm tra**:
  - Cả hai đầu ra đều có schema hoàn toàn đồng nhất:
    `struct<order_id:string,customer_id:string,province:string,amount_clean:double,status_clean:string,order_date_clean:date,updated_at:string>`

### 2. Khám phá thư mục Checkpoint:
Cấu trúc thư mục thực tế tại `checkpoint/stream_parquet_write/`:
```text
checkpoint/
├── metadata
├── commits/
│   ├── 0
│   ├── 1
│   └── 2
├── offsets/
│   ├── 0
│   ├── 1
│   └── 2
└── sources/
    └── 0/
        ├── 0
        ├── 1
        └── 2
```

**Quan sát các thành phần trong checkpoint:**
- `metadata`: Chứa ID định danh duy nhất của Streaming Query (`runId`).
- `offsets/`: Lưu trạng thái offset của dữ liệu đầu vào cho từng micro-batch (ví dụ batch 0 đọc đến đâu, batch 1 đọc đến đâu).
- `commits/`: Đánh dấu các micro-batch đã ghi thành công vào sink (Parquet). Nếu crash giữa chừng, Spark biết batch nào đã commit, batch nào chưa để replay.
- `sources/0/`: Đối với File Stream, đây là `FileStreamSourceLog` ghi lại danh sách tên file cụ thể đã được đọc trong từng micro-batch (giúp Spark không bao giờ đọc lặp lại file cũ).

---

## 7. TRẢ LỜI 7 CÂU HỎI LÝ THUYẾT BẢN CHẤT

### Câu 1: Vì sao gọi Structured Streaming là "bảng vô hạn" (Unbounded Table)?
- **Trả lời**: Trong tư duy thiết kế của Spark Structured Streaming, luồng dữ liệu liên tục được trừu tượng hóa dưới dạng một bảng quan hệ không có giới hạn về số dòng (Unbounded Table).
- Mỗi mẩu dữ liệu mới (record hoặc file) đến với hệ thống được xem như một **dòng mới được append vào cuối bảng**.
- Nhà phát triển viết câu lệnh truy vấn (SELECT, FILTER, AGGREGATE) trên bảng vô hạn này với cú pháp giống hệt như đang truy vấn một bảng tĩnh (Batch), và Spark Engine sẽ tự động chuyển đổi logic này thành kế hoạch thực thi gia tăng (incremental execution) liên tục.

### Câu 2: Micro-batch là gì? Dựa trên console, một micro-batch tương ứng với gì?
- **Trả lời**: 
  - **Micro-batch** là kiến trúc xử lý của Spark Structured Streaming, trong đó luồng dữ liệu liên tục được gom lại thành các "lô cực nhỏ" (micro-batches) theo một chu kỳ thời gian (trigger) ngắn (từ vài mili-giây đến vài giây), sau đó kích hoạt một job Spark engine mini để xử lý lô đó.
  - Dựa trên thực tế quan sát ở console Yêu cầu 2:
    - Khi đặt `maxFilesPerTrigger=1`, **mỗi micro-batch (Batch 0, Batch 1, Batch 2) tương ứng với việc Spark phát hiện, nạp và xử lý đúng 1 file CSV mới** vừa xuất hiện trong thư mục `stream_input/`.
    - Console in ra khối `------------------------------------------- Batch: X -------------------------------------------` tương ứng với một lần engine cập nhật trạng thái kết quả của micro-batch đó.

### Câu 3: Vì sao readStream yêu cầu khai báo schema bắt buộc?
- **Trả lời**:
  1. Trong Batch, toàn bộ dữ liệu đã tồn tại sẵn trong thư mục, Spark có thể quét qua một số file hoặc toàn bộ tập tin để suy diễn kiểu dữ liệu (Schema Inference).
  2. Trong Streaming, khi ứng dụng bắt đầu khởi chạy (`readStream`), **thư mục nguồn có thể hoàn toàn trống** (chưa có tệp tin nào) hoặc dữ liệu tương lai có thể có sự thay đổi. Spark không thể biết trước kiểu dữ liệu nếu không được khai báo.
  3. Schema bắt buộc đóng vai trò như một **Data Contract (Hợp đồng dữ liệu)** bất biến, giúp Spark tối ưu hóa kế hoạch thực thi trước (Catalyst Optimizer) và phân bổ bộ nhớ cố định cho các cột xuyên suốt toàn bộ vòng đời của stream.

### Câu 4: Với complete mode, mỗi micro-batch in ra dữ liệu mới hay toàn bộ kết quả? Dựa trên bảng theo dõi của bạn.
- **Trả lời**:
  - Trong `complete` mode, mỗi micro-batch **in ra TOÀN BỘ kết quả tổng hợp** tính từ đầu đến thời điểm hiện tại, chứ không chỉ in phần chênh lệch hay dữ liệu mới.
  - **Dẫn chứng từ bảng theo dõi**:
    - Ở Batch 0: In 5 tỉnh với tổng 7 đơn (9.6M).
    - Ở Batch 1 (thêm `orders_2.csv`): Console in lại đầy đủ cả 5 tỉnh với số liệu lũy kế 14 đơn (25.2M), bao gồm cả các đơn từ Batch 0 cộng với Batch 1.
    - Ở Batch 2: Console in lại toàn bộ 5 tỉnh với số liệu lũy kế 21 đơn (40.0M).

### Câu 5: Thả lại cùng một file thì Spark xử lý lại hay bỏ qua? Bạn đoán checkpoint có liên quan gì?
- **Trả lời**:
  - Spark **hoàn toàn BỎ QUA** file đó và không xử lý lại.
  - **Mối liên hệ với Checkpoint**: Thư mục checkpoint lưu trữ tệp nhật ký `sources/0/X` (`FileStreamSourceLog`). Mỗi khi một tệp tin được xử lý trong một batch, đường dẫn tuyệt đối (hoặc URI) của file đó được ghi vào log. Khi chu kỳ trigger tiếp theo quét thư mục, Spark so sánh danh sách file hiện có với danh sách đã lưu trong checkpoint. Vì file đó đã có tên trong log, Spark xác định nó là file đã hoàn thành và bỏ qua, đảm bảo nguyên tắc xử lý chính xác một lần (Exactly-Once Semantics).

### Câu 6: Vì sao `Window + row_number()` để dedup chạy được trong batch nhưng lỗi trên stream?
- **Trả lời**:
  - **Trong Batch**: Tập dữ liệu là hữu hạn. Phép toán `row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC)` yêu cầu gom toàn bộ các bản ghi của từng `order_id` về cùng một executor partition, sort toàn bộ trong RAM/Disk, và gán số thứ tự từ 1 đến N.
  - **Trong Streaming**: Luồng dữ liệu là vô hạn theo thời gian. Nếu dùng `Window` theo kiểu batch, Spark sẽ phải lưu giữ mãi mãi trong bộ nhớ (State Store) toàn bộ các bản ghi của mọi `order_id` từ ngày này qua tháng khác vì không biết liệu ngày mai có một bản ghi mới với `updated_at` cao hơn xuất hiện hay không $\rightarrow$ Gây tràn bộ nhớ (Out-Of-Memory) không thể kiểm soát.
  - Vì vậy, Spark cấm non-time-based windowing trên streaming DataFrame. Thay vào đó, streaming dedup bắt buộc phải dùng `dropDuplicates(["order_id"])` kết hợp với **Watermark** để Spark biết sau bao lâu thì được phép xóa `order_id` cũ khỏi State Store.

### Câu 7: Trong bài này bước nào gây shuffle?
- **Trả lời**:
  1. Phép tổng hợp **`groupBy("province").agg(...)`**:
     - Phép gom nhóm theo `province` bắt buộc các bản ghi có cùng giá trị `province` (nhưng nằm rải rác ở các file hoặc các partition khác nhau) phải được băm (hash) và truyền qua mạng (network shuffle) về cùng một partition đích để tính tổng `count` và `sum(amount)`.
  2. Phép **`orderBy("province")`** (trong Batch):
     - Việc sắp xếp toàn bộ bảng kết quả theo thứ tự bảng chữ cái của `province` đòi hỏi Range Partitioning và shuffle dữ liệu giữa các node.
  - *Lưu ý*: Các bước đọc CSV, chuẩn hóa `F.upper(status)`, cast `amount`, parse `order_date`, và lọc `filter(is_valid)` là các phép biến đổi cục bộ (Narrow Transformations - `map`/`filter`), được thực thi song song trên từng partition mà hoàn toàn **không gây shuffle**.
