# Báo cáo thực hành: Spark Batch và Structured Streaming

Tài liệu này ghi nhận kết quả thực nghiệm và lý giải cơ chế hoạt động khi đối chiếu giữa Spark Batch và Spark Structured Streaming trên cùng một tập dữ liệu đơn hàng.

---

## 1. Dữ liệu gốc và số liệu kiểm tra đối chiếu

Dữ liệu gồm 30 dòng được chia đều vào 3 tệp CSV đặt trong thư mục data/batch_input:
- orders_1.csv: 10 dòng, gồm 7 dòng hợp lệ và 3 dòng lỗi.
- orders_2.csv: 10 dòng, gồm 7 dòng hợp lệ và 3 dòng lỗi; chứa order_id 1001 xuất hiện lại từ file 1 với updated_at mới hơn.
- orders_3.csv: 10 dòng, gồm 7 dòng hợp lệ và 3 dòng lỗi; chứa order_id 1002 xuất hiện lại từ file 1 với updated_at mới hơn.

### Chi tiết các bản ghi và phân loại lỗi

| File | order_id | province | amount | status | order_date | updated_at | Trạng thái kiểm tra |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| orders_1 | 1001 | HaNoi | 1,000,000 | SUCCESS | 2025-01-05 | 2025-01-05 08:30:00 | Hợp lệ (1.0M) |
| | 1002 | HoChiMinh | 2,500,000 | completed | 2025-01-06 | 2025-01-06 09:15:00 | Hợp lệ (2.5M) |
| | 1003 | DaNang | 1,500,000 | PENDING | 2025-01-07 | 2025-01-07 10:00:00 | Hợp lệ (1.5M) |
| | 1004 | CanTho | 800,000 | COMPLETED | 2025-01-08 | 2025-01-08 11:20:00 | Hợp lệ (0.8M) |
| | 1005 | HaNoi | 0.0 | SUCCESS | 2025-01-09 | 2025-01-09 14:00:00 | Lỗi amount bằng 0 |
| | 1006 | HaiPhong | 1,200,000 | Shipping | 2025-01-10 | 2025-01-10 15:30:00 | Hợp lệ (1.2M) |
| | 1007 | HoChiMinh | -50,000 | SUCCESS | 2025-01-11 | 2025-01-11 16:45:00 | Lỗi amount âm |
| | 1008 | DaNang | 3,000,000 | COMPLETED | 31/02/2025 | 2025-01-12 17:00:00 | Lỗi ngày không hợp lệ |
| | 1009 | CanTho | 600,000 | SUCCESS | 2025-01-13 | 2025-01-13 18:10:00 | Hợp lệ (0.6M) |
| | 1010 | HaNoi | 2,000,000 | Pending | 2025-01-14 | 2025-01-14 19:30:00 | Hợp lệ (2.0M) |
| orders_2 | 1001 | HaNoi | 1,500,000 | COMPLETED | 2025-01-15 | 2025-01-15 10:00:00 | Hợp lệ (1.5M, trùng order_id 1001) |
| | 1011 | HoChiMinh | 4,000,000 | SUCCESS | 2025-01-16 | 2025-01-16 11:00:00 | Hợp lệ (4.0M) |
| | 1012 | DaNang | N/A | COMPLETED | 2025-01-17 | 2025-01-17 12:30:00 | Lỗi amount dạng chuỗi chữ |
| | 1013 | CanTho | 1,100,000 | pending | 2025-01-18 | 2025-01-18 13:45:00 | Hợp lệ (1.1M) |
| | 1014 | HaiPhong | 900,000 | SUCCESS | 2025-01-19 | 2025-01-19 14:15:00 | Hợp lệ (0.9M) |
| | 1015 | HaNoi | 1,800,000 | COMPLETED | 2025-13-40 | 2025-01-20 15:00:00 | Lỗi ngày sai định dạng |
| | 1016 | HoChiMinh | 3,200,000 | Shipping | 2025-01-21 | 2025-01-21 16:20:00 | Hợp lệ (3.2M) |
| | 1017 | DaNang | 2,200,000 | SUCCESS | 2025-01-22 | 2025-01-22 17:35:00 | Hợp lệ (2.2M) |
| | 1018 | CanTho | *(rỗng)* | COMPLETED | 2025-01-23 | 2025-01-23 18:40:00 | Lỗi amount bị null |
| | 1019 | HaNoi | 2,700,000 | SUCCESS | 2025-01-24 | 2025-01-24 19:50:00 | Hợp lệ (2.7M) |
| orders_3 | 1002 | HoChiMinh | 2,800,000 | COMPLETED | 2025-01-25 | 2025-01-25 09:00:00 | Hợp lệ (2.8M, trùng order_id 1002) |
| | 1020 | DaNang | 1,700,000 | SUCCESS | 2025-01-26 | 2025-01-26 10:15:00 | Hợp lệ (1.7M) |
| | 1021 | CanTho | 500,000 | pending | 2025-01-27 | 2025-01-27 11:30:00 | Hợp lệ (0.5M) |
| | 1022 | HaiPhong | -100,000 | FAILED | 2025-01-28 | 2025-01-28 12:45:00 | Lỗi amount âm |
| | 1023 | HaNoi | 3,500,000 | COMPLETED | *(rỗng)* | 2025-01-29 13:50:00 | Lỗi order_date bị null |
| | 1024 | HoChiMinh | 1,900,000 | SUCCESS | 2025-01-30 | 2025-01-30 14:20:00 | Hợp lệ (1.9M) |
| | 1025 | DaNang | 0.0 | CANCELLED | 2025-01-31 | 2025-01-31 15:10:00 | Lỗi amount bằng 0 |
| | 1026 | CanTho | 1,300,000 | Shipping | 2025-02-01 | 2025-02-01 16:00:00 | Hợp lệ (1.3M) |
| | 1027 | HaiPhong | 2,100,000 | SUCCESS | 2025-02-02 | 2025-02-02 17:15:00 | Hợp lệ (2.1M) |
| | 1028 | HaNoi | 4,500,000 | SUCCESS | 2025-02-03 | 2025-02-03 18:30:00 | Hợp lệ (4.5M) |

Số liệu tính toán đối chiếu ban đầu:
- Tổng số dòng: 30 dòng.
- Số dòng lỗi: 9 dòng.
- Số dòng hợp lệ: 21 dòng.
- Tổng amount hợp lệ: 40,000,000 VND.
- Phân bổ theo tỉnh: CanTho 5 đơn (4,300,000 VND), DaNang 3 đơn (5,400,000 VND), HaiPhong 3 đơn (4,200,000 VND), HaNoi 5 đơn (11,700,000 VND), HoChiMinh 5 đơn (14,400,000 VND).

---

## 2. Kết quả Yêu cầu 1: Batch Processing

Tập lệnh `req1_batch.py` đọc toàn bộ thư mục `data/batch_input`, thực hiện lọc và tính toán trong một lần chạy duy nhất.

Kết quả thu được trên console:
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

Số lượng đơn hợp lệ ghi nhận là 21 và tổng doanh thu đạt 40,000,000 VND. Giá trị này khớp với kết quả tính toán trước đó.

---

## 3. Kết quả Yêu cầu 2: Streaming cùng logic

Tập lệnh `req2_streaming_console.py` thiết lập cấu hình readStream với `maxFilesPerTrigger=1`, `trigger(processingTime="5 seconds")` và `outputMode("complete")`. Ba tệp CSV lần lượt được nạp vào thư mục `data/stream_input`.

| Micro-batch | Tệp vừa nạp | Kết quả trên console | Khác biệt so với batch liền trước |
| :--- | :--- | :--- | :--- |
| Batch 0 | orders_1.csv | HaNoi: 2 đơn (3.0M)<br>CanTho: 2 đơn (1.4M)<br>HoChiMinh: 1 đơn (2.5M)<br>HaiPhong: 1 đơn (1.2M)<br>DaNang: 1 đơn (1.5M)<br>Tổng: 7 đơn (9.6M) | Khởi tạo State Store ban đầu. Bảng kết quả phản ánh 7 dòng hợp lệ đầu tiên của orders_1.csv. |
| Batch 1 | orders_2.csv | HaNoi: 4 đơn (7.2M)<br>CanTho: 3 đơn (2.5M)<br>HoChiMinh: 3 đơn (9.7M)<br>HaiPhong: 2 đơn (2.1M)<br>DaNang: 2 đơn (3.7M)<br>Tổng: 14 đơn (25.2M) | State Store tích lũy các dòng mới vào trạng thái cũ. Do dùng complete mode, console xuất toàn bộ bảng kết quả tổng hợp mới nhất thay vì chỉ xuất phần chênh lệch. |
| Batch 2 | orders_3.csv | HaNoi: 5 đơn (11.7M)<br>CanTho: 5 đơn (4.3M)<br>HoChiMinh: 5 đơn (14.4M)<br>HaiPhong: 3 đơn (4.2M)<br>DaNang: 3 đơn (5.4M)<br>Tổng: 21 đơn (40.0M) | State Store tiếp tục cộng dồn 7 dòng hợp lệ từ orders_3.csv. Tổng cộng có 21 đơn hợp lệ với 40,000,000 VND. |

---

## 4. Yêu cầu 3: So sánh Batch và Streaming

Kết quả tính toán sau khi luồng hoàn tất xử lý 3 tệp tin trùng khớp với phương pháp xử lý theo lô ở Yêu cầu 1. Tổng số dòng hợp lệ và doanh số từng tỉnh thành không có sự sai lệch.

Ba điểm khác nhau giữa hai phương pháp:
1. Về cú pháp và khai báo: Xử lý theo lô sử dụng `spark.read` trên tập dữ liệu tĩnh và không đòi hỏi cấu hình checkpoint hay trigger. Xử lý luồng sử dụng `spark.readStream`, bắt buộc người dùng cung cấp schema tĩnh, xác định chu kỳ trigger, chế độ xuất (outputMode) và đường dẫn checkpointLocation.
2. Về chu kỳ thực thi: Xử lý theo lô chỉ quét dữ liệu một lần duy nhất rồi giải phóng tài nguyên. Xử lý luồng duy trì tiến trình nền chạy liên tục, định kỳ quét nguồn dữ liệu theo chu kỳ trigger và cập nhật trạng thái State Store.
3. Về cơ chế xuất kết quả: Lệnh `.show()` trong batch chỉ in một bảng kết quả tĩnh cuối cùng. Trong streaming với complete mode, console liên tục in lại toàn bộ bảng tổng hợp mới nhất mỗi khi một micro-batch hoàn thành.

---

## 5. Yêu cầu 4: Thử nghiệm và quan sát lỗi

Tập lệnh `req4_experiments.py` ghi nhận các lỗi thực tế phát sinh:

### Thử nghiệm 1: Không khai báo schema cho readStream
Thao tác load dừng lại và trả về ngoại lệ:
```text
java.lang.IllegalArgumentException: Schema must be specified when creating a streaming source DataFrame.
```
Cơ chế streaming không tự động suy diễn kiểu dữ liệu của các tệp tin trong tương lai nhằm bảo đảm tính nhất quán của kế hoạch thực thi.

### Thử nghiệm 2: Đổi chu kỳ trigger thành 20 giây
Khi nạp tệp orders_1.csv, dữ liệu không được xử lý ngay mà phải chờ đến mốc chu kỳ 20 giây tiếp theo của đồng hồ trigger. Độ trễ xử lý phụ thuộc trực tiếp vào khoảng thời gian trigger đã thiết lập.

### Thử nghiệm 3: Nạp lại cùng một tệp tin đã xử lý
Sau khi ghi đè lại file orders_1.csv vào stream_input, số dòng đọc vào của micro-batch tiếp theo bằng 0. Spark kiểm tra nhật ký FileStreamSourceLog trong checkpoint và bỏ qua tệp tin đã có đường dẫn ghi nhận trước đó.

### Thử nghiệm 4: Sử dụng outputMode append trên phép toán aggregate
Spark dừng câu lệnh start và trả về ngoại lệ:
```text
pyspark.errors.exceptions.captured.AnalysisException: 
[STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION] Invalid streaming output mode: append. 
This output mode is not supported for streaming aggregations without watermark on streaming DataFrames/DataSets.
```
Khi thiếu watermark, hệ thống không xác định được thời điểm đóng của một nhóm dữ liệu tổng hợp để xuất ra theo chế độ append.

### Thử nghiệm 5: Khử trùng lặp bằng Window và row_number
Spark từ chối kế hoạch thực thi với thông báo:
```text
pyspark.errors.exceptions.captured.AnalysisException: 
[NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING] Window function is not supported in ROW_NUMBER() 
on streaming DataFrames/Datasets. Structured Streaming only supports time-window aggregation using the WINDOW function.
```
Hàm Window không dựa trên mốc thời gian đòi hỏi lưu trữ vô hạn toàn bộ lịch sử bản ghi trong bộ nhớ. Structured Streaming yêu cầu dùng `dropDuplicates` đi kèm watermark thay cho hàm window thông thường.

---

## 6. Yêu cầu 5: Ghi Parquet và cấu trúc checkpoint

Tập lệnh `req5_write_and_check.py` ghi 21 bản ghi hợp lệ ra định dạng Parquet theo hai cơ chế: ghi đè ở batch và append có checkpoint ở streaming.

Kết quả kiểm tra đối chiếu:
- Số lượng bản ghi: Batch đạt 21 dòng, Streaming đạt 21 dòng.
- Tổng amount: Batch đạt 40,000,000 VND, Streaming đạt 40,000,000 VND.
- Schema: Hai bên đồng nhất về kiểu dữ liệu struct gồm order_id, customer_id, province, amount_clean, status_clean, order_date_clean và updated_at.

Cấu trúc thư mục tại `checkpoint/stream_parquet_write`:
- `metadata`: Chứa runId định danh luồng truy vấn.
- `commits/`: Lưu trữ các số hiệu micro-batch đã ghi thành công ra tệp Parquet.
- `offsets/`: Ghi nhận vị trí offset của nguồn dữ liệu cho từng batch.
- `sources/0/`: Nhật ký FileStreamSourceLog lưu danh sách tệp tin CSV đã đọc vào hệ thống.

---

## 7. Trả lời các câu hỏi lý thuyết

### Vì sao gọi Structured Streaming là bảng vô hạn (Unbounded Table)?
Mô hình Structured Streaming trừu tượng hóa luồng dữ liệu liên tục dưới dạng một bảng quan hệ không giới hạn kích thước. Mỗi bản ghi mới phát sinh được xem như một dòng được nối tiếp vào cuối bảng. Kỹ sư xây dựng câu truy vấn tương tự như trên bảng tĩnh, sau đó Spark tự động chuyển dịch thành kế hoạch thực thi gia tăng liên tục.

### Micro-batch là gì và tương ứng với điều gì trên console?
Micro-batch là phương pháp gom các sự kiện dữ liệu phát sinh liên tục thành từng lô nhỏ theo chu kỳ thời gian rồi xử lý bằng một công việc nội bộ của Spark. Khi thiết lập `maxFilesPerTrigger=1`, mỗi micro-batch tương ứng với một tệp CSV mới xuất hiện trong thư mục đầu vào.

### Vì sao readStream yêu cầu khai báo schema?
Thư mục nguồn của luồng dữ liệu có thể đang rỗng tại thời điểm khởi động ứng dụng và các tệp trong tương lai có thể biến động. Việc yêu cầu schema đóng vai trò như một giao ước cấu trúc dữ liệu cố định, hỗ trợ bộ tối ưu hóa Catalyst lập kế hoạch thực thi trước khi dữ liệu thực tế xuất hiện.

### Với complete mode, mỗi micro-batch in ra dữ liệu mới hay toàn bộ kết quả?
Console in ra toàn bộ kết quả tổng hợp tích lũy từ thời điểm bắt đầu cho tới micro-batch hiện tại. Dữ liệu từ các tệp trước đó vẫn được giữ lại và cộng dồn cùng dữ liệu mới trong State Store.

### Thả lại cùng một file thì Spark xử lý lại hay bỏ qua?
Spark bỏ qua tệp tin đó. Thư mục checkpoint lưu nhật ký danh sách tệp tin đã xử lý tại đường dẫn `sources/0`. Mỗi khi quét thư mục, hệ thống đối chiếu đường dẫn của tệp với nhật ký này để ngăn chặn việc xử lý lặp lại.

### Vì sao Window kết hợp row_number chạy được trong batch nhưng lỗi trên stream?
Trong xử lý theo lô, tập dữ liệu là hữu hạn nên hệ thống có thể gom toàn bộ các dòng về phân vùng để sắp xếp thứ tự. Trong xử lý luồng, dữ liệu đến liên tục không giới hạn; nếu duy trì hàm cửa sổ không có thời gian kết thúc, bộ nhớ State Store sẽ phải lưu trữ trạng thái mãi mãi và dẫn đến lỗi tràn bộ nhớ. Thay vào đó, streaming yêu cầu sử dụng hàm `dropDuplicates` cùng cơ chế watermark để dọn dẹp các bản ghi cũ.

### Bước nào trong bài gây ra hiện tượng shuffle?
Phép toán `groupBy("province").agg(...)` gây ra shuffle vì các bản ghi có cùng giá trị province nằm rải rác ở nhiều phân vùng khác nhau cần được hoán chuyển qua mạng để gom về cùng một phân vùng tính toán. Ngoài ra, phép sắp xếp `orderBy("province")` trong xử lý theo lô cũng yêu cầu shuffle dữ liệu để sắp xếp toàn cục. Các bước đọc, chuẩn hóa chuỗi, ép kiểu và lọc dữ liệu là các phép biến đổi hẹp (narrow transformation) diễn ra cục bộ và không gây shuffle.
