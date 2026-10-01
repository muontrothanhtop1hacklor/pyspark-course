# Báo cáo Thực hành Ngày 4: Spark Structured Streaming - Checkpoint, Output Modes & Watermark

## Yêu cầu 1: Output Mode trên Aggregate
1. **complete mode**:
   - Đặc điểm: Mỗi khi có một batch dữ liệu mới tới, Spark sẽ tính toán lại toàn bộ kết quả aggregate từ trước đến nay và ghi đè/in ra toàn bộ kết quả (toàn bộ bảng kết quả).
   - Quan sát: Bảng kết quả in ra luôn chứa tất cả các group (ở đây là province) và tổng số cập nhật nhất, dù batch đó có chứa province đó hay không. 
   - Điều kiện: Chỉ hỗ trợ các truy vấn có aggregation.

2. **update mode**:
   - Đặc điểm: Tương tự complete nhưng chỉ in ra/ghi những dòng trong bảng kết quả đã bị thay đổi hoặc mới thêm vào trong batch hiện tại.
   - Quan sát: Khi gửi message mới vào Kafka, console chỉ in ra các province có số lượng order/amount thay đổi. Các province không có message mới trong batch sẽ không hiện.

3. **append mode**:
   - Đặc điểm: Chỉ xuất những dòng kết quả "cuối cùng" (sẽ không bao giờ bị thay đổi nữa).
   - Lỗi nhận được khi không có Watermark: `Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark`.
   - Giải thích: Spark không biết khi nào dữ liệu cho một khoá aggregation (ví dụ province) thực sự dừng lại, nên không thể chắc chắn kết quả aggregate sẽ không thay đổi. Do đó nó không thể xuất dữ liệu "cuối cùng" ra được.

## Yêu cầu 2: Watermark
- **Tác dụng**: Giới hạn thời gian chờ dữ liệu trễ (late data). Spark sẽ loại bỏ (không cộng gộp) những record nào có timestamp (order_date/updated_at) nhỏ hơn `Max(EventTime_so_far) - Watermark_delay`.
- **Kết hợp với Append mode**:
  - Khi thêm watermark (ví dụ 1 minute) và group by thêm Window (ví dụ 5 minutes), Spark có thể sử dụng `append` mode.
  - Khi một window trôi qua (thời gian event_time lớn nhất vượt qua thời điểm kết thúc của window + watermark), Spark biết chắc chắn sẽ không cập nhật window đó nữa, và sẽ "đẩy" dòng kết quả đó ra sink (in ra console).

## Yêu cầu 3: Checkpointing
- **Mục đích**: Lưu trữ trạng thái (state) và các offset đã xử lý vào thư mục an toàn (checkpointLocation).
- **Quan sát thư mục checkpoint**:
  - Khi chạy, Spark tạo ra các thư mục con trong `checkpointLocation`: `offsets`, `commits`, `sources`, `sinks`, `state`.
- **Restart (Khôi phục)**:
  - Khi tắt script và bật lại (giữ nguyên thư mục checkpoint), ứng dụng tự động đọc `offsets` và `commits` để tiếp tục xử lý chính xác từ nơi nó dừng lại (không xử lý lặp, không bỏ sót).
- **Lỗi Metadata Mismatch**:
  - Khi thay đổi logic truy vấn (đổi query, đổi kiểu schema của state) hoặc thay đổi `outputMode` rồi restart với thư mục checkpoint cũ, Spark báo lỗi do logic hoặc schema state lưu trong checkpoint không khớp với mã nguồn hiện tại.
  - Khắc phục: Phải xóa thư mục checkpoint cũ (hoặc chỉ định thư mục mới) để chạy lại.

## Yêu cầu 4: Lỗi và Khắc phục (Failure Scenarios)
- **Tắt Kafka đột ngột**:
  - Lỗi: `NetworkException`, `TimeoutException` hoặc `OffsetOutOfRangeException` (tuỳ thuộc vào thời điểm ngắt kết nối).
- **Phục hồi**:
  - Nếu Kafka bật lại trong khi ứng dụng Spark vẫn đang chạy và đang retry (nếu có cấu hình hoặc do cơ chế mặc định của client), luồng sẽ tự khôi phục kết nối và đọc tiếp.
  - Nếu ứng dụng Spark bị crash, ta bật lại ứng dụng (có checkpoint) -> Ứng dụng đọc lại data từ offset cuối cùng chưa commit.
- **Xóa checkpoint và chạy lại**:
  - Nếu xóa checkpoint: Spark coi đây là 1 job hoàn toàn mới.
  - Offset sẽ chạy từ đầu (`startingOffsets="earliest"`) hoặc chạy từ offset mới nhất (`"latest"`), tuỳ vào cấu hình `startingOffsets` trong mã nguồn. Dữ liệu cũ đã lưu trong sink (nếu có) có thể bị duplicate nếu không được thiết kế idempotent.
