# Báo cáo Tổng kết Tuần 1: Kafka & Spark Structured Streaming

## 1. Viết lại bằng lời của mình (Tổng kết)
* **Batch vs. Streaming:** 
  - **Batch** là gom một lượng lớn dữ liệu (có điểm bắt đầu và kết thúc) rồi mới tính toán một lần, độ trễ cao, thích hợp để chạy báo cáo cuối ngày. 
  - **Streaming** là xử lý các mẩu dữ liệu nhỏ liên tục ngay khi nó sinh ra (không có điểm kết thúc), độ trễ thấp, phục vụ các nhu cầu phân tích realtime.
* **Vai trò của Kafka và Spark:** 
  - **Kafka** đóng vai trò là ống dẫn (Message Broker), nó giữ và chứa dữ liệu (tạm thời hoặc dài ngày) từ nguồn sinh ra, giúp chia tách (decouple) hệ thống tạo data và hệ thống xử lý.
  - **Spark** đóng vai trò là bộ não xử lý (Engine), nó "hút" dữ liệu từ Kafka (Consumer), biến đổi (clean, join, aggregate) và đẩy kết quả sang nơi lưu trữ cuối cùng.
* **Output Mode nào dùng khi nào:**
  - **Complete Mode:** Dùng khi muốn cập nhật lại bảng tính tổng (aggregation) từ đầu đến cuối sau mỗi batch. Chỉ áp dụng được nếu có aggregate.
  - **Update Mode:** Tương tự Complete nhưng tối ưu hơn, chỉ xuất ra những kết quả bị thay đổi hoặc mới phát sinh.
  - **Append Mode:** Dùng phổ biến nhất khi không có aggregate (dữ liệu cứ thế chèn thêm vào file/database). Nếu có aggregate, buộc phải có Watermark để Spark xác định bản ghi nào đã "khoá", không thay đổi nữa mới được đẩy ra.
* **Checkpoint:** Đóng vai trò làm "điểm đánh dấu an toàn". Nó lưu lại Spark đã đọc Kafka tới vị trí (offset) nào và trạng thái aggregate trước đó là bao nhiêu, giúp Spark có thể tiếp tục tự động khi bị sập hay khởi động lại (Recovery).
* **Watermark:** Giải quyết bài toán "Late Data" (Dữ liệu đến trễ). Nó quy định thời hạn tối đa mà hệ thống chờ đợi dữ liệu cũ. Qua thời hạn đó, dữ liệu đến muộn sẽ bị bỏ qua để hệ thống có thể kết thúc window, giải phóng bộ nhớ (State) và xuất kết quả an toàn.

---

## 2. Trả lời câu hỏi Yêu cầu 6

### once / availableNow phù hợp với kiến trúc nào hơn?
`availableNow=True` (hoặc `once=True` cũ) phù hợp nhất với kiến trúc **Chạy theo lịch định kỳ (Batching/Cron jobs/Airflow)**. 
Thay vì giữ cụm (cluster) Spark chạy 24/7 (tốn tài nguyên), ta thiết lập Airflow kích hoạt job Spark 1 giờ/lần. Spark sẽ bật lên, đọc *toàn bộ* dữ liệu có sẵn trong Kafka (kể từ offset lưu trong checkpoint gần nhất), xử lý hết, rồi tự động tắt (tiết kiệm chi phí) mà vẫn giữ nguyên được các logic stateful/checkpoint của Streaming.

### foreachBatch cho phép làm những việc gì mà writeStream thông thường không làm được trực tiếp?
1. **Ghi nhiều Sink:** Dễ dàng ghi ra Parquet, MySQL, và Console trong cùng một batch bằng cách lấy DataFrame nội bộ ra (mà không phải đọc Kafka 3 lần độc lập với 3 checkpoint phức tạp như ở Ngày 5).
2. **Dùng các hàm Batch:** Cho phép dùng các Transformation không được hỗ trợ trong Streaming (ví dụ: dùng hàm `Window` với `partitionBy` để khử trùng lặp phức tạp, hoặc thực hiện Sort toàn cục).
3. **Thao tác External:** Cho phép gọi các API ngoại vi, lưu DB truyền thống không có sink connector chuẩn, hoặc thực thi câu lệnh SQL tuỳ biến sau khi ghi xong một chunk dữ liệu.

### Sau một tuần, phần nào của Structured Streaming thấy khó nắm nhất?

- **Window + Watermark State:** Rất dễ bị nhầm lẫn giữa Event-time và Processing-time. Spark quản lý trạng thái (stateStore) ngầm, đôi khi gây khó hiểu tại sao dữ liệu bị drop hoặc tại sao kết quả append mãi chưa chịu in ra màn hình.
- **Checkpoint Compatibility:** Lỗi "Metadata Mismatch" do sửa đổi schema hoặc thay đổi outputMode. Cảm giác checkpoint của Spark quá cứng nhắc, nếu đổi logic code thì thường phải xoá toàn bộ thư mục checkpoint và chịu khó chạy lại từ đầu.

---

## 3. Câu hỏi và phần chưa chắc 
1. **Quản lý kích thước thư mục Checkpoint:** Làm sao để thư mục checkpoint không phình to ra theo thời gian khi state ngày càng lớn?
2. **Tối ưu Watermark:** Làm sao để chọn giá trị delay hợp lý cho Watermark mà không ảnh hưởng lớn đến độ trễ hiển thị (latency)? Dựa vào thông số nào từ Kafka để biết data thường đến trễ bao lâu?
3. **Kafka offset vs. Spark Checkpoint:** Nếu một ngày thư mục Checkpoint bị lỡ mất hoặc mất đồng bộ hoàn toàn với Kafka retention thì làm sao phục hồi luồng dữ liệu mà không bị duplicate?
