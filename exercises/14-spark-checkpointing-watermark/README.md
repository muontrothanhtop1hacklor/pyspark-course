# Bài 14: Spark Structured Streaming - Checkpoint, Output Modes & Watermark

Thư mục này chứa mã nguồn thực hành và báo cáo cho các tính năng nâng cao của Spark Structured Streaming: Output Modes, Watermark (xử lý late data), và Checkpointing (phục hồi lỗi).

## Nội dung thư mục
- `customers.csv`: Dữ liệu tĩnh giả lập chứa thông tin khách hàng (Dùng để thực hiện Stream-Static Join).
- `req1_output_modes.py`: Script thử nghiệm sự khác biệt giữa các Output Mode (`complete`, `update`, `append`).
- `req2_watermark.py`: Script triển khai Watermark kết hợp với Group By Window để cho phép lưu trữ và in ra kết quả bằng `append` mode.
- `req3_4_checkpoint_failure.py`: Script giả lập các trường hợp liên quan đến ghi nhận trạng thái (checkpoint) và xử lý sự cố (ngắt kết nối, lỗi lệch metadata).
- `kafka_notes.md`: Báo cáo chi tiết tổng kết các hiện tượng và lý thuyết quan sát được sau khi chạy các file trên.
- `checkpoint_dir/`: Thư mục (tự sinh ra khi chạy script 3) chứa metadata về commit, offset, và state để phục hồi stream.

## Hướng dẫn chạy (Thực hành)

**Bước 1: Chạy Kafka và tạo dữ liệu (Dùng Docker từ Bài 12)**
Bạn cần đảm bảo container Kafka đang chạy và tạo producer để gửi JSON messages:
```bash
docker exec -it kafka /bin/bash
kafka-console-producer.sh --topic orders_stream --bootstrap-server localhost:9092
```
*Gửi vào producer các message JSON (có thể lấy ví dụ từ ngày 2).*

**Bước 2: Chạy kiểm tra các Output Mode**
Chạy `req1_output_modes.py` với tham số là tên mode muốn thử:
```bash
python req1_output_modes.py complete
python req1_output_modes.py update
python req1_output_modes.py append  # Sẽ gặp lỗi vì chưa có watermark
```
Gửi message mới vào producer và quan sát log in ra trên console Spark.

**Bước 3: Chạy kiểm tra Watermark**
Để chạy thành công `append` mode khi có aggregate, chạy file:
```bash
python req2_watermark.py
```
Gửi các message có `updated_at` tuân thủ watermark đã cài đặt và chờ xem console in ra dữ liệu khi window đóng lại.

**Bước 4: Chạy kiểm tra Checkpoint và giả lập lỗi**
Chạy script kiểm tra checkpoint:
```bash
python req3_4_checkpoint_failure.py
```
Trong lúc đang chạy:
1. Bạn có thể nhấn `Ctrl+C` để tắt script đi, sau đó chạy lại y hệt câu lệnh trên để xem dữ liệu có được đọc tiếp nối không bị lặp không.
2. Thử thay đổi logic bằng argument: `python req3_4_checkpoint_failure.py change` (Dự kiến báo lỗi metadata).
3. Thử xoá dọn checkpoint cũ để chạy lại từ đầu: `python req3_4_checkpoint_failure.py clear`.
