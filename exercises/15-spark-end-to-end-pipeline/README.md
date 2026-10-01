# Bài 15: Spark Structured Streaming - End-to-End Pipeline

Thư mục này ghép nối toàn bộ kiến thức từ ngày 1 đến ngày 4 thành một pipeline hoàn chỉnh:
- **Đọc dữ liệu:** Từ Kafka (streaming) và CSV (static).
- **Làm sạch (Clean & Transform):** Chuẩn hoá kiểu dữ liệu, cast type, filter JSON.
- **Xử lý sự cố (DLQ/Invalid):** Tách luồng dữ liệu hợp lệ và không hợp lệ kèm `error_reason`.
- **Deduplication:** Khử trùng lặp `order_id` lấy bản ghi mới nhất.
- **Join:** Left join dữ liệu luồng với dữ liệu bảng tĩnh.
- **Aggregation & Watermark:** Group theo province và time window, xử lý late data.
- **Multiple Sinks & Checkpointing:** Ghi ra nhiều nhánh Parquet chạy song song với checkpoint độc lập.

## Hướng dẫn chạy (Thực hành)

**Bước 1: Bật Kafka và tạo dữ liệu (Dùng Docker)**
```bash
docker exec -it kafka /bin/bash
kafka-console-producer.sh --topic orders_stream --bootstrap-server localhost:9092
```

**Bước 2: Chạy Streaming Pipeline**
```bash
# Thêm tham số --clean nếu muốn xoá sạch checkpoint cũ trước khi chạy
python end_to_end_pipeline.py --clean
```
Script sẽ giữ trạng thái (running) và in ra terminal các message báo log. Nó sẽ tự động tạo thư mục `output/` và `checkpoints/`.

**Bước 3: Gửi Data từ Producer**
Hãy gửi nhiều loại data vào producer (ví dụ: data chuẩn, data thiếu amount, data trùng order_id nhưng updated_at khác nhau, JSON hỏng...).
*Tip: Gửi một vài message chuẩn, chờ 1-2 phút rồi gửi tiếp để Watermark đóng window và ghi ra report.*

**Bước 4: Kiểm tra đầu ra (Parquet)**
Mở một terminal thứ hai và chạy:
```bash
python verify_outputs.py
```
Script này sẽ dùng Spark batch để đọc các thư mục Parquet trong `output/` và in ra `count`, `schema` cùng dữ liệu mẫu để bạn kiểm tra chéo với những gì đã nhập.

**Bước 5: Thử nghiệm Recovery**
1. Nhấn `Ctrl+C` ở terminal chạy pipeline.
2. Gửi thêm message vào Kafka.
3. Chạy lại `python end_to_end_pipeline.py` (lần này KHÔNG có `--clean`).
4. Chạy lại `verify_outputs.py` để chắc chắn data vẫn được xử lý tiếp nối đầy đủ mà không bị lặp.
