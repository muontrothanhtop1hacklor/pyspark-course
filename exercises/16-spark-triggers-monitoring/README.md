# Bài 16: Spark Structured Streaming - Trigger, Multi-Sink, Monitoring & Tổng kết

Thư mục này chứa các file bài tập minh hoạ cho các tính năng quản lý vòng đời và xuất dữ liệu của Spark Structured Streaming.

## Nội dung
- `req1_3_triggers_monitoring.py`: Script dùng để chạy streaming với các loại Trigger khác nhau (`processingTime`, `availableNow`, `once`) và truy xuất thông tin từ `query.status`, `query.lastProgress`.
- `req2_foreachbatch_multisink.py`: Thay vì tạo nhiều query chạy song song, script này chỉ dùng 1 query duy nhất nhưng phân luồng dữ liệu (cache) và ghi ra nhiều đích (console + parquet) trong cùng một batch bằng `foreachBatch`.
- `kafka_notes.md`: Bản tổng kết toàn bộ kiến thức tuần 1, giải thích chi tiết về khác biệt Batch vs Streaming, chức năng từng loại Mode, cơ chế Checkpoint/Watermark và giải đáp các câu hỏi trong Yêu cầu 4.

## Hướng dẫn chạy
**1. Khởi động Kafka & Producer**
Tương tự các bài trước, bạn cần có Kafka đang chạy ở cổng `9092` và topic `orders_stream` có chứa dữ liệu.

**2. Test Trigger & Monitoring**
```bash
# Mặc định là processingTime="5 seconds"
python req1_3_triggers_monitoring.py

# Để test trigger availableNow (chạy hết data rồi dừng)
python req1_3_triggers_monitoring.py availableNow
```

**3. Test Multi-Sink (foreachBatch)**
```bash
python req2_foreachbatch_multisink.py
```
Bạn sẽ thấy trên console dữ liệu được in ra cho từng batch, đồng thời Parquet file được sinh ra trong thư mục `output/multisink_parquet`.
