# Bài 18: Kỹ thuật bốc mẫu ngẫu nhiên (Sampling) và Đánh giá hiệu năng ETL theo quy mô dữ liệu

## 1. Tên chapter/bài
Bài 18: Lấy mẫu dữ liệu và Benchmark quy trình ETL trên 1M và 10M bản ghi.

## 2. Mục tiêu
- Thực hành thao tác lấy mẫu (sampling) ngẫu nhiên từ tập dữ liệu khổng lồ (100 triệu dòng) để tạo ra các tập dữ liệu nhỏ hơn (1 triệu và 10 triệu dòng) nhằm mục đích phát triển và test (Dev/Test).
- Tái sử dụng lại luồng ETL chuẩn Medallion (đã chia làm Bronze, Silver, Gold).
- Đo đếm, so sánh thời gian thực thi (Benchmark) giữa tập 1M và tập 10M để quan sát quy luật phình to thời gian xử lý và tài nguyên khi Data Scale tăng lên.

## 3. Bối cảnh dữ liệu / khái niệm liên quan
- Trong môi trường thực tế, việc code và chạy thử trực tiếp trên cụm dữ liệu siêu lớn (hàng chục GB/TB) gây lãng phí tài nguyên và làm chậm quá trình sửa lỗi code.
- Mẫu dữ liệu (Sampled Data) phải đảm bảo độ ngẫu nhiên để vẫn giữ được tính đa dạng và tỷ lệ phân phối đặc trưng (ví dụ: vẫn đủ các loại status, category, null fields...).

## 4. Phương pháp / kỹ thuật áp dụng
- **`df.sample(withReplacement=False, fraction=0.01)`**: Kĩ thuật trích xuất xấp xỉ 1% dữ liệu. Ưu điểm là phân tán đều đặn qua các partition (không bị hớt váng toàn bộ dữ liệu ở 1 file duy nhất).
- **Benchmarking**: Bao bọc các hàm `run_bronze`, `run_silver`, `run_gold` trong Python `time.time()` để bắt chính xác thời gian IO và Shuffle của Spark.

## 5. Code đã viết
- `files (5)/scripts/create_samples.py`: Script dùng Spark để đọc toàn bộ 100M dòng, bốc mẫu 1% (cho ra tập `raw_1m`) và 10% (cho ra tập `raw_10m`).
- `files (5)/scripts/benchmark_etl.py`: Lần lượt gọi 3 module `etl_bronze`, `etl_silver`, `etl_gold` và ghi nhận lại thời gian chạy (tính bằng giây).

## 6. Kết quả chạy thử (Benchmark)
*(Đang cập nhật...)*

## 7. Lỗi gặp phải và cách xử lý
- **Lỗi tràn RAM (Out Of Memory / Py4JNetworkError)**: Xảy ra trong Bài 17 khi chạy 100M trên môi trường cục bộ. 
  - **Cách xử lý**: Yêu cầu bắt buộc phải truyền thông số `spark.driver.memory="8g"` và `spark.executor.memory="8g"` để cấp đủ RAM cho quá trình trộn dữ liệu (shuffle) tại node cục bộ.
- **Lỗi in chữ tiếng Việt (UnicodeEncodeError)**: Windows Terminal mặc định không hỗ trợ in chữ `Đ`.
  - **Cách xử lý**: Thiết lập biến môi trường `$env:PYTHONIOENCODING="utf-8"` trước khi chạy script.

## 8. Kết luận / ghi chú
- Khi dữ liệu tăng x10 (từ 1M lên 10M), thời gian xử lý thường... (Đang đợi kết quả đo).
- Việc chia dữ liệu ra 3 file chạy và 3 lớp (Medallion) giúp ta có thể chạy lại riêng lẻ một công đoạn bị lỗi (vd: lỗi ở Gold thì chỉ chạy lại hàm Gold mà không phải load lại từ đầu).

## 9. Plan cho buổi sau
- Làm quen với Spark Web UI để xem chi tiết DAG và Execution Plan thực sự đằng sau lớp code.
- Áp dụng Caching/Persist vào lớp Silver để tăng tốc độ ghi ra 3 bảng Gold.
