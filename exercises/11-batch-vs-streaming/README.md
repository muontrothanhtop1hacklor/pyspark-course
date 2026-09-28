# Bài 11: So Sánh Spark Batch và Structured Streaming

Thư mục này triển khai trọn vẹn bài thực hành đối chiếu toàn diện giữa mô hình **Batch Processing** và **Structured Streaming** trên Apache Spark.

---

## 📁 Cấu Trúc Thư Mục

```text
exercises/11-batch-vs-streaming/
├── generate_streaming_data.py   # Tạo 30 dòng dữ liệu mẫu tách vào 3 file CSV
├── req1_batch.py                 # Yêu cầu 1: Đọc Batch, clean và aggregate theo province
├── req2_streaming_console.py     # Yêu cầu 2: Streaming console complete mode, maxFilesPerTrigger=1
├── req4_experiments.py           # Yêu cầu 4: 5 thử nghiệm quan sát lỗi và hành vi đặc thù của stream
├── req5_write_and_check.py       # Yêu cầu 5: Ghi Parquet (Batch vs Streaming append), đối chiếu & khám phá checkpoint
├── BAO_CAO_LAB_STREAMING.md      # Báo cáo chi tiết: Bảng theo dõi micro-batch, đối chiếu số liệu & trả lời 7 câu hỏi
├── README.md                     # Tài liệu hướng dẫn sử dụng
├── data/
│   ├── source_files/             # File gốc orders_1.csv, orders_2.csv, orders_3.csv
│   ├── batch_input/              # Chứa cả 3 file phục vụ đọc Batch
│   ├── stream_input/             # Thư mục trống dùng để nạp file cho streaming
│   └── stream_input_write/       # Thư mục nạp file cho bài test ghi Parquet
├── output/
│   ├── batch_parquet/            # Kết quả Parquet từ Batch
│   └── stream_parquet/           # Kết quả Parquet từ Streaming
└── checkpoint/
    ├── stream_console/           # Checkpoint cho console complete stream
    ├── stream_exp/               # Checkpoint cho các thử nghiệm
    └── stream_parquet_write/     # Checkpoint cho streaming Parquet append
```

---

## 🚀 Hướng Dẫn Chạy Từng Bước

### Bước 0: Sinh dữ liệu mẫu
```bash
python exercises/11-batch-vs-streaming/generate_streaming_data.py
```

### Bước 1: Chạy Batch Processing (Yêu cầu 1)
```bash
python exercises/11-batch-vs-streaming/req1_batch.py
```
- Khai báo schema tĩnh, đọc cả thư mục `batch_input/`.
- Chuẩn hóa `status`, cast `amount`, parse `order_date`.
- Lọc 21 dòng hợp lệ, tính tổng theo province.

### Bước 2: Chạy Structured Streaming (Yêu cầu 2)
```bash
python exercises/11-batch-vs-streaming/req2_streaming_console.py
```
- Đọc `stream_input/` với `maxFilesPerTrigger=1`, `trigger("5 seconds")`, `outputMode("complete")`.
- Tự động nạp lần lượt `orders_1.csv`, `orders_2.csv`, `orders_3.csv`.
- Quan sát Console in ra kết quả lũy kế qua Micro-batch 0, 1, 2.

### Bước 3: Chạy 5 thử nghiệm thực tế (Yêu cầu 4)
```bash
python exercises/11-batch-vs-streaming/req4_experiments.py
```
- Thử nghiệm 1: Bỏ `.schema(...)` khỏi readStream $\rightarrow$ Ngoại lệ thiếu schema.
- Thử nghiệm 2: Trigger 20 giây $\rightarrow$ Độ trễ dữ liệu theo chu kỳ clock.
- Thử nghiệm 3: Thả lại file cũ $\rightarrow$ Spark tự động bỏ qua nhờ nhật ký file trong checkpoint.
- Thử nghiệm 4: Append mode trên aggregate không watermark $\rightarrow$ AnalysisException.
- Thử nghiệm 5: Window `row_number()` dedup trên stream $\rightarrow$ AnalysisException.

### Bước 4: Ghi Parquet và Đối chiếu Parity (Yêu cầu 5)
```bash
python exercises/11-batch-vs-streaming/req5_write_and_check.py
```
- Ghi 21 dòng hợp lệ ra Parquet bằng cả Batch và Streaming (append mode có checkpoint).
- Đọc lại hai bên và đối chiếu: Row count (21), Total amount (40M VND), Schema (100% khớp).
- Liệt kê toàn bộ cây thư mục `checkpoint/` (`metadata`, `commits/`, `offsets/`, `sources/0/`).

---

## 📊 Báo Cáo Chi Tiết
Xem toàn bộ bảng theo dõi, đối chiếu số liệu và giải thích 7 câu hỏi lý thuyết tại:
👉 [BAO_CAO_LAB_STREAMING.md](file:///c:/Users/Administrator/spark%20introduce%20learn/exercises/11-batch-vs-streaming/BAO_CAO_LAB_STREAMING.md)
