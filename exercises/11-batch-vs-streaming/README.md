# Bài 11: So sánh Spark Batch và Structured Streaming

Thư mục này chứa bài thực hành đối chiếu mô hình Batch Processing với Structured Streaming trên Apache Spark.

## Cấu trúc thư mục

```text
exercises/11-batch-vs-streaming/
├── generate_streaming_data.py   # Tạo 30 dòng dữ liệu mẫu, chia vào 3 file CSV
├── req1_batch.py                # Yêu cầu 1: đọc batch, làm sạch và tổng hợp theo province
├── req2_streaming_console.py    # Yêu cầu 2: streaming ra console (complete mode, maxFilesPerTrigger=1)
├── req4_experiments.py          # Yêu cầu 4: 5 thử nghiệm quan sát lỗi và hành vi riêng của stream
├── req5_write_and_check.py      # Yêu cầu 5: ghi Parquet (batch và streaming append), đối chiếu, xem checkpoint
├── BAO_CAO_LAB_STREAMING.md     # Báo cáo: bảng theo dõi micro-batch, đối chiếu số liệu, trả lời 7 câu hỏi
├── README.md
├── data/
│   ├── source_files/            # File gốc orders_1.csv, orders_2.csv, orders_3.csv
│   ├── batch_input/             # Chứa cả 3 file, dùng cho đọc batch
│   ├── stream_input/            # Thư mục trống, nạp file dần cho streaming
│   └── stream_input_write/      # Thư mục nạp file cho bài test ghi Parquet
├── output/
│   ├── batch_parquet/           # Kết quả Parquet từ batch
│   └── stream_parquet/          # Kết quả Parquet từ streaming
└── checkpoint/
    ├── stream_console/          # Checkpoint cho stream console complete
    ├── stream_exp/              # Checkpoint cho các thử nghiệm
    └── stream_parquet_write/    # Checkpoint cho streaming ghi Parquet append
```

## Hướng dẫn chạy

### Bước 0: Sinh dữ liệu mẫu

```bash
python exercises/11-batch-vs-streaming/generate_streaming_data.py
```

### Bước 1: Batch Processing (Yêu cầu 1)

```bash
python exercises/11-batch-vs-streaming/req1_batch.py
```

Script khai báo schema tĩnh và đọc cả thư mục `batch_input/`. Sau đó chuẩn hóa `status`, ép kiểu `amount`, parse `order_date`, lọc ra 21 dòng hợp lệ và tính tổng theo province.

### Bước 2: Structured Streaming (Yêu cầu 2)

```bash
python exercises/11-batch-vs-streaming/req2_streaming_console.py
```

Script đọc `stream_input/` với `maxFilesPerTrigger=1`, `trigger("5 seconds")`, `outputMode("complete")`. Các file `orders_1.csv`, `orders_2.csv`, `orders_3.csv` được nạp lần lượt, và console in kết quả lũy kế qua micro-batch 0, 1, 2.

### Bước 3: Năm thử nghiệm (Yêu cầu 4)

```bash
python exercises/11-batch-vs-streaming/req4_experiments.py
```

1. Bỏ `.schema(...)` khỏi `readStream`: Spark báo lỗi thiếu schema.
2. Đặt trigger 20 giây: dữ liệu bị trễ theo chu kỳ trigger.
3. Thả lại file cũ vào thư mục nguồn: Spark bỏ qua nhờ nhật ký file trong checkpoint.
4. Dùng append mode cho aggregate không có watermark: `AnalysisException`.
5. Dedup bằng window `row_number()` trên stream: `AnalysisException`.

### Bước 4: Ghi Parquet và đối chiếu (Yêu cầu 5)

```bash
python exercises/11-batch-vs-streaming/req5_write_and_check.py
```

Script ghi 21 dòng hợp lệ ra Parquet bằng cả batch và streaming (append mode, có checkpoint), rồi đọc lại hai bên để so sánh: số dòng (21), tổng amount (40 triệu VND), schema (khớp hoàn toàn). Cuối cùng liệt kê cây thư mục `checkpoint/` gồm `metadata`, `commits/`, `offsets/`, `sources/0/`.

## Báo cáo

Bảng theo dõi micro-batch, đối chiếu số liệu và phần giải thích 7 câu hỏi lý thuyết nằm trong [BAO_CAO_LAB_STREAMING.md](BAO_CAO_LAB_STREAMING.md).
