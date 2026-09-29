# Bài 12: Kafka Cơ Bản & Khởi Tạo Luồng Dữ Liệu Streaming (Ngày 2)

Thư mục này chứa toàn bộ mã nguồn, cấu hình Docker và báo cáo thực hành bài tập **Kafka Cơ Bản (Ngày 2)**, chuẩn bị nguồn dữ liệu cho bài toán **Spark Structured Streaming đọc Kafka (Ngày 3)**.

---

## 1. Cấu trúc thư mục

```text
exercises/12-kafka-basics/
├── kafka_lab_12.py      # TẤT CẢ TRONG 1 FILE: Tự động khởi tạo topic, chạy producer, consumer và 5 thử nghiệm
├── orders_producer.py   # Python Producer gửi 30 message (3 đợt x 10 message) với các ca lỗi/dirty data
├── kafka_notes.md       # Báo cáo chi tiết: Lệnh đã dùng, log thực tế, bảng đối chiếu và trả lời 7 câu hỏi
└── README.md            # Tài liệu hướng dẫn bài lab
```

---

## 2. Hướng dẫn chạy nhanh

### Bước 1: Dựng Kafka Container (Chế độ KRaft)
```powershell
docker run -d --name kafka -p 9092:9092 --restart unless-stopped apache/kafka:latest
```

Kiểm tra broker đã hoạt động:
```powershell
docker logs kafka --tail 10
```

### Bước 2: Chạy toàn bộ bài lab (Tất cả trong 1 file)
Chạy script tổng hợp [`kafka_lab_12.py`](./kafka_lab_12.py) để tự động hóa toàn bộ các bước từ khởi tạo topic, gửi message đến chạy 5 thử nghiệm:
```powershell
python exercises/12-kafka-basics/kafka_lab_12.py
```

Hoặc chạy riêng lẻ từng phần:
```powershell
# Gửi 30 message bằng producer
python exercises/12-kafka-basics/orders_producer.py

# Đọc topic bằng console consumer từ đầu
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders_stream --from-beginning --property print.key=true --property print.partition=true --property print.offset=true
```

---

## 3. Tổng kết số liệu Producer

- **Tổng số message gửi:** 30 message (chia làm 3 đợt x 10 message).
- **Message không phải JSON:** 2 message (1 chuỗi thô, 1 lỗi cú pháp JSON).
- **Message đơn hàng hợp lệ (gốc):** 19 message.
  - **Tổng amount đơn hàng hợp lệ (gốc):** **33,500,000 VND**.
- **Số đơn hàng hợp lệ sau dedup `order_id` (lấy bản ghi `updated_at` mới nhất):** 17 đơn hàng duy nhất.
  - **Tổng amount sau dedup:** **30,000,000 VND**.

---

## 4. Báo cáo & Trả lời câu hỏi lý thuyết

Xem toàn bộ nhật ký lệnh, bảng phân tích offset/partition và lời giải cho 7 câu hỏi lý thuyết tại:
👉 [**Báo cáo thực hành chi tiết (kafka_notes.md)**](./kafka_notes.md)
