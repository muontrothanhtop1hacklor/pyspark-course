# Báo cáo Thực hành Ngày 5: End-to-End Pipeline

Pipeline kết hợp đầy đủ các thao tác xử lý từ việc nhận luồng, làm sạch, phân luồng (valid/invalid), khử trùng lặp (dedup), enrich dữ liệu (join), aggregate và xuất ra nhiều điểm đến khác nhau. 

Dưới đây là phần trả lời cho các câu hỏi lý thuyết của bài:

## 1. Vì sao dùng `foreachBatch` để dedup bằng Window thay vì dùng Window trực tiếp trên stream?
- **Hạn chế của Spark Streaming:** Spark Structured Streaming hiện không hỗ trợ trực tiếp hàm Window có chứa mệnh đề `partitionBy(...).orderBy(...)` trên luồng DataFrame, vì nó không thể lưu trữ trạng thái (state) vô hạn để tìm ra dòng dữ liệu "cuối cùng/mới nhất" cho mọi order từ trước đến nay.
- **Sử dụng `foreachBatch`:** Bằng cách dùng `foreachBatch`, luồng dữ liệu (micro-batch) được chuyển đổi thành một DataFrame tĩnh (Batch DataFrame). Trên DataFrame tĩnh này, toàn bộ tập hợp Window operations (như `row_number() over (Window...)`) đều được hỗ trợ. Việc này giúp ta linh hoạt lọc lấy bản ghi có timestamp mới nhất trong *từng micro-batch* thay vì chỉ lấy bản ghi đến trước (như hàm `dropDuplicates()` native trên stream).

## 2. Khi có nhiều streaming query ghi ra nhiều sink từ cùng một nguồn, mỗi query có đọc Kafka độc lập không, hay dùng chung một lần đọc?
- **Đọc độc lập:** Bất cứ khi nào bạn gọi hành động `.start()` trên một `writeStream`, Spark sẽ tạo ra một physical Streaming Query riêng biệt và hoàn toàn độc lập. Do đó, nếu bạn có 3 query (cho nhánh valid, invalid và report), Spark sẽ tạo ra 3 consumer group Kafka (mặc định) độc lập và đọc Kafka 3 lần (với 3 checkpoint khác nhau để theo dõi offset).
- *(Lưu ý mở rộng)*: Nếu bạn muốn chỉ đọc 1 lần nhưng ghi nhiều sink, bạn có thể gọi `.writeStream.foreachBatch(process_batch_function)`. Bên trong function đó, bạn có thể `.cache()` micro-batch lại, xử lý, rồi `.write` ra 3 sink tĩnh (Parquet, console) khác nhau, sau đó `.unpersist()`. Tuy nhiên, yêu cầu của bài này chủ ý đòi hỏi chạy 3 nhánh streaming query độc lập để mô phỏng tính chịu lỗi cô lập (isolation).

## 3. Nếu một trong ba nhánh bị lỗi và dừng, hai nhánh còn lại có bị ảnh hưởng không?
- **Không bị ảnh hưởng (Về mặt xử lý của Spark):** Do 3 nhánh được thiết lập thành 3 Streaming Query với 3 thư mục `checkpointLocation` khác nhau, chúng hoạt động không đồng bộ trong cùng một SparkContext. Nếu query 1 bị crash (vd: lỗi Parse hoặc cấu hình sai), chỉ có query 1 dừng lại. Query 2 và Query 3 vẫn tiếp tục consume dữ liệu mới từ Kafka và lưu tiến trình của chúng bình thường. 
- Khi bạn sửa lỗi và bật lại query 1, nó sẽ đọc `offsets` từ checkpoint của chính nó để bắt kịp tiến độ với 2 nhánh kia. Việc tách checkpoint cung cấp khả năng cô lập lỗi (fault isolation) rất mạnh mẽ trong kiến trúc Data Pipeline.
