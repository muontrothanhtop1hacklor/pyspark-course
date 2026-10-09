# Bài 21: Full End-to-End ETL Pipeline (Dữ liệu Bảo hiểm xã hội)

Đây là bài thực hành tổng hợp nhằm xây dựng một luồng ETL hoàn chỉnh. Quá trình này áp dụng các kỹ thuật tối ưu hóa phổ biến nhất của PySpark trên tập dữ liệu hệ thống BHXH (có thể chạy từ 1M lên đến 100M dòng).

Có thể coi đây là bài kiểm tra độ hiểu biết và khả năng ứng dụng thực tế sau khi bạn đã làm quen với các khái niệm về đọc/ghi dữ liệu, phép kết nối (Join), kiểm thử (Validate) và phân mảnh (Partitioning).

## 1. Nhận xét & Đánh giá khi Scale từ 1M lên 100M

Kịch bản ETL này phô diễn sức mạnh của các kỹ thuật tối ưu hóa (Best Practices) rõ ràng nhất khi dữ liệu phình to đến 100M:

1. **Schema Enforcement (Ép kiểu sơ đồ):**
   - **Tại 1M:** Tốc độ đọc file Parquet có hoặc không có Schema khai báo sẵn chênh lệch nhau không đáng kể (chỉ vài giây).
   - **Tại 100M:** Nếu để Spark tự đoán Schema (Infer Schema), nó phải quét qua metadata của hàng ngàn file Parquet trước khi bắt đầu xử lý. Việc ép Schema ngay từ đầu bằng `StructType` tiết kiệm đáng kể thời gian I/O và giảm tải cho Driver.
2. **Broadcast Join thay vì UDF:**
   - Trong bài này, chúng ta phân loại "Phân khúc lương".
   - **Tại 1M:** Dùng UDF (User Defined Function) chậm hơn một chút nhưng vẫn chạy được.
   - **Tại 100M:** Nếu dùng UDF trong Python, quá trình serialization/deserialization dữ liệu giữa JVM và Python Worker sẽ làm sập hệ thống hoặc chạy cực kỳ lâu. Khi thay thế bằng một bảng Dimension nhỏ và dùng `Broadcast Join`, bảng này được đẩy thẳng vào RAM của toàn bộ các Executor. Phép Join diễn ra hoàn toàn trong bộ nhớ In-Memory, tốc độ xử lý 100M dòng nhanh như chớp.
3. **Bài toán Data Skewness (Lệch dữ liệu) khi Phân mảnh:**
   - **Tại 1M:** Ghi Partition theo `Phân Khúc` không gây hậu quả rõ rệt.
   - **Tại 100M:** Khi ghi Partition theo `Phân Khúc`, do nhóm Phổ thông chiếm đến 80-90% dữ liệu, sẽ có 1 hoặc 2 Node (Executor) phải è cổ ra ghi hàng chục triệu dòng, trong khi các Node khác ngồi chơi. Sự lệch pha (Data Skew) này là nguyên nhân chính gây thắt cổ chai ở các hệ thống Big Data lớn. Giải pháp lý tưởng ở 100M là phân vùng theo `MA_TINH` (phân tán tương đối đều đặn).

---

## 2. Mục tiêu của bài học
Chương trình `etl_pipeline.py` sẽ chạy qua 4 bước cơ bản của một luồng ETL chuẩn:

1. **Trích xuất (Extract):** Quá trình đọc file kết hợp với Schema Enforcement.
2. **Kiểm thử và Làm sạch (Validate):** Loại bỏ ngay các dòng dữ liệu thiếu sổ BHXH hoặc mức lương bị âm.
3. **Biến đổi (Transform):** Làm sạch mã tỉnh và Broadcast Join bảng Phân khúc.
4. **Tải dữ liệu và Đánh giá (Load):** Ghi ra đĩa theo 3 cách phân mảnh khác nhau (Không phân mảnh, Phân mảnh theo Mã Tỉnh, Phân mảnh theo Phân Khúc) để so sánh thời gian.

## 3. Hướng dẫn chạy chương trình

1. **Chuẩn bị dữ liệu:**
   Để chương trình hoạt động, bạn cần đảm bảo đã có sẵn dữ liệu tại thư mục `synthetic_bhxh/output/1M/detail`. 
2. **Khởi chạy:**
   Mở terminal tại thư mục bài 21 này và chạy lệnh:
   ```bash
   python etl_pipeline.py
   ```

## Một vài lưu ý thêm
Thư mục đầu ra `output_bhxh_etl` của bài tập này đã được đưa vào `.gitignore` để tránh việc vô tình đẩy file dữ liệu nặng lên GitHub. Bạn có thể thoải mái chạy thử và kiểm tra, sau đó môi trường sẽ tự động dọn dẹp hoặc bạn có thể tự tay xoá thư mục đó nếu muốn.
