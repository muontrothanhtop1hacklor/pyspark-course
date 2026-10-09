# Bài 21: Full End-to-End ETL Pipeline (Dữ liệu Bảo hiểm xã hội)

Đây là bài thực hành tổng hợp nhằm xây dựng một luồng ETL hoàn chỉnh. Quá trình này áp dụng các kỹ thuật tối ưu hóa phổ biến nhất của PySpark trên tập dữ liệu 1 triệu dòng (1M) vừa được tự sinh ở bước trước.

Có thể coi đây là bài kiểm tra độ hiểu biết và khả năng ứng dụng thực tế sau khi bạn đã làm quen với các khái niệm về đọc/ghi dữ liệu, phép kết nối (Join), kiểm thử (Validate) và phân mảnh (Partitioning).

## Mục tiêu của bài học
Chương trình `etl_pipeline.py` sẽ chạy qua 4 bước cơ bản của một luồng ETL chuẩn:

1. **Trích xuất (Extract):** Quá trình đọc file Parquet sẽ được kết hợp với việc tự khai báo Schema từ trước (Schema Enforcement). Cách này giúp Spark không phải tốn thời gian đọc rà quét toàn bộ file để đoán định dạng, qua đó tối ưu chi phí I/O ngay từ đầu.
2. **Kiểm thử và Làm sạch (Validate):** Loại bỏ ngay các dòng dữ liệu không đạt yêu cầu nghiệp vụ, chẳng hạn như thiếu số sổ BHXH hoặc mức lương bị âm.
3. **Biến đổi (Transform):**
   - Làm sạch cột mã tỉnh bằng các hàm tích hợp sẵn (như `trim` và `upper`).
   - Phân loại "phân khúc lương" bằng cách thay thế các hàm UDF chậm chạp bằng kỹ thuật Broadcast Join. Cụ thể, một bảng ánh xạ nhỏ gọn sẽ được đẩy thẳng vào bộ nhớ (Memory) của các Executor để tăng tốc độ xử lý.
4. **Tải dữ liệu và Đánh giá (Load):** Ở bước cuối cùng, dữ liệu sẽ được ghi ra ổ đĩa theo 3 cách phân mảnh khác nhau. Việc so sánh thời gian ghi của 3 cách này sẽ giúp chúng ta hiểu rõ chiến lược nào là tốt nhất cho bài toán thực tế.

## Hướng dẫn chạy chương trình

1. **Chuẩn bị dữ liệu:**
   Để chương trình hoạt động, bạn cần đảm bảo đã có sẵn dữ liệu tại thư mục `synthetic_bhxh/output/1M/detail`. Nếu chưa có, bạn cần quay lại thư mục gốc dự án và chạy script sinh dữ liệu 1M trước.
2. **Khởi chạy:**
   Mở terminal tại thư mục bài 21 này và chạy lệnh:
   ```bash
   python etl_pipeline.py
   ```

## Đánh giá kết quả phân mảnh (Partitioning)
Ở bước Load, chương trình sẽ tạo ra 3 thư mục tương ứng với 3 cách ghi dữ liệu khác nhau. Khi chạy xong, bạn sẽ thấy kết quả phản ánh các đặc điểm sau:

- **Ghi không phân mảnh (`1_no_partition`):** Tốc độ ghi thường khá nhanh do hệ thống không mất thời gian tổ chức lại thư mục. Nhưng bù lại, khi truy vấn phân tích (ví dụ: cần lọc ra một tỉnh cụ thể), chi phí tìm kiếm sẽ rất tốn kém vì hệ thống phải quét lại toàn bộ dữ liệu (Full scan).
- **Phân mảnh theo mã tỉnh (`2_partition_tinh_thanh`):** Đây là lựa chọn cân bằng và tối ưu nhất. Với 63 tỉnh thành, lượng phân mảnh sinh ra (~63 thư mục) ở mức vừa phải, không làm vỡ vụn file. Khi cần truy vấn dữ liệu của một tỉnh sau này, tốc độ đọc sẽ được cải thiện rõ rệt.
- **Phân mảnh theo phân khúc lương (`3_partition_phan_khuc`):** Phân khúc chỉ bao gồm 3 nhóm (Phổ thông, Trung cấp, Cao cấp). Nếu chia partition theo tiêu chí này, dữ liệu sẽ bị lệch (Data Skewness). Lượng người ở nhóm phổ thông quá lớn so với các nhóm còn lại khiến các Executor xử lý nhóm này bị quá tải, gây nghẽn cổ chai cho toàn bộ hệ thống.

## Một vài lưu ý thêm
Thư mục đầu ra `output_bhxh_etl` của bài tập này đã được đưa vào `.gitignore` để tránh việc vô tình đẩy file dữ liệu nặng lên GitHub. Bạn có thể thoải mái chạy thử và kiểm tra, sau đó môi trường sẽ tự động dọn dẹp hoặc bạn có thể tự tay xoá thư mục đó nếu muốn.
