# Bài 21: Full End-to-End ETL Pipeline (Dữ liệu Bảo hiểm xã hội)

Bài thực hành này minh hoạ một luồng (pipeline) ETL hoàn chỉnh và tích hợp các kỹ thuật tối ưu hóa tốt nhất (Best Practices) của PySpark, sử dụng tập dữ liệu **1 Triệu dòng (1M)** tự sinh của dự án. 

Đây là bài kiểm tra độ tích hợp cuối cùng sau khi bạn đã học qua các bài về Đọc/Ghi, Join, Validate và Partitioning.

## 🎯 Mục tiêu bài học
Luồng xử lý (ETL) trong script `etl_pipeline.py` sẽ thực hiện 4 bước cốt lõi:

1. **Extract (Trích xuất):** Đọc file Parquet kết hợp với việc **tự khai báo Schema (Schema Enforcement)**. Việc này tối ưu hoá tốc độ I/O do Spark không cần phải tốn chi phí rà quét (infer) toàn bộ file metadata.
2. **Validate (Kiểm thử & Làm sạch):** Loại bỏ ngay những dữ liệu rác không đáp ứng nghiệp vụ. Ví dụ: những dòng không có số sổ BHXH hoặc mức lương không hợp lý.
3. **Transform (Biến đổi):**
   - Làm sạch cột `MA_TINH` (sử dụng Built-in function: `trim`, `upper`).
   - Tối ưu thay thế việc sử dụng UDF (cực kỳ chậm) bằng **Broadcast Join**. Áp dụng một bảng map (Dimension table) siêu nhỏ trực tiếp vào Memory của các Executor để phân loại "Phân khúc lương".
4. **Load (Tải dữ liệu & Đánh giá Partitioning):** Ghi dữ liệu ra bộ nhớ với 3 chiến lược Partitioning khác nhau và đo lường sự chênh lệch thời gian, qua đó kết luận chiến lược hiệu quả nhất.

## 🚀 Cách chạy chương trình

1. **Đảm bảo dữ liệu nguồn đã có sẵn:**
   Pipeline yêu cầu phải có dữ liệu tại thư mục `synthetic_bhxh/output/1M/detail`. Nếu bạn chưa có, hãy chạy script sinh dữ liệu 1M ở gốc dự án trước.
2. **Khởi chạy Pipeline:**
   Mở terminal tại thư mục bài 21 và chạy lệnh:
   ```bash
   python etl_pipeline.py
   ```

## 📊 Giải thích kết quả Partitioning (Load)
Tại bước 4, dữ liệu sẽ được ghi ra 3 thư mục khác nhau. Bạn sẽ quan sát thấy:

- **1. Ghi KHÔNG Partition (`1_no_partition`):** Thường ghi khá nhanh vì không tốn chi phí tổ chức file, nhưng khi bạn query phân tích sau này (như tìm theo tỉnh) thì chi phí quét dữ liệu cực kỳ lớn (Full scan).
- **2. Ghi Partition theo Mã Tỉnh (`2_partition_tinh_thanh`):** Đây là **điểm cân bằng tối ưu nhất**. Có 63 tỉnh thành => số lượng phân vùng (Partition) ở mức trung bình (~63 folder). Không bị chia quá nhỏ, và khi query theo từng tỉnh sau này sẽ rất nhanh.
- **3. Ghi Partition theo Phân Khúc (`3_partition_phan_khuc`):** Phân khúc chỉ có 3 nhóm (Phổ thông, Trung cấp, Cao cấp). Việc chia partition theo cột này sẽ làm phát sinh hiện tượng **Data Skewness (Lệch dữ liệu)**. Nhóm Phổ thông có thể chứa hàng triệu dòng, trong khi nhóm Cao cấp chỉ có vài chục ngàn dòng. Các Executor xử lý nhóm Phổ thông sẽ bị quá tải, gây nghẽn cổ chai (Bottleneck) cho toàn bộ hệ thống. 

## 🧹 Lưu ý
- Thư mục đầu ra `output_bhxh_etl` của bài này chứa các tập dữ liệu parquet và đã được đưa vào `.gitignore` để không bị push nhầm rác lên GitHub. Môi trường của bạn sẽ tự động dọn dẹp hoặc bạn có thể xoá chúng sau khi quan sát kết quả.
