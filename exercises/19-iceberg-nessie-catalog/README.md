# Bài 19: Quản lý Table và Metadata với Iceberg, Nessie và Data Catalog

Trong bài tập này, chúng ta sẽ thiết kế một Data Catalog sử dụng **Apache Iceberg** làm định dạng bảng (Table Format) và **Project Nessie** làm Data Catalog (tương đương với vai trò của Git nhưng dành riêng cho Dữ liệu). 

Mục tiêu chính là quản lý an toàn và hiệu quả các tập dữ liệu BHXH khổng lồ đã sinh ra (1M, 10M, 100M).

## 1. Nhận xét & Đánh giá khi Scale từ 1M lên 100M với Iceberg

Tại sao chúng ta phải dùng Iceberg thay vì cứ lưu file Parquet thô (như Bài 17)? Việc nâng quy mô dữ liệu từ 1 Triệu lên 100 Triệu bộc lộ rõ giới hạn của kiến trúc thư mục truyền thống (Hive-style partitioning):

1. **Vấn đề liệt kê file (File Listing Bottleneck):**
   - **Tại 1M:** Spark đọc thư mục `1M/DETAIL` (chứa vài chục file) gần như tức thì.
   - **Tại 100M:** Thư mục `100M/DETAIL` có thể chứa hàng ngàn file Parquet. Khi dùng Spark đọc file Parquet thuần, hàm ListFiles của HDFS/S3 sẽ mất cực kỳ nhiều thời gian chỉ để quét xem có những file nào. **Iceberg giải quyết triệt để** vấn đề này vì nó không list thư mục; nó đọc trực tiếp file `Manifest` chứa con trỏ tới đích danh các file cần đọc.
2. **Thao tác UPDATE/DELETE (Xử lý bản ghi bị thay đổi):**
   - Giả sử trong hệ thống BHXH, có hàng loạt cá nhân điều chỉnh `MUC_LUONG`.
   - Nếu dùng Parquet thuần ở **100M dòng**, bạn phải đọc toàn bộ 100M dòng, lọc ra những dòng cần thay, và ghi đè (Overwrite) lại toàn bộ. Rất tốn kém!
   - Với **Iceberg ở 100M**, bạn có thể dùng lệnh `UPDATE` hoặc `DELETE` y hệt SQL. Iceberg sử dụng cơ chế *Merge-On-Read (MOR)* để chỉ ghi ra một file nhỏ chứa các dòng thay đổi, tốc độ nhanh hơn hàng trăm lần.
3. **Partitioning Vô Hình (Hidden Partitioning):**
   - Nếu bạn chia Partition theo `Tháng` ở Parquet thuần, user phải nhớ thêm cột `Tháng` vào câu query `WHERE`.
   - Với Iceberg, nếu dữ liệu BHXH 100M tăng dần theo thời gian, Iceberg tự động "chắn" (prune) các file không liên quan mà user không cần phải thay đổi câu lệnh truy vấn.

## 2. Trải nghiệm Nessie (Git for Data) ở quy mô lớn
Khi nhiều Data Engineer cùng chọc vào bảng 100M dòng để làm Feature Engineering (MLOps) hoặc thử nghiệm các thuật toán, rủi ro làm hỏng dữ liệu gốc (Gold Layer) là cực kỳ cao.

Nessie cung cấp các tính năng:
- **Time Travel:** Nếu một bước ETL bị lỗi làm hỏng toàn bộ mức lương của 100M cá nhân, bạn có thể rollback hệ thống về trạng thái của 5 phút trước chỉ với 1 câu lệnh. (Điều bất khả thi với Parquet thuần).
- **Tạo Branch/Tag:** Bạn có thể tạo một nhánh (Branch) `experiment-ml` tách biệt từ nhánh `main` để thoải mái train mô hình MLOps mà không sợ làm ảnh hưởng đến luồng báo cáo BI của nhánh chính.

---

## 3. Cấu trúc bài tập
- `docker-compose.yml`: Dựng MinIO (đóng vai trò là S3 storage lưu trữ file Iceberg) và Nessie (Catalog Server).
- `01_load_data.py`: Khởi tạo bảng Iceberg cho 3 mức dữ liệu BHXH (1M, 10M, 100M).
- `02_nessie_git_for_data.py`: Trình diễn các tính năng phân nhánh (branching) và time travel của Nessie.

## 4. Hướng dẫn chạy
### Bước 1: Khởi động MinIO và Nessie
```bash
cd "exercises/19-iceberg-nessie-catalog"
docker-compose up -d
```
Truy cập MinIO: http://localhost:9001 (User: admin / Pass: password123)
Truy cập Nessie API: http://localhost:19120/api/v1/trees

### Bước 2: Tải dữ liệu vào Iceberg
Chạy script `01_load_data.py` để đọc các file 1M, 10M, 100M Parquet và ghi thành định dạng Iceberg.
```bash
python 01_load_data.py
```

### Bước 3: Trải nghiệm Nessie (Git for Data)
Chạy script `02_nessie_git_for_data.py` để xem cách tạo nhánh, ghi đè dữ liệu trên nhánh và truy vấn lại dữ liệu gốc.
```bash
python 02_nessie_git_for_data.py
```
