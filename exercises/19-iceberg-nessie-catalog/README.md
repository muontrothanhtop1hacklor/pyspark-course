# Bài 19: Quản lý Table và Metadata với Iceberg, Nessie và Data Catalog

Trong bài tập này, chúng ta sẽ thiết kế một Data Catalog sử dụng **Apache Iceberg** làm định dạng bảng (Table Format) và **Project Nessie** làm Data Catalog (tương tự như Git cho dữ liệu) để quản lý các tập dữ liệu có kích thước khác nhau (1 triệu, 10 triệu và 100 triệu dòng) đã được sinh ra từ bài trước.

## Mục tiêu
1. Hiểu cách thiết lập môi trường Data Lakehouse thực tế với S3 (MinIO), Iceberg và Nessie.
2. Nắm được cách ghi dữ liệu từ file Parquet thô vào bảng Iceberg.
3. Trải nghiệm các tính năng mạnh mẽ của Nessie như **Time Travel**, **Tạo Branch/Tag** để quản lý phiên bản dữ liệu giống như Git.

## Cấu trúc bài tập
- `docker-compose.yml`: Dùng để dựng MinIO (đóng vai trò là S3 storage lưu trữ file Iceberg) và Nessie (Catalog Server).
- `01_load_data.py`: Khởi tạo bảng Iceberg cho 3 mức dữ liệu (1M, 10M, 100M) và load dữ liệu từ thư mục `synthetic_bhxh` vào.
- `02_nessie_git_for_data.py`: Trình diễn các tính năng phân nhánh (branching) và time travel của Nessie.

## Hướng dẫn chạy
### Bước 1: Khởi động MinIO và Nessie
```bash
cd "exercises/19-iceberg-nessie-catalog"
docker-compose up -d
```
Truy cập MinIO: http://localhost:9001 (User: admin / Pass: password)
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
