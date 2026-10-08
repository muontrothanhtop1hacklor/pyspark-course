# Synthetic QTTG BHXH Data Generator

Bộ sinh dữ liệu tổng hợp mô phỏng bảng Quá trình tham gia BHXH (Master + Detail) dùng cho mục đích thực hành ETL/MLOps.

## Cảnh báo
- **ĐÂY LÀ DỮ LIỆU TỔNG HỢP (SYNTHETIC)**: Toàn bộ thông tin định danh (SO_SO_BHXH, HO_TEN, SO_CCCD) đều được sinh ngẫu nhiên, không phản ánh bất kỳ cá nhân hay tổ chức có thật nào.
- Dữ liệu cố ý chứa một lượng nhỏ nhiễu/lỗi thường gặp trong thực tế (trùng khóa, sai logic thời gian, v.v.). Người dùng tự xây dựng pipeline làm sạch (Data Quality) để phát hiện và xử lý.
- Các nhãn ML (ML_LABELS, ML_ANOMALY) được sinh theo logic nội bộ cộng nhiễu. Metric mô hình huấn luyện trên tập dữ liệu này **không có ý nghĩa nghiệp vụ thực tế**.

## Cấu trúc thư mục
- `config/params.yaml`: Các tham số hệ thống, tỷ lệ đóng, mức lương vùng (cần cập nhật theo văn bản luật nếu muốn khớp hoàn toàn hiện hành).
- `src/`: Mã nguồn sinh dữ liệu (Python/PyArrow/Pandas).
- `scripts/run_generation.py`: Script chính để sinh dữ liệu.
- `scripts/check_data.py`: Script PySpark để kiểm tra nhanh tính nhất quán của dữ liệu.
- `output/{scale}/`: Nơi chứa dữ liệu sinh ra (Parquet, CSV).
- `_answer_key/`: KHÔNG MỞ. Chứa danh sách các ID bị chèn lỗi cố ý để đối chiếu sau khi tự làm pipeline.

## Cách chạy

Yêu cầu môi trường: Python + PySpark 4.0.1 (conda env `pyspark_env`), thư viện `pandas`, `pyarrow`, `tqdm`, `pyyaml`.

1. **Sinh dữ liệu 1M (1 triệu dòng detail)**:
   ```bash
   python scripts/run_generation.py --scale 1M --csv
   ```
2. **Sinh dữ liệu 10M**:
   ```bash
   python scripts/run_generation.py --scale 10M --csv
   ```
3. **Sinh dữ liệu 100M** (Cần xác nhận dung lượng trước):
   ```bash
   python scripts/run_generation.py --scale 100M
   ```

4. **Kiểm tra dữ liệu**:
   ```bash
   python scripts/check_data.py --scale 1M
   ```
