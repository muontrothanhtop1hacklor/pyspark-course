# Bài 17: Quản lý gián đoạn (Resume) sinh dữ liệu 100M và Thiết kế quy trình ETL ban đầu

## 1. Tên chapter/bài
Bài 17: Tiếp tục sinh dữ liệu quy mô lớn (100M) và Viết script tổng hợp (ETL).

## 2. Mục tiêu
- Phục hồi an toàn quá trình sinh dữ liệu BHXH 100 triệu dòng đã bị huỷ giữa chừng (đã sinh được 45/167 chunk, tổng dung lượng 1.39GB).
- Tiếp tục chạy bằng cơ chế đa tiến trình (multiprocessing) mà không làm ghi đè hay mất tính tất định (determinism) của dữ liệu đã có.
- Xây dựng một kịch bản ETL với PySpark để sẵn sàng xử lý dữ liệu ngay khi quá trình sinh dữ liệu hoàn tất.

## 3. Bối cảnh dữ liệu / khái niệm liên quan
- **Dataset**: Dữ liệu Bảo hiểm Xã hội tổng hợp, chia làm 4 cấu phần (MASTER, DETAIL, ML_LABELS, ML_ANOMALY).
- **Vấn đề**: Việc sinh ra 100 triệu dòng có kích thước lớn, tiêu tốn nhiều RAM và thời gian. Khi bị dừng đột ngột, việc bắt đầu lại từ đầu là không hiệu quả. 
- **ETL**: Cần đưa các bảng rời rạc (Master, Chi tiết đóng, Nhãn bất thường) thành các bảng tổng hợp theo cấp cá nhân (`AGG_PERSON`) và cấp doanh nghiệp (`AGG_COMPANY`) phục vụ cho máy học hoặc phân tích.

## 4. Phương pháp / kỹ thuật áp dụng
- **Kiểm tra tính toàn vẹn (Integrity)**: Sử dụng Parquet footer (PyArrow) thay vì load cả file vào RAM, giúp đếm số dòng rất nhanh.
- **Tính tất định (Determinism)**: Phân bố `seed` ngẫu nhiên phụ thuộc tuyến tính vào biến đếm vòng lặp/chunk (`seed = 42 + i`) để dù chạy ở luồng nào hay thời điểm nào, chunk thứ `i` luôn ra một kết quả giống hệt nhau.
- **Ghi nguyên tử (Atomic Write)**: Ghi ra file `.tmp` trước, sau đó đổi tên (`os.replace`) thành tên chính thức. Cách này đảm bảo file không bao giờ bị hỏng dù tiến trình bị chết đột ngột.
- **Xử lý theo giai đoạn (Phased Resume)**: 
  - Giai đoạn 1: Sinh tiếp và ghi các chunk chưa có (45..166).
  - Giai đoạn 2: Trích xuất file kết quả phân loại lỗi (error keys) từ các chunk cũ (0..44) bằng cách chạy lại chuỗi ngẫu nhiên nhưng bỏ qua bước ghi đĩa file Parquet.
- **Thiết kế ETL (PySpark)**: Khai báo đọc hàng loạt phân vùng thư mục, sử dụng các phép `join` (đặc biệt `left join` để tránh mất dữ liệu) và các phép gom nhóm `groupBy()` kèm tính tổng `sum()`, trung bình `avg()`.

## 5. Code đã viết và Lệnh chạy
- **Script sinh tiếp dữ liệu 100M** (`synthetic_bhxh/scripts/resume_100M.py`): Script điều phối resume đa tiến trình an toàn. Đã khởi chạy và tạm dừng an toàn để giữ tiến độ.
  - *Lệnh chạy khi cần tiếp tục:*
    ```bash
    # Chạy 3 tiến trình song song
    python synthetic_bhxh/scripts/resume_100M.py run --procs 3
    ```

- **Script ETL** (`synthetic_bhxh/scripts/etl_100M.py`): Script ETL PySpark để làm sạch, ghép và tổng hợp dữ liệu 5 tập file.
  - *Lệnh chạy sau khi sinh xong 100M:*
    ```bash
    python synthetic_bhxh/scripts/etl_100M.py
    ```

## 6. Kết quả chạy thử
- Quét nhanh và xác nhận 45 chunk đã sinh là nguyên vẹn, số dòng khớp với lý thuyết (50,000 dòng/chunk). Khớp hoàn toàn chuỗi tất định.
- Quá trình chạy ngầm `resume_100M.py` bắt đầu thành công, sinh ra file theo dõi tiến độ, và đã tạm dừng lại chờ lúc có thời gian rảnh.
- Đã có sẵn công cụ ETL hoàn chỉnh chờ dataset 100M.

## 7. Lỗi gặp phải và cách xử lý
- **Lỗi/Khó khăn**: Cần danh sách file "đáp án lỗi" cho 100M, nhưng danh sách này nằm rải rác ở từng chunk lúc sinh. Việc resume không tạo ra được file lỗi của 45 chunk đầu vì đã sinh xong trước đó và bị ghi đè.
- **Xử lý**: Lên thiết kế vòng lặp phụ: Load lại hàm sinh cho 45 chunk cũ để trích xuất mảng báo lỗi, ghi ra CSV phụ mà tuyệt đối không touch/overwrite tới các file `.parquet` cũ (Đọc thì được, Ghi thì bị cấm).

## 8. Kết luận / ghi chú
- Khi thao tác sinh hoặc xử lý Big Data, luôn phải thiết kế cơ chế lưu tiến trình (checkpoint) và ghi nguyên tử (atomic).
- PyArrow là cứu cánh cho các bài toán phân tích siêu dữ liệu (metadata) của Parquet khi ta không muốn load dữ liệu (Vd: lấy schema hoặc row count).

## 9. Plan cho buổi sau
- Bật máy để chạy nốt 122 chunk còn lại của 100M.
- Khởi chạy file `etl_100M.py` trên cluster Spark hoặc local để kiểm chứng logic Join, đánh giá thời gian shuffle khi join tập 100 triệu dòng.
- Viết các câu query phân tích (SQL) trực tiếp trên 2 bảng `AGG_PERSON` và `AGG_COMPANY`.
