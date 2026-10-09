# Bài 18: Đánh giá hiệu năng mở rộng với tài nguyên giới hạn (Low Memory)

Mục tiêu của bài thực hành này là đánh giá sức chịu đựng của luồng ETL (Bronze -> Silver -> Gold) trên 3 quy mô dữ liệu hệ thống Bảo Hiểm Xã Hội (BHXH): **1 Triệu (1M)**, **10 Triệu (10M)**, và **100 Triệu (100M)** dòng. 

Thay vì cấu hình cấp phát tối đa RAM để chạy nhanh nhất có thể (như đã thấy ở Bài 17), chúng ta sẽ cố tình **giới hạn RAM nghiêm ngặt** (Low Memory). Mục đích là để quan sát cách PySpark xoay sở qua việc liên tục đọc/ghi ổ cứng (Spill to Disk) để nhường tài nguyên cho máy tính thực hiện các tác vụ khác.

## 1. Nhận xét & Đánh giá khi Scale (Low Memory)

Cấu hình RAM bị bóp nghẹt khiến cơ chế xử lý của Spark bộc lộ những điểm yếu chí mạng khi dữ liệu ngày càng lớn:

### Tại mốc 1 Triệu dòng (1M)
- **Tài nguyên cấp phát:** `1g` RAM, `10` Partitions.
- **Hiện tượng:** Chạy mượt mà, thời gian hoàn thành nhanh (~30 giây). 
- **Lý do:** Kích thước của 1 triệu dòng dữ liệu BHXH (khoảng vài chục MB) hoàn toàn nằm lọt thỏm trong bộ nhớ 1GB. Các thao tác `.groupBy("SO_SO_BHXH")` hay Join ở bước Gold không gặp khó khăn gì vì không cần phải đẩy dữ liệu ra đĩa cứng.

### Tại mốc 10 Triệu dòng (10M)
- **Tài nguyên cấp phát:** `1g` RAM, `50` Partitions.
- **Hiện tượng:** Quá trình bắt đầu chậm đi đáng kể (vài phút). Log liên tục xuất hiện cảnh báo Memory và `Spill in-memory bytes to disk`.
- **Lý do:** Lúc này, thao tác Broadcast Join bảng Dimension (Nhãn trốn đóng) với 10 triệu dòng Detail đã phình to vượt quá 1GB bộ nhớ khả dụng. Spark buộc phải dừng việc xử lý in-memory để ghi các khối dữ liệu tràn (spill) ra ổ cứng (Disk I/O), sau đó đọc lại. Quá trình này cực kỳ tốn chi phí thời gian.

### Tại mốc 100 Triệu dòng (100M)
- **Tài nguyên cấp phát:** `2g` RAM (Buộc phải tăng một chút nếu không muốn sập ngay lập tức), `200` Partitions.
- **Hiện tượng:** Thời gian chạy có thể lên tới hàng chục phút, ổ cứng máy tính bị đọc/ghi (Disk Usage) ở mức 100%. Quá trình Shuffle Data (di chuyển hàng tỷ giá trị `MA_TINH` và `SO_SO_BHXH` qua lại giữa các Node) làm nghẽn cổ chai toàn bộ hệ thống.
- **Bài học rút ra:** 
  - Khi không có đủ RAM, số lượng Partitions (`spark.sql.shuffle.partitions`) cực kỳ quan trọng. Nếu để mặc định, mỗi phần công việc quá lớn sẽ khiến Spark chết ngợp. Bằng cách chẻ nhỏ thành 200 (hoặc 400) Partitions, mỗi Executor chỉ phải xử lý một mẩu dữ liệu nhỏ, vừa đủ với lượng RAM ít ỏi.
  - Tuy hệ thống ETL chạy chậm rì, nhưng máy tính của bạn (hệ điều hành) không bị đơ. Bạn vẫn có thể lướt web làm việc khác vì Spark đã bị khóa mức sử dụng RAM.

---

## 2. Cách chạy lại (Reproduction)
Nếu bạn muốn tự kiểm chứng sự "đọa đày" này, mở terminal trong thư mục `18-data-scaling-performance` và chạy lệnh sau (bạn sẽ cần phải điều chỉnh code để trỏ đúng thư mục `synthetic_bhxh`):

```bash
# Định dạng log Unicode cho Tiếng Việt
$env:PYTHONIOENCODING="utf-8"

# Chạy cho tập 1 Triệu
python etl_bhxh_low_mem.py --scale 1M

# Chạy cho tập 10 Triệu
python etl_bhxh_low_mem.py --scale 10M

# Chạy cho tập 100 Triệu (Cảnh báo: Sẽ tốn nhiều thời gian!)
python etl_bhxh_low_mem.py --scale 100M
```
