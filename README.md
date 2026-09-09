
 ## Mục tiêu bài học
Nắm vững các khái niệm nền tảng đầu tiên khi làm việc với PySpark, hiểu được luồng khởi tạo và các thao tác kiểm tra dữ liệu cơ bản.

Kiến thức cốt lõi
* **SparkSession**: Điểm khởi đầu (entry point) bắt buộc của mọi ứng dụng PySpark.
* **DataFrame**: Cấu trúc dữ liệu phân tán dạng bảng (gồm row và column) - cấu trúc nền tảng cho mọi tác vụ ETL trong Spark.
* **Các hàm kiểm tra dữ liệu cơ bản:**
  * `show()`: Hiển thị dữ liệu mẫu ra console (mặc định 20 dòng).
  * `printSchema()`: In cấu trúc của DataFrame (tên cột, kiểu dữ liệu, có cho phép Null hay không).
  * `count()`: Đếm tổng số bản ghi. *(Lưu ý: Đây là một Action, thao tác này sẽ kích hoạt cơ chế Lazy Evaluation để Spark thực sự chạy tính toán).*

 ## Bài tập thực hành
**Yêu cầu:** Sửa file `exercise.py` để thực hiện các bước sau:
1. Tạo DataFrame `employee` bao gồm các trường:
   * `employee_id`
   * `employee_name`
   * `department`
   * `salary`
2. Thực thi các lệnh kiểm tra: In schema, hiển thị dữ liệu và đếm tổng số nhân viên.

Chạy demo

'''bash


 # python chapters/01_dataframe_basics/demo.py


 # python chapters/01_dataframe_basics/exercise.py
