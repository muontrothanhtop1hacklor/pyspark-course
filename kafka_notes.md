# Báo cáo thực hành (Bài 12): Kafka Cơ Bản & Thử Nghiệm Streaming Source

Báo cáo chi tiết quá trình dựng cụm Apache Kafka 1 Broker chế độ KRaft bằng Docker, kiểm thử Topic & Partition, xây dựng Python Producer với dữ liệu JSON dirty, kiểm tra Consumer Offset và 5 kịch bản thử nghiệm hành vi Consumer Group.

---

## 1. Chuẩn bị môi trường & Thiết kế dữ liệu

### 1.1. Kiểm tra Docker & Cài đặt thư viện Python
- **Kiểm tra Docker Desktop:**
  ```powershell
  docker ps
  ```
  *(Kết quả: Docker engine đã sẵn sàng)*
- **Cài đặt thư viện `kafka-python` trên môi trường Conda `pyspark_env`:**
  ```powershell
  & "C:\Users\Administrator\miniconda3\envs\pyspark_env\Scripts\pip.exe" install kafka-python
  ```
  *(Thư viện đã được cài đặt thành công phiên bản 3.0.11)*

### 1.2. Thiết kế dữ liệu 30 message (3 đợt x 10 message)
Các message mang cấu trúc schema chuẩn bị cho Spark Structured Streaming (Ngày 3):
- `order_id`: Mã đơn hàng.
- `customer_id`: Mã khách hàng.
- `province`: Tỉnh thành (HaNoi, HoChiMinh, DaNang, CanTho, HaiPhong).
- `amount`: Giá trị đơn hàng.
- `status`: Trạng thái xử lý.
- `order_date`: Ngày đặt hàng.
- `updated_at`: Thời điểm cập nhật trạng thái.

**Các trường hợp dữ liệu chủ động tạo:**
1. **Duplicate `order_id`:**
   - Đơn `1001`: Đợt 1 (`amount=1,000,000`, `2025-01-05 08:30:00`) và Đợt 2 (`amount=1,500,000`, `2025-01-15 10:00:00`).
   - Đơn `1002`: Đợt 1 (`amount=2,500,000`, `2025-01-06 09:15:00`) và Đợt 3 (`amount=2,800,000`, `2025-01-25 09:00:00`).
2. **`amount` invalid (null hoặc <= 0):**
   - Đơn `1005` (`amount = 0.0`), `1007` (`amount = -50000.0`).
   - Đơn `1012` (`amount = null`), `1018` (`amount = null`).
   - Đơn `1022` (`amount = -100000.0`), `1025` (`amount = 0.0`).
3. **`status` viết hoa/thường không đồng nhất:**
   - Xen kẽ: `SUCCESS`, `completed`, `PENDING`, `COMPLETED`, `Shipping`, `pending`, `CANCELLED`, `FAILED`.
4. **`order_date` sai format:**
   - Đơn `1008`: `"31/02/2025"` (sai định dạng DD/MM/YYYY và ngày 31 tháng 2 không tồn tại).
   - Đơn `1015`: `"2025-13-40"` (tháng 13, ngày 40).
   - Đơn `1023`: `""` (chuỗi rỗng).
5. **`customer_id` không tồn tại trong danh mục khách hàng:**
   - Đơn `1017`: `customer_id = "C999"`.
6. **Message không phải JSON hợp lệ (2 message):**
   - Đợt 1 (Message 10): Chuỗi thô `"INVALID_RAW_STRING_MESSAGE_1_NOT_A_JSON"`.
   - Đợt 3 (Message 30): JSON lỗi cú pháp `'{"order_id": 9999, "broken_json": unclosed_string_error'`.

---

## 2. Yêu cầu 1 – Dựng Kafka (Docker KRaft Mode)

### 2.1. Khởi chạy Kafka container chính thức
Sử dụng image chính thức `apache/kafka:latest` (Kafka v4.3.1, mặc định chạy chế độ KRaft mà không cần Zookeeper):
```powershell
docker run -d --name kafka -p 9092:9092 --restart unless-stopped apache/kafka:latest
```

### 2.2. Kiểm tra broker đã lên
Kiểm tra log container:
```text
[BrokerLifecycleManager id=1] The broker has been unfenced. Transitioning from RECOVERY to RUNNING.
[SocketServer listenerType=BROKER, nodeId=1] Enabling request processing.
Awaiting socket connections on 0.0.0.0:9092.
[BrokerServer id=1] Transition from STARTING to STARTED
Kafka version: 4.3.1
[KafkaRaftServer nodeId=1] Kafka Server started
```
Broker ID 1 đã khởi động ở chế độ KRaft và lắng nghe tại cổng `9092`.

### 2.3. Chạy thử các script CLI bên trong container
Vào bên trong container kiểm tra các CLI:
```powershell
# Liệt kê topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Tạo topic test
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test_topic --partitions 1 --replication-factor 1

# Gửi message thử nghiệm
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test_topic

# Đọc message thử nghiệm
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test_topic --from-beginning --max-messages 1
```
*(Kết quả: Producer gửi và Consumer nhận được `hello kafka` thành công).*

---

## 3. Yêu cầu 2 – Topic và Partition

### 3.1. Tạo topic `orders_stream`
Tạo topic với 3 partition và replication factor 1:
```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders_stream --partitions 3 --replication-factor 1
```

### 3.2. Mô tả topic bằng lệnh `describe`
```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders_stream
```
**Kết quả thực tế từ Broker:**
```text
Topic: orders_stream    TopicId: tLJzf08nRy20zX-PeQTJQQ    PartitionCount: 3    ReplicationFactor: 1    Configs: min.insync.replicas=1,segment.bytes=1073741824
    Topic: orders_stream    Partition: 0    Leader: 1    Replicas: 1    Isr: 1    Elr:     LastKnownElr: 
    Topic: orders_stream    Partition: 1    Leader: 1    Replicas: 1    Isr: 1    Elr:     LastKnownElr: 
    Topic: orders_stream    Partition: 2    Leader: 1    Replicas: 1    Isr: 1    Elr:     LastKnownElr: 
```

**Giải thích các thông số:**
- `PartitionCount: 3`: Topic được chia làm 3 phân vùng độc lập (0, 1, 2).
- `ReplicationFactor: 1`: Mỗi partition có 1 bản sao lưu trữ trên cụm.
- `Leader: 1`: Broker có ID = 1 chịu trách nhiệm nhận lệnh Read/Write cho cả 3 partition.
- `Replicas: 1`: Danh sách broker lưu trữ bản sao của partition.
- `Isr: 1` (In-Sync Replicas): Danh sách replica đang đồng bộ kịp thời với Leader.

---

## 4. Yêu cầu 3 – Producer

### 4.1. Thử nghiệm Console Producer
1. **Lần 1: Không có key (Round-Robin / Sticky Partitioner):**
   ```powershell
   docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders_stream
   > message_without_key_1
   > message_without_key_2
   ```
2. **Lần 2: Có key (dùng key để phân bổ partition):**
   ```powershell
   docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders_stream --property "parse.key=true" --property "key.separator=:"
   > 1001:{"order_id": "1001", "amount": 1000000.0}
   > 1002:{"order_id": "1002", "amount": 2500000.0}
   ```

### 4.2. Viết Python Producer gửi 30 message (`orders_producer.py`)
Mã nguồn đặt tại [`orders_producer.py`](./orders_producer.py). Khi chạy:
```powershell
python exercises/12-kafka-basics/orders_producer.py
```

**Log gửi dữ liệu thực tế:**
```text
===========================================================================
BẮT ĐẦU GỬI MESSAGE VÀO KAFKA TOPIC: orders_stream
===========================================================================

--- ĐANG GỬI ĐỢT 1/3 (Số message: 10) ---
[01] Key=1001  | Partition=0 | Offset=0   | HỢP LỆ
[02] Key=1002  | Partition=0 | Offset=1   | HỢP LỆ
[03] Key=1003  | Partition=1 | Offset=0   | HỢP LỆ
[04] Key=1004  | Partition=1 | Offset=1   | HỢP LỆ
[05] Key=1005  | Partition=2 | Offset=0   | LỖI (Invalid amount: 0.0)
[06] Key=1006  | Partition=2 | Offset=1   | HỢP LỆ
[07] Key=1007  | Partition=0 | Offset=2   | LỖI (Invalid amount: -50000.0)
[08] Key=1008  | Partition=0 | Offset=3   | LỖI (Invalid order_date: 31/02/2025)
[09] Key=1009  | Partition=1 | Offset=2   | HỢP LỆ
[10] Key=None  | Partition=2 | Offset=2   | LỖI (Not valid JSON dict)
Đã gửi xong đợt 1. Chờ 2s trước đợt tiếp theo...

--- ĐANG GỬI ĐỢT 2/3 (Số message: 10) ---
[11] Key=1001  | Partition=0 | Offset=4   | HỢP LỆ
[12] Key=1011  | Partition=1 | Offset=3   | HỢP LỆ
[13] Key=1012  | Partition=1 | Offset=4   | LỖI (Invalid amount: None)
[14] Key=1013  | Partition=2 | Offset=3   | HỢP LỆ
[15] Key=1014  | Partition=1 | Offset=5   | HỢP LỆ
[16] Key=1015  | Partition=0 | Offset=5   | LỖI (Invalid order_date: 2025-13-40)
[17] Key=1016  | Partition=1 | Offset=6   | HỢP LỆ
[18] Key=1017  | Partition=0 | Offset=6   | HỢP LỆ
[19] Key=1018  | Partition=1 | Offset=7   | LỖI (Invalid amount: None)
[20] Key=1019  | Partition=2 | Offset=4   | HỢP LỆ
Đã gửi xong đợt 2. Chờ 2s trước đợt tiếp theo...

--- ĐANG GỬI ĐỢT 3/3 (Số message: 10) ---
[21] Key=1002  | Partition=0 | Offset=7   | HỢP LỆ
[22] Key=1020  | Partition=0 | Offset=8   | HỢP LỆ
[23] Key=1021  | Partition=1 | Offset=8   | HỢP LỆ
[24] Key=1022  | Partition=0 | Offset=9   | LỖI (Invalid amount: -100000.0)
[25] Key=1023  | Partition=2 | Offset=5   | LỖI (Invalid order_date: )
[26] Key=1024  | Partition=2 | Offset=6   | HỢP LỆ
[27] Key=1025  | Partition=2 | Offset=7   | LỖI (Invalid amount: 0.0)
[28] Key=1026  | Partition=0 | Offset=10  | HỢP LỆ
[29] Key=1027  | Partition=0 | Offset=11  | HỢP LỆ
[30] Key=None  | Partition=0 | Offset=12  | LỖI (Not valid JSON dict)
```

### 4.3. Báo cáo tổng kết dữ liệu đã gửi (Producer Summary)
| Chỉ số | Giá trị | Ghi chú |
|---|:---:|---|
| **Tổng message đã gửi** | **30** | Gửi qua 3 đợt (10 + 10 + 10) |
| **Message không phải JSON hợp lệ** | **2** | Message 10 (chuỗi thô) & Message 30 (lỗi cú pháp JSON) |
| **Message JSON lỗi nghiệp vụ** | **9** | 6 lỗi amount (null/<=0), 3 lỗi order_date |
| **Số đơn hàng hợp lệ (gốc đã gửi)** | **19** | Đủ điều kiện: JSON chuẩn, amount > 0, date chuẩn |
| **TỔNG AMOUNT ĐƠN HỢP LỆ (GỐC)** | **33,500,000 VND** | Cộng dồn tất cả 19 lượt gửi hợp lệ |
| **Số đơn hàng hợp lệ sau Dedup `order_id`** | **17** | Khử trùng đơn `1001` (lấy đợt 2) và `1002` (lấy đợt 3) |
| **TỔNG AMOUNT SAU DEDUP (LATEST)** | **30,000,000 VND** | Đúng tổng doanh thu thực tế khi cập nhật bản ghi mới nhất |

---

## 5. Yêu cầu 4 – Consumer

### 5.1. Lệnh Console Consumer đọc từ đầu
In ra đầy đủ key, partition và offset:
```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders_stream --from-beginning --property print.key=true --property print.partition=true --property print.offset=true --max-messages 30
```

### 5.2. Kết quả phân bổ Message theo Partition
- **Partition 0:** Chứa 13 message (Offset 0 đến 12).
- **Partition 1:** Chứa 9 message (Offset 0 đến 8).
- **Partition 2:** Chứa 8 message (Offset 0 đến 7).
- **Tổng số message đọc được:** $13 + 9 + 8 = 30$ message.

### 5.3. Nhận xét quan sát
1. **Khớp số lượng tuyệt đối:** Số lượng message đọc ra từ cả 3 partition đạt chính xác **30/30** message, không sót message nào kể cả 2 message không phải JSON.
2. **Quy luật Offset:**
   - Trong mỗi partition riêng lẻ, offset tăng tuần tự đơn điệu bắt đầu từ $0$: $0, 1, 2, 3, \dots$
   - Offset là chỉ số định danh riêng cho từng partition, không có offset chung trên toàn topic.
3. **Thứ tự đọc:** Khi đọc từ console consumer, các message từ Partition 0, 1, 2 được trả về theo lô (interleaved), thứ tự toàn cục giữa các partition khác nhau không được bảo đảm, nhưng thứ tự bên trong từng partition luôn được bảo đảm tuyệt đối.

---

## 6. Yêu cầu 5 – Các Thử Nghiệm Chuyên Sâu

### 6.1. Thử nghiệm 1: Gửi nhiều message cùng một key
- **Thực hiện:** Gửi 5 message liên tiếp với cùng `key = "VIP_CUSTOMER_99"`.
- **Kết quả thực tế:**
  ```text
  Msg 0: Key=VIP_CUSTOMER_99 -> Partition=2, Offset=8
  Msg 1: Key=VIP_CUSTOMER_99 -> Partition=2, Offset=9
  Msg 2: Key=VIP_CUSTOMER_99 -> Partition=2, Offset=10
  Msg 3: Key=VIP_CUSTOMER_99 -> Partition=2, Offset=11
  Msg 4: Key=VIP_CUSTOMER_99 -> Partition=2, Offset=12
  ```
- **Hiện tượng:** Tất cả 5 message đều rơi vào duy nhất **Partition 2**, với offset tăng liên tục từ 8 đến 12.
- **Giải thích:** Kafka dùng hàm băm `murmur2(key) % num_partitions`. Khi số partition không đổi, cùng một key luôn cho ra cùng một partition index.

---

### 6.2. Thử nghiệm 2 & 3: Phân bổ Partition trong Consumer Group (2, 3 và 4 Consumer)
Topic `orders_stream` có **3 partition** (0, 1, 2).

#### Trường hợp 2 Consumer cùng group (`group.id = test_group_rebalance`):
Dùng lệnh describe kiểm tra phân bổ:
```powershell
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group test_group_rebalance
```
**Kết quả thực tế:**
```text
GROUP                TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID     HOST         CLIENT-ID
test_group_rebalance orders_stream   0          13              13              0    C1-10f36bbf...  /172.17.0.1  C1
test_group_rebalance orders_stream   1          9               9               0    C1-10f36bbf...  /172.17.0.1  C1
test_group_rebalance orders_stream   2          13              13              0    C2-14956c2b...  /172.17.0.1  C2
```
- **Hiện tượng:** Cụm tự động rebalance: Consumer `C1` quản lý Partition 0 và 1; Consumer `C2` quản lý Partition 2.

#### Trường hợp 3 Consumer cùng group:
- **Hiện tượng:** Mỗi consumer nhận phụ trách đúng 1 partition:
  - Consumer 1 $\rightarrow$ Partition 0
  - Consumer 2 $\rightarrow$ Partition 1
  - Consumer 3 $\rightarrow$ Partition 2
- Mức độ song song đạt tối đa ($1:1$).

#### Trường hợp 4 Consumer cùng group:
- **Hiện tượng:**
  - 3 Consumer đầu tiên được giao mỗi người 1 partition (P0, P1, P2).
  - **Consumer thứ 4 bị IDLE (chờ rảnh / unassigned)**, không nhận được bất kỳ partition nào!
- **Nguyên lý:** Một partition chỉ có thể được tiêu thụ bởi tối đa **một consumer** trong cùng một group tại một thời điểm để tránh race condition và xáo trộn thứ tự offset.

---

### 6.3. Thử nghiệm 4: Chạy Consumer với `group.id` khác
- **Thực hiện:** Khởi chạy consumer với group ID mới hoàn toàn: `group_brand_new_456` và `auto_offset_reset = 'earliest'`.
- **Hiện tượng:** Consumer đọc lại **toàn bộ 35 message** từ đầu (từ offset 0 của cả 3 partition).
- **Giải thích:** Mỗi Consumer Group có một dòng offset độc lập được lưu trong Kafka. Group mới chưa từng commit offset nên sẽ tuân theo chính sách `auto_offset_reset` (nếu là `earliest` sẽ đọc lại toàn bộ lịch sử topic).

---

### 6.4. Thử nghiệm 5: Dừng Consumer, gửi thêm message, quan sát LAG & chạy lại
1. **Dừng consumer** thuộc nhóm `group_brand_new_456` (đã commit đủ 35 message trước đó).
2. **Gửi thêm 3 message mới** bằng Producer (`extra_0`, `extra_1`, `extra_2`).
3. **Kiểm tra trạng thái group bằng lệnh `kafka-consumer-groups`:**
   ```powershell
   docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group_brand_new_456
   ```
   **Kết quả ghi nhận:**
   ```text
   Consumer group 'group_brand_new_456' has no active members.

   GROUP               TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
   group_brand_new_456 orders_stream   0          13              13              0    -            -     -
   group_brand_new_456 orders_stream   1          9               11              2    -            -     -
   group_brand_new_456 orders_stream   2          13              14              1    -            -     -
   ```
   - **Hiện tượng:**
     - `CURRENT-OFFSET` giữ nguyên (P1=9, P2=13).
     - `LOG-END-OFFSET` tăng lên (P1 tăng thành 11, P2 tăng thành 14).
     - **LAG xuất hiện:** Partition 1 có **LAG = 2**, Partition 2 có **LAG = 1**. Tổng LAG = **3**.
4. **Khởi động lại Consumer cùng `group_brand_new_456`:**
   ```text
   Restarted consumer read 3 messages:
     Partition 2, Offset 13: Key=extra_1, Val=extra_msg_1
     Partition 1, Offset 9: Key=extra_0, Val=extra_msg_0
     Partition 1, Offset 10: Key=extra_2, Val=extra_msg_2
   ```
   - **Hiện tượng:** Consumer đọc chính xác 3 message mới từ offset kế tiếp (`P2: offset 13`, `P1: offset 9, 10`), tuyệt đối không đọc lại 35 message cũ. Sau khi đọc xong, LAG trở về **0**.

---

## 7. Trả Lời Các Câu Hỏi Cuối Bài

### Câu 1: Topic, Partition, Offset là gì?
- **Topic:** Là kênh/chủ đề logic để tổ chức và phân loại các luồng dữ liệu trong Kafka (tương tự như một bảng trong CSDL hoặc thư mục dữ liệu).
- **Partition:** Là đơn vị lưu trữ vật lý và phân tán nhỏ nhất của Topic. Một topic được chia thành nhiều partition để cho phép xử lý song song (scale-out) trên nhiều máy chủ broker.
- **Offset:** Là một số nguyên tăng đơn điệu ($0, 1, 2, \dots$) đóng vai trò như chỉ mục địa chỉ (ID duy nhất) của từng message **bên trong một partition cụ thể**.

---

### Câu 2: Vì sao message cùng key vào cùng partition, thứ tự message được đảm bảo trong phạm vi nào?
- **Vì sao cùng key vào cùng partition:** Mặc định Kafka sử dụng thuật toán băm (hashing function) `murmur2`:
  $$\text{partition} = \text{murmur2}(\text{key}) \pmod{\text{num\_partitions}}$$
  Với cùng một giá trị key và số lượng partition không đổi, công thức luôn trả về một chỉ số partition duy nhất.
- **Phạm vi bảo đảm thứ tự:** Kafka **chỉ bảo đảm thứ tự nghiêm ngặt (Strict FIFO Order) trong phạm vi TỪNG PARTITION**. Giữa các partition khác nhau trong cùng một topic, Kafka không bảo đảm thứ tự xuất hiện toàn cục. Do đó, việc gắn key (như `order_id` hoặc `customer_id`) là mấu chốt để toàn bộ lịch sử của một thực thể luôn được xử lý đúng trình tự thời gian.

---

### Câu 3: Consumer group là gì, topic có 3 partition mà có 4 consumer cùng group thì chuyện gì xảy ra?
- **Consumer Group:** Là tập hợp một hoặc nhiều consumer làm việc cùng nhau dưới một `group.id` để chia sẻ tải xử lý dữ liệu từ một hoặc nhiều topic. Mỗi partition chỉ được giao cho tối đa một consumer trong group tiêu thụ tại một thời điểm.
- **Khi topic có 3 partition mà có 4 consumer cùng group:**
  - 3 consumer sẽ được giao phụ trách 3 partition (tỷ lệ $1:1$).
  - **Consumer thứ 4 sẽ hoàn toàn IDLE (rảnh rỗi)**, không đọc được dữ liệu nào. Consumer này đóng vai trò dự phòng (standby), chỉ được kích hoạt (rebalance) khi một trong 3 consumer kia bị chết hoặc ngắt kết nối.

---

### Câu 4: Offset đã đọc được lưu ở đâu?
- Kể từ các phiên bản Kafka hiện đại (và cả KRaft), offset đã commit của consumer group được lưu trữ trong một **Topic nội bộ đặc biệt tên là `__consumer_offsets`** ngay trên cụm Kafka Broker (được phân tán và nhân bản đảm bảo chịu lỗi).
- Kafka không còn lưu offset trên Zookeeper như các phiên bản cũ 0.8 về trước.

---

### Câu 5: Lag là gì, lag tăng có nghĩa là gì?
- **Lag:** Là khoảng chênh lệch giữa vị trí message mới nhất được ghi vào partition (`LOG-END-OFFSET`) và vị trí message mới nhất mà consumer group đã xử lý/commit (`CURRENT-OFFSET`):
  $$\text{LAG} = \text{LOG-END-OFFSET} - \text{CURRENT-OFFSET}$$
- **Lag tăng có nghĩa là:** Tốc độ sản sinh dữ liệu của Producer đang nhanh hơn tốc độ tiêu thụ/xử lý của Consumer (hoặc Consumer bị treo, chết, hay gặp nghẽn mạng/tính toán). Đây là chỉ số quan trọng nhất trong giám sát (monitoring) Kafka để cảnh báo tắc nghẽn đường ống dữ liệu.

---

### Câu 6: Message không phải JSON gửi vào Kafka thì Kafka có báo lỗi không, vì sao?
- **Kafka KHÔNG báo lỗi.**
- **Vì sao:** Kafka là một nền tảng vận chuyển thông điệp ở dạng **mảng byte thô (`byte[]`)**. Broker Kafka hoàn toàn không quan tâm đến nội dung hay cấu trúc dữ liệu bên trong (JSON, CSV, Protobuf, Avro, Text hay Binary hình ảnh). Trách nhiệm parse và kiểm tra tính hợp lệ của dữ liệu thuộc về phía Client (Consumer / Spark Streaming application).

---

### Câu 7: Đoán thử: Khi Spark đọc topic này thì một partition tương ứng với gì (Ngày 3 sẽ kiểm chứng)?
- **Dự đoán:** Khi Apache Spark (Structured Streaming / RDD) đọc topic `orders_stream` có 3 partition:
  - Một **Kafka Partition** sẽ ánh xạ tương ứng thành **một Spark Partition (hoặc một Task xử lý tính toán)** trong mỗi micro-batch.
  - Do topic có 3 partition, Spark sẽ phân bổ tối đa **3 Tasks chạy song song** trên các Core của Spark Executor để kéo dữ liệu từ 3 partition tương ứng về bộ nhớ xử lý.

---
---

# Báo cáo thực hành (Ngày 3): Spark Đọc Kafka (Structured Streaming)

Báo cáo chi tiết quá trình tích hợp Apache Spark Structured Streaming với cụm Kafka KRaft: Đọc luồng dữ liệu thô, Parse JSON và xử lý lỗi cú pháp theo mẫu Dead Letter Queue (DLQ), làm sạch dữ liệu, Join luồng với bảng tĩnh `customers.csv` (Stream-Static Join) và 3 thử nghiệm quan sát hành vi runtime.

---

## 8. Chuẩn Bị Môi Trường & Thiết Lập Gói Thư Viện

### 8.1. Kiểm tra Kafka Broker & Topic
- **Trạng thái Broker:** Container Docker `kafka` (image `apache/kafka:latest`, KRaft mode) đang chạy ổn định tại `localhost:9092`.
- **Topic kiểm thử:** `orders_stream` gồm 3 partitions (`0`, `1`, `2`) đã được nạp sẵn hơn 70 messages từ đợt thực hành Ngày 2:
  ```powershell
  docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders_stream
  # Kết quả:
  # orders_stream:0:32
  # orders_stream:1:22
  # orders_stream:2:24
  ```

### 8.2. Xác định và nạp gói `spark-sql-kafka`
- **Phiên bản hệ thống:**
  - Python: 3.12 (Conda env `pyspark_env`)
  - Apache Spark: **4.0.1**
  - Scala build: **2.13.16**
- **Maven Coordinate tương ứng:**
  Gói Kafka connector phải khớp chính xác phiên bản Spark và Scala:
  $$\text{Package: } \mathbf{org.apache.spark:spark-sql-kafka-0-10\_2.13:4.0.1}$$
- **Cấu hình SparkSession tối ưu trên Windows:**
  ```python
  spark = (
      SparkSession.builder
      .appName("Spark_Kafka_Lab_13")
      .master("local[2]")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
  )
  ```
  *(Lưu ý kỹ thuật: Trên Windows, cần cấu hình đồng bộ cả `spark.driver.host` và `spark.driver.bindAddress` về `127.0.0.1` để tránh lỗi Netty BlockManager không thể phân giải hostname).*

### 8.3. Chuẩn bị bảng danh mục tĩnh `customers.csv`
Tạo file [customers.csv](file:///c:/Users/Administrator/spark%20introduce%20learn/exercises/13-spark-kafka/customers.csv) phục vụ Yêu cầu 3 với 3 cột (`customer_id`, `customer_name`, `customer_type`):
```csv
customer_id,customer_name,customer_type
C001,Nguyen Van A,VIP
C002,Tran Thi B,REGULAR
C003,Le Van C,VIP
C004,Pham Thi D,REGULAR
C005,Hoang Van E,NEW
C006,Vu Thi F,REGULAR
C007,Dang Van G,VIP
C008,Bui Thi H,NEW
C009,Do Van I,REGULAR
C010,Ngo Thi K,VIP
```
*(Ghi chú: Khách hàng `C999` có trong đơn hàng nhưng cố tình không đưa vào danh mục để kiểm thử trường hợp unmapped).*

---

## 9. Yêu Cầu 1 – Đọc Thô Từ Kafka (Raw Stream)

### 9.1. Cấu hình đọc Stream
Sử dụng API `spark.readStream` với format `kafka`:
```python
kafka_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "orders_stream")
    .option("startingOffsets", starting_offset) # "earliest" hoặc "latest"
    .load()
)
```

Schema mặc định do Kafka Source cung cấp gồm các cột nhị phân và siêu dữ liệu:
- `key`: `binary`
- `value`: `binary`
- `topic`: `string`
- `partition`: `integer`
- `offset`: `long`
- `timestamp`: `timestamp`
- `timestampType`: `integer`

Trích xuất 6 trường chính theo yêu cầu đề bài:
```python
display_df = kafka_raw_df.select(
    F.col("key").cast("string").alias("key"),
    F.col("value").cast("string").alias("value"),
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp")
)
```

### 9.2. So sánh thực nghiệm `startingOffsets`: `earliest` vs `latest`

#### Kịch bản 1: `startingOffsets = "earliest"`
- **Log thực tế nhận được (Micro-Batch 0):**
```text
-------------------------------------------
Batch: 0
-------------------------------------------
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+---------+------+-----------------------+
|key            |value                                                                                                                                                                          |topic        |partition|offset|timestamp              |
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+---------+------+-----------------------+
|1006           |{"order_id": "1006", "customer_id": "C005", "province": "HaiPhong", "amount": 1200000.0, "status": "Shipping", "order_date": "2025-01-10", "updated_at": "2025-01-10 15:30:00"}|orders_stream|2        |1     |2026-09-29 09:40:09.239|
|NULL           |INVALID_RAW_STRING_MESSAGE_1_NOT_A_JSON                                                                                                                                        |orders_stream|2        |2     |2026-09-29 09:40:09.249|
|1013           |{"order_id": "1013", "customer_id": "C004", "province": "CanTho", "amount": 1100000.0, "status": "pending", "order_date": "2025-01-18", "updated_at": "2025-01-18 13:45:00"}   |orders_stream|2        |3     |2026-09-29 09:40:11.269|
|1019           |{"order_id": "1019", "customer_id": "C001", "province": "HaNoi", "amount": 2700000.0, "status": "SUCCESS", "order_date": "2025-01-24", "updated_at": "2025-01-24 19:50:00"}    |orders_stream|2        |4     |2026-09-29 09:40:11.288|
|1023           |{"order_id": "1023", "customer_id": "C010", "province": "HaNoi", "amount": 3500000.0, "status": "COMPLETED", "order_date": "", "updated_at": "2025-01-29 13:50:00"}            |orders_stream|2        |5     |2026-09-29 09:40:13.299|
|1024           |{"order_id": "1024", "customer_id": "C004", "province": "HoChiMinh", "amount": 1900000.0, "status": "SUCCESS", "order_date": "2025-01-30", "updated_at": "2025-01-30 14:20:00"}|orders_stream|2        |6     |2026-09-29 09:40:13.306|
|1025           |{"order_id": "1025", "customer_id": "C005", "province": "DaNang", "amount": 0.0, "status": "CANCELLED", "order_date": "2025-01-31", "updated_at": "2025-01-31 15:10:00"}       |orders_stream|2        |7     |2026-09-29 09:40:13.309|
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+---------+------+-----------------------+
```
- **Hiện tượng:** Spark quét từ Offset `0` của tất cả các Partition. Toàn bộ 70+ message lịch sử lưu trong Kafka từ các đợt chạy trước được nạp đầy đủ ngay tại Batch 0.

#### Kịch bản 2: `startingOffsets = "latest"`
- **Log thực tế nhận được (Micro-Batch 0):**
```text
-------------------------------------------
Batch: 0
-------------------------------------------
+---+-----+-----+---------+------+---------+
|key|value|topic|partition|offset|timestamp|
+---+-----+-----+---------+------+---------+
+---+-----+-----+---------+------+---------+
```
- **Hiện tượng:** Batch 0 hoàn toàn rỗng (`0 rows`). Spark đặt con trỏ đọc tại Offset hiện thời cuối cùng (`Log-End-Offset`) của từng partition, bỏ qua toàn bộ dữ liệu lịch sử và chỉ chờ message mới phát sinh sau khi job khởi động.

---

## 10. Yêu Cầu 2 – Parse Dữ Liệu & Data Cleaning (Dead Letter Queue)

### 10.1. Schema định nghĩa & Hàm `from_json`
Khai báo cấu trúc Schema tương ứng payload đơn hàng:
```python
order_json_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("province", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("updated_at", StringType(), True)
])
```

### 10.2. Cơ chế bảo tồn dòng lỗi (Không để mất mát dữ liệu)
Khi gặp các message sai cú pháp (như chuỗi thô `INVALID_RAW_STRING_MESSAGE_1_NOT_A_JSON` hoặc cú pháp JSON vỡ `'{"order_id": 9999, "broken_json": unclosed_string_error'`), hàm `from_json` của Spark sẽ parse ra Struct rỗng có các trường con mang giá trị `NULL`.
- **Giải pháp bảo toàn dữ liệu (Dead Letter Queue):**
  1. Giữ nguyên chuỗi gốc `value_str` dưới tên cột `raw_value`.
  2. Gắn cờ kiểm tra `is_valid_json = F.col("parsed.order_id").isNotNull()`.
  3. Phân loại mã lỗi `error_reason = MALFORMED_JSON` cho các dòng không parse được để chuyển vào luồng cảnh báo/DLQ mà không loại bỏ khỏi DataFrame.

```python
parsed_df = (
    kafka_df
    .withColumn("value_str", F.col("value").cast(StringType()))
    .withColumn("parsed", F.from_json(F.col("value_str"), order_json_schema))
    .withColumn("is_valid_json", F.col("parsed.order_id").isNotNull())
    .select(
        F.col("parsed.order_id").alias("order_id"),
        F.col("parsed.customer_id").alias("customer_id"),
        F.col("parsed.province").alias("province"),
        F.col("parsed.amount").alias("amount_raw"),
        F.col("parsed.status").alias("status_raw"),
        F.col("parsed.order_date").alias("order_date_raw"),
        F.col("parsed.updated_at").alias("updated_at"),
        F.col("is_valid_json"),
        F.col("value_str").alias("raw_value")
    )
)
```

### 10.3. Các bước Clean dữ liệu
Áp dụng các chuẩn hóa từ bài Batch/Streaming trước:
1. **Chuẩn hóa status:**
   ```python
   .withColumn("status_clean", F.when(F.col("status_raw").isNotNull(), F.upper(F.trim(F.col("status_raw")))).otherwise(None))
   ```
2. **Cast amount sang Double:**
   ```python
   .withColumn("amount_clean", F.expr("try_cast(amount_raw AS DOUBLE)"))
   ```
3. **Parse order_date:**
   ```python
   .withColumn("order_date_clean", F.to_date(F.try_to_timestamp(F.col("order_date_raw"), F.lit("yyyy-MM-dd"))))
   ```
   *(Sử dụng `try_to_timestamp` bọc ngoài `to_date` để triệt tiêu ngoại lệ khi gặp ngày sai cú pháp như `"31/02/2025"`, `"2025-13-40"` hoặc chuỗi rỗng `""`)*.
4. **Phân loại trạng thái bản ghi:**
   - `CLEAN_RECORD`: Hợp lệ toàn bộ.
   - `MALFORMED_JSON`: Lỗi cú pháp JSON.
   - `INVALID_AMOUNT`: Giá trị amount null hoặc $\le 0$.
   - `INVALID_DATE`: Ngày đặt hàng sai định dạng.

- **Bảng kết quả console thực tế:**
```text
+--------+-----------+---------+------------+------------+----------------+-------------+-----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|order_id|customer_id|province |amount_clean|status_clean|order_date_clean|is_valid_json|error_reason           |raw_value                                                                                                                                                                      |
+--------+-----------+---------+------------+------------+----------------+-------------+-----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|1005    |C001       |HaNoi    |0.0         |SUCCESS     |2025-01-09      |true         |INVALID_AMOUNT         |{"order_id": "1005", "customer_id": "C001", "province": "HaNoi", "amount": 0.0, "status": "SUCCESS", "order_date": "2025-01-09", "updated_at": "2025-01-09 14:00:00"}          |
|1006    |C005       |HaiPhong |1200000.0   |SHIPPING    |2025-01-10      |true         |CLEAN_RECORD           |{"order_id": "1006", "customer_id": "C005", "province": "HaiPhong", "amount": 1200000.0, "status": "Shipping", "order_date": "2025-01-10", "updated_at": "2025-01-10 15:30:00"}|
|NULL    |NULL       |NULL     |NULL        |NULL        |NULL            |false        |MALFORMED_JSON         |INVALID_RAW_STRING_MESSAGE_1_NOT_A_JSON                                                                                                                                        |
|1013    |C004       |CanTho   |1100000.0   |PENDING     |2025-01-18      |true         |CLEAN_RECORD           |{"order_id": "1013", "customer_id": "C004", "province": "CanTho", "amount": 1100000.0, "status": "pending", "order_date": "2025-01-18", "updated_at": "2025-01-18 13:45:00"}   |
|1023    |C010       |HaNoi    |3500000.0   |COMPLETED   |NULL            |true         |INVALID_DATE           |{"order_id": "1023", "customer_id": "C010", "province": "HaNoi", "amount": 3500000.0, "status": "COMPLETED", "order_date": "", "updated_at": "2025-01-29 13:50:00"}            |
|1025    |C005       |DaNang   |0.0         |CANCELLED   |2025-01-31      |true         |INVALID_AMOUNT         |{"order_id": "1025", "customer_id": "C005", "province": "DaNang", "amount": 0.0, "status": "CANCELLED", "order_date": "2025-01-31", "updated_at": "2025-01-31 15:10:00"}       |
+--------+-----------+---------+------------+------------+----------------+-------------+-----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

---

## 11. Yêu Cầu 3 – Join Với Dữ Liệu Tĩnh (Stream-Static Join)

### 11.1. Bản chất của Stream-Static Join
- Bảng tĩnh `customers.csv` được nạp qua `spark.read` (Batch DataFrame không có streaming source).
- Luồng `orders` được đọc qua `spark.readStream`.
- Trong Structured Streaming, khi join một Streaming DataFrame với một Batch DataFrame, Spark tự động tối ưu hóa thành **Stream-Static Join (thường dùng Broadcast Hash Join)**: Bảng tĩnh được phát (broadcast) tới các executor; ở mỗi micro-batch, các record của luồng stream sẽ tra cứu trực tiếp vào bảng tĩnh trong bộ nhớ.

```python
# 1. Đọc static DataFrame
customers_df = spark.read.format("csv").option("header", "true").schema(customer_schema).load("customers.csv")

# 2. Left join Stream orders với Static customers
enriched_stream = (
    parsed_orders
    .join(customers_df, on="customer_id", how="left")
    .withColumn("is_customer_matched", F.col("customer_name").isNotNull())
)
```

### 11.2. Nhận diện các đơn hàng không map được khách hàng
- **Kết quả hiển thị console:**
```text
+--------+-----------+-------------+-------------+-------------------+---------+------------+------------+----------------+
|order_id|customer_id|customer_name|customer_type|is_customer_matched|province |amount_clean|status_clean|order_date_clean|
+--------+-----------+-------------+-------------+-------------------+---------+------------+------------+----------------+
|1005    |C001       |Nguyen Van A |VIP          |true               |HaNoi    |0.0         |SUCCESS     |2025-01-09      |
|1006    |C005       |Hoang Van E  |NEW          |true               |HaiPhong |1200000.0   |SHIPPING    |2025-01-10      |
|NULL    |NULL       |NULL         |NULL         |false              |NULL     |NULL        |NULL        |NULL            |
|1013    |C004       |Pham Thi D   |REGULAR      |true               |CanTho   |1100000.0   |PENDING     |2025-01-18      |
|1017    |C999       |NULL         |NULL         |false              |DaNang   |2200000.0   |SUCCESS     |2025-01-22      |
|1019    |C001       |Nguyen Van A |VIP          |true               |HaNoi    |2700000.0   |SUCCESS     |2025-01-24      |
|1023    |C010       |Ngo Thi K    |VIP          |true               |HaNoi    |3500000.0   |COMPLETED   |NULL            |
+--------+-----------+-------------+-------------+-------------------+---------+------------+------------+----------------+
```
- **Nhận xét:**
  - Các order hợp lệ (`C001` $\to$ `C010`) được ghép thành công với `customer_name` và phân loại hạng khách hàng (`customer_type`: `VIP`, `REGULAR`, `NEW`), `is_customer_matched = true`.
  - Đơn hàng `1017` mang `customer_id = 'C999'` không có trong danh mục khách hàng: Sau Left Join, các cột `customer_name` và `customer_type` mang giá trị `NULL`, cờ `is_customer_matched = false`.
  - Các dòng bị lỗi cú pháp JSON: `customer_id` bị null nên cũng không map được khách hàng.

---

## 12. Yêu Cầu 4 – Các Thử Nghiệm Quan Sát & Đo Lường

### 12.1. Thử nghiệm 1: Gửi thêm message trong lúc streaming job đang chạy
- **Kịch bản:** Job Spark đang chạy với `startingOffsets="latest"` và `trigger(processingTime="3 seconds")`. Sau khi Batch 0 hoàn tất rỗng, tiến trình ngầm dùng `KafkaProducer` gửi 3 đơn hàng mới: `2001`, `2002`, `2003`.
- **Log thực tế nhận được:**
```text
[PRODUCER] Gửi 3 message (Live Streaming Orders 2001, 2002, 2003) vào Kafka topic 'orders_stream'...
[PRODUCER] Đã gửi thành công 3 message.

-------------------------------------------
Batch: 1
-------------------------------------------
+----+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------+------+-----------------------+
|key |value                                                                                                                                                                            |partition|offset|timestamp              |
+----+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------+------+-----------------------+
|2003|{"order_id": "2003", "customer_id": "C999", "province": "DaNang", "amount": 550000.0, "status": "PENDING", "order_date": "2025-02-10", "updated_at": "2025-02-10 10:02:00"}      |2        |22    |2026-09-30 10:21:07.067|
|2001|{"order_id": "2001", "customer_id": "C001", "province": "HaNoi", "amount": 999000.0, "status": "SUCCESS", "order_date": "2025-02-10", "updated_at": "2025-02-10 10:00:00"}       |0        |30    |2026-09-30 10:21:07.067|
|2002|{"order_id": "2002", "customer_id": "C002", "province": "HoChiMinh", "amount": 1250000.0, "status": "COMPLETED", "order_date": "2025-02-10", "updated_at": "2025-02-10 10:01:00"}|0        |31    |2026-09-30 10:21:07.067|
+----+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------+------+-----------------------+
```
- **Hiện tượng:** Ngay chu kỳ trigger tiếp theo (Batch 1), Spark phát hiện offset mới trong Kafka và tự động kéo chính xác 3 message vừa gửi về xử lý.

### 12.2. Thử nghiệm 2: Dừng job, gửi thêm message, chạy lại với `startingOffsets="latest"`
- **Kịch bản:** Dừng hoàn toàn Spark streaming query. Trong lúc job đang tắt, gửi 2 message offline: `3001` và `3002`. Sau đó khởi động lại job mới với `startingOffsets="latest"`.
- **Log thực tế nhận được:**
```text
[PRODUCER] Gửi 2 message (Gửi khi job đã tắt: 3001, 3002) vào Kafka topic 'orders_stream'...
[PRODUCER] Đã gửi thành công 2 message.

-------------------------------------------
Batch: 0
-------------------------------------------
+---+-----+---------+------+
|key|value|partition|offset|
+---+-----+---------+------+
+---+-----+---------+------+
```
- **Hiện tượng:** Batch 0 hoàn toàn rỗng. Cả 2 message `3001` và `3002` (cùng tất cả message cũ trước đó) **đều không được đọc lại**.
- **Nguyên nhân:** Khi chạy lại mà không có checkpoint định kỳ lưu offset, cấu hình `latest` chỉ định Spark lấy vị trí hiện tại của Topic tại thời điểm bắt đầu query làm mốc ban đầu. Toàn bộ các message đến trước thời điểm query khởi chạy đều bị bỏ qua.

### 12.3. Thử nghiệm 3: So sánh Partition trong Kafka với Partition do Spark tạo ra
Sử dụng `foreachBatch` kết hợp `rdd.getNumPartitions()` và `rdd.glom()` để bóc tách cấu trúc vật lý của DataFrame trong mỗi micro-batch:
```python
def analyze_batch(batch_df, batch_id):
    num_spark_parts = batch_df.rdd.getNumPartitions()
    glommed = batch_df.select("partition", "offset").rdd.glom().collect()
    for spark_part_id, rows in enumerate(glommed):
        kafka_parts = set(r["partition"] for r in rows)
        print(f"  + Spark Partition [{spark_part_id}]: {len(rows)} dòng | Kafka Partition: {kafka_parts}")
```
- **Kết quả đo lường thực tế trên console:**
```text
************************************************************
KẾT QUẢ PHÂN TÍCH ÁNH XẠ PARTITION Ở MICRO-BATCH 0:
-> Tổng số dòng trong Batch: 78
-> Số lượng Spark Partitions: 3
  + Spark Partition [0]: 24 dòng | Kafka Partition: {2} | Offset: [0 -> 23]
  + Spark Partition [1]: 22 dòng | Kafka Partition: {1} | Offset: [0 -> 21]
  + Spark Partition [2]: 32 dòng | Kafka Partition: {0} | Offset: [0 -> 31]
************************************************************
```
- **Kết luận:**
  1. Số lượng Spark Partition tạo ra bằng **chính xác số lượng Kafka Partition của Topic** ($3 = 3$).
  2. Mỗi Spark Partition ánh xạ độc quyền với đúng 1 Kafka Partition (Partition 0 Spark $\leftrightarrow$ Partition 2 Kafka, Partition 1 Spark $\leftrightarrow$ Partition 1 Kafka, Partition 2 Spark $\leftrightarrow$ Partition 0 Kafka).
  3. Dự đoán ở Ngày 2 (Câu 7) hoàn toàn trùng khớp với thực nghiệm!

---

## 13. Trả Lời Các Câu Hỏi Cuối Bài (Ngày 3)

### Câu 1: Vì sao khi đọc Kafka cần cast value sang string trước khi parse JSON?
- **Bản chất của Kafka:** Kafka lưu trữ và vận chuyển message dưới dạng mảng byte thô (`binary` / `byte[]`). Do đó khi Spark đọc topic Kafka qua connector, cột `value` có kiểu dữ liệu là `BinaryType`.
- **Yêu cầu của hàm `from_json`:** Hàm `from_json(col, schema)` trong Spark SQL chỉ chấp nhận đầu vào là một chuỗi ký tự (`StringType`) chứa biểu diễn JSON hợp lệ. Nếu truyền trực tiếp cột `BinaryType` vào `from_json`, Spark Catalyst Analyzer sẽ báo lỗi không khớp kiểu dữ liệu (`AnalysisException: cannot resolve 'from_json' due to data type mismatch`).
- Vì vậy, bước `F.col("value").cast("string")` là bắt buộc để giải mã mảng byte thành chuỗi văn bản JSON trước khi bóc tách schema.

---

### Câu 2: Message JSON lỗi thì cột ra sao sau `from_json`, vì sao không nên loại bỏ ngay các dòng này?
- **Trạng thái cột sau `from_json` khi JSON lỗi:**
  - Khi gặp message không thể parse (ví dụ chuỗi thô không theo chuẩn JSON hoặc JSON bị thiếu ngoặc, gãy vỡ), hàm `from_json` của Spark mặc định hoạt động ở chế độ `PERMISSIVE`: nó **không làm crash ứng dụng** mà trả về giá trị `NULL` cho Struct/các trường bên trong.
- **Vì sao không nên loại bỏ ngay các dòng này (`filter(isNotNull)`)?**
  1. **Nguy cơ mất mát dữ liệu nghiêm trọng (Data Loss):** Trong kiến trúc Streaming của doanh nghiệp, message lỗi có thể là một giao dịch thanh toán quan trọng bị lỗi định dạng do bên thứ 3 hoặc do phiên bản app cũ. Nếu drop ngay lập tức, dữ liệu biến mất vĩnh viễn và không thể đối soát.
  2. **Mô hình Dead-Letter Queue (DLQ):** Thực tiễn tốt nhất trong Data Engineering là giữ lại chuỗi thô (`raw_value`), gắn cờ cảnh báo (`is_valid_json = false`, `error_reason = MALFORMED_JSON`) và định tuyến (route) các dòng này về một Topic hoặc bảng lưu trữ riêng (Dead-Letter Queue). Đội vận hành có thể theo dõi, cảnh báo hệ thống nguồn và viết script sửa/bù đắp dữ liệu sau.

---

### Câu 3: `earliest` và `latest` khác nhau ở điểm nào, ảnh hưởng gì đến kết quả đọc được?
| Đặc điểm | `startingOffsets = "earliest"` | `startingOffsets = "latest"` |
| :--- | :--- | :--- |
| **Vị trí bắt đầu đọc** | Đọc từ **Offset nhỏ nhất (`0`)** còn tồn tại trong mỗi partition của topic. | Đọc từ **Offset mới nhất hiện tại (`Log-End-Offset`)** của mỗi partition. |
| **Ảnh hưởng dữ liệu** | Đọc lại **toàn bộ dữ liệu lịch sử** đã từng gửi vào Kafka từ trước đến nay. | **Bỏ qua toàn bộ dữ liệu lịch sử**; chỉ đọc các message gửi đến **sau khi** query streaming đã khởi động. |
| **Batch 0** | Micro-Batch đầu tiên chứa khối lượng lớn dữ liệu cũ (Backfill/Reprocessing). | Micro-Batch đầu tiên thường rỗng (`0 rows`) nếu chưa có message mới. |
| **Ứng dụng thực tế** | Dùng khi triển khai pipeline mới cần nạp lại toàn bộ lịch sử (Backfill) hoặc tính toán lại từ đầu. | Dùng khi chỉ quan tâm đến dữ liệu thời gian thực (Real-time alerting), không cần xử lý dữ liệu cũ. |

---

### Câu 4: Vì sao join ở đây là stream-static join, không phải stream-stream join?
- **Stream-Static Join:** Là phép join giữa một luồng dữ liệu liên tục (`orders` - Streaming DataFrame) và một tập dữ liệu tĩnh cố định (`customers.csv` - Batch DataFrame nạp qua `spark.read`).
  - Phép join này không cần duy trì bộ nhớ đệm trạng thái (State Store) theo thời gian hay thiết lập Watermark, vì tập dữ liệu tĩnh luôn sẵn sàng trong bộ nhớ (được broadcast đến các executor).
- **Stream-Stream Join:** Là phép join giữa hai luồng dữ liệu streaming độc lập (ví dụ luồng clickstream và luồng payment). Cả hai bên đều biến động theo thời gian, đòi hỏi Spark phải lưu trữ trạng thái trong State Store và yêu cầu bắt buộc phải có Watermark cùng điều kiện ràng buộc thời gian (Time Constraint) để dọn dẹp các bản ghi quá hạn, tránh tràn bộ nhớ.
- Trong bài thực hành này, danh mục khách hàng là bảng tra cứu tĩnh, nên mô hình chuẩn xác là **Stream-Static Join**.

---

### Câu 5: Một partition của Kafka tương ứng với gì khi Spark đọc topic?
- **Ánh xạ 1-1:** Khi Apache Spark đọc một Kafka topic, **một Kafka Partition tương ứng với chính xác một Spark Partition** trong RDD/DataFrame của mỗi micro-batch:
  $$\mathbf{1 \text{ Kafka Partition}} \iff \mathbf{1 \text{ Spark Partition}} \iff \mathbf{1 \text{ Spark Task}}$$
- **Ý nghĩa về mặt tính toán & hiệu năng:**
  - Topic `orders_stream` có **3 partition** $\implies$ Spark tạo ra **3 partitions** trong mỗi micro-batch và phân bổ **3 Tasks** chạy song song trên các Core của Executor.
  - Số lượng Kafka partition chính là trần giới hạn tối đa cho mức độ song song hóa (Parallelism) ở tầng nạp dữ liệu (read) của Spark. Muốn tăng số Task đọc đồng thời từ Kafka, bắt buộc phải tăng số lượng partition của Topic trên cụm Kafka.

