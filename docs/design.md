# Thiết kế hệ thống – DS02: Chia sẻ file P2P mô phỏng BitTorrent

> Tài liệu này là nguồn để viết các chương 5–12 của `docs/report.pdf`.

## 1. Kiến trúc tổng quan

```
                    +-------------------------------+
                    |            TRACKER            |
                    |  HTTP/JSON, đa luồng          |
                    |  - danh sách peer (soft state)|
                    |  - metadata file (lưu đĩa)    |
                    |  - peer nào giữ chunk nào     |
                    +---^-----------^-----------^---+
     register/heartbeat |  announce |  get peers| (chỉ metadata, KHÔNG có nội dung file)
                        |           |           |
          +-------------+--+   +----+--------+  +--+-------------+
          |   PEER A       |   |   PEER B    |  |   PEER C        |
          | (seeder)       |   | (seeder)    |  | (leecher)       |
          | UploadServer   |<--+-------------+--+ Downloader      |
          | FileStore      |   | UploadServer|<-+ (song song)     |
          +-------^--------+   +------^------+  | UploadServer ---+--> peer D ...
                  |   TCP: HELLO / GET_CHUNK / CHUNK (nội dung chunk đi trực tiếp giữa các peer)
                  +-------------------------------+
```

| Thành phần | File | Trách nhiệm |
|---|---|---|
| Tracker | `src/tracker/server.py`, `state.py` | Đăng ký và xác thực peer, cấp token, nhận heartbeat, loại peer quá hạn, lưu metadata, nhận announce, trả danh sách peer có chunk |
| Metadata generator | `src/common/metadata.py` | Chia file thành chunk, tính SHA-256 từng chunk và cả file, tính `file_id`, kiểm tra metadata |
| FileStore | `src/peer/storage.py` | Lưu chunk ra đĩa (ghi atomic), kiểm tra hash, quét lại khi restart (resume), ghép file |
| UploadServer | `src/peer/uploader.py` | Server TCP phục vụ chunk cho peer khác, giới hạn số kết nối, xác thực token |
| Downloader | `src/peer/downloader.py` | Tải song song, rarest-first, retry, chuyển nguồn, từ chối chunk hỏng |
| PeerNode | `src/peer/node.py` | Gắn các phần trên lại, heartbeat, announce định kỳ, tự đăng ký lại |
| Giao thức | `src/common/protocol.py` | Đóng/mở frame TCP, giới hạn kích thước |

Mọi node là **tiến trình riêng**, giao tiếp **chỉ qua mạng**: HTTP với tracker, TCP giữa các peer. Không dùng bộ nhớ hay file chung giữa các node.

## 2. Định danh và khám phá (naming & discovery)

- **Peer ID**: chuỗi `[A-Za-z0-9_-]{1,64}`, do người chạy đặt (`--peer-id`). Tracker không cho hai peer đang online dùng cùng một ID.
- **File ID**: `SHA-256(JSON chuẩn hóa của {name, size, chunk_size, num_chunks, chunk_hashes, file_hash})`. Hai seeder có cùng file và cùng chunk_size sẽ ra cùng file_id, nhờ vậy một file có nhiều nguồn mà không cần phối hợp trước.
- **Khám phá peer**: dùng tracker tập trung. Peer gọi `GET /api/files/<id>/peers` để lấy danh sách `{peer_id, host, port, chunks}` của các peer còn sống, và làm mới danh sách mỗi `PEER_REFRESH_INTERVAL` giây.
- **Địa chỉ**: peer tự khai `--advertise-host`; nếu không khai, tracker dùng IP nguồn của request.

## 3. Giao thức

### 3.1 Peer ↔ Tracker (HTTP/JSON)

| Method | Path | Body | Ghi chú |
|---|---|---|---|
| POST | `/api/register` | `{peer_id, port, host?, join_key}` | Trả về `{token, heartbeat_interval, peer_timeout}` |
| POST | `/api/heartbeat` | – | 404 `UNKNOWN_PEER` → peer phải đăng ký lại |
| POST | `/api/leave` | – | Peer rời mạng chủ động |
| POST | `/api/files` | `{metadata}` | Idempotent: đăng ký lại cùng file không tạo bản sao |
| GET | `/api/files` | – | Danh sách file kèm số peer và số seeder |
| GET | `/api/files/<id>` | – | Metadata |
| POST | `/api/announce` | `{file_id, chunks:[...], seq}` | `seq` tăng dần, announce cũ bị bỏ qua |
| GET | `/api/files/<id>/peers` | – | Không trả về chính peer gọi |
| POST | `/api/verify` | `{token}` | Uploader dùng để xác thực peer đang gọi tới |

Mọi endpoint, trừ `register` và `health`, cần header `Authorization: Bearer <token>`.

### 3.2 Peer ↔ Peer (TCP, frame nhị phân)

```
| 4 byte: độ dài header (big-endian) | header JSON | payload (header.size byte) |
```

```
Downloader                         Uploader
   | HELLO {peer_id, token}  --------> |  kiểm tra token qua tracker (/api/verify, cache 30s)
   | <-------- HELLO_OK | ERROR AUTH_FAILED
   | GET_CHUNK {file_id, index, request_id} -->
   | <-- CHUNK {file_id, index, request_id} + bytes | ERROR NOT_HAVE | ERROR BUSY
   | BYE ----------------------------> |
```

Mã lỗi: `AUTH_FAILED`, `BUSY` (hết slot upload), `NOT_HAVE`, `BAD_REQUEST`.

### 3.3 Kết nối lâu dài và pipeline (`src/peer/connection.py`)

Downloader giữ **một kết nối TCP lâu dài tới mỗi peer nguồn**. Bắt tay `HELLO` và xác thực token chỉ làm một lần cho mỗi kết nối. Các luồng tải gửi nhiều `GET_CHUNK` liên tiếp trên cùng kết nối mà không chờ response của request trước (**pipeline**), tối đa `MAX_REQUESTS_PER_PEER` request đang chờ trên mỗi peer.

```
luồng tải 1 --GET_CHUNK(rid=a)-->|
luồng tải 2 --GET_CHUNK(rid=b)-->|  1 kết nối TCP  -->  uploader
luồng tải 3 --GET_CHUNK(rid=c)-->|
reader thread <--CHUNK(rid=b)----|   ghép response với request theo request_id
              <--CHUNK(rid=a)----|   (đúng cả khi response về khác thứ tự)
```

- Mỗi kết nối có một **reader thread** đọc response và hoàn tất `Future` tương ứng theo `request_id`.
- Response mang `request_id` không có trong danh sách đang chờ (trùng lặp hoặc giả mạo) bị coi là lỗi giao thức, và kết nối bị đóng.
- `ERROR NOT_HAVE` chỉ làm hỏng request đó, kết nối vẫn dùng tiếp được.
- Kết nối hỏng (peer chết, timeout, sai giao thức) thì **mọi request đang chờ trên kết nối thất bại ngay**. Downloader đưa các chunk đó về hàng đợi và chọn peer khác. Lần sau nếu peer sống lại sẽ mở kết nối mới.
- Peer bị cấm vì gửi chunk hỏng thì kết nối tới peer đó bị đóng ngay.

Hiệu quả đo được: xem `docs/test-plan.md`, mục "Kết nối lâu dài + pipeline".

## 4. Trạng thái phân tán

| Trạng thái | Nơi giữ | Bền vững? |
|---|---|---|
| Metadata file | Tracker (`data/tracker/state.json`) và mỗi peer (`meta.json`) | Có, được checkpoint ra đĩa |
| Peer online | Tracker, trong RAM | Không (soft state, tự dựng lại nhờ heartbeat và register) |
| Peer nào giữ chunk nào | Tracker, trong RAM | Không, dựng lại từ announce |
| Chunk đã có | Peer, trên đĩa (`chunks/<i>.chunk`) | Có, đây là nguồn sự thật |
| Token của peer | Peer (`data/<peer>/token`) | Có, để đăng ký lại ngay sau khi restart |

Danh sách chunk trên tracker là **bản sao có độ trễ** (eventual consistency) của trạng thái thật ở peer. Nếu tracker trả thông tin cũ, downloader sẽ nhận `NOT_HAVE`; khi đó nó đánh dấu lỗi, làm mới danh sách và chọn peer khác. Vì vậy tracker sai tạm thời **không làm sai dữ liệu**, chỉ làm chậm.

## 5. Phát hiện online/offline, timeout, ACK, retry

- **Heartbeat**: peer gửi mỗi `HEARTBEAT_INTERVAL` giây (mặc định 3s). Luồng reaper của tracker chạy mỗi 1s và xóa peer quá `PEER_TIMEOUT` (mặc định 10s) không heartbeat (log `PEER_EXPIRED`).
- **Timeout**: `CONNECT_TIMEOUT` khi kết nối TCP, `REQUEST_TIMEOUT` khi chờ response, timeout cho request HTTP tới tracker.
- **ACK**: mỗi `GET_CHUNK` phải được trả bằng `CHUNK` hoặc `ERROR`. Hash SHA-256 đúng là bằng chứng chunk đã đến nguyên vẹn.
- **Bounded retry**:
  - Mỗi chunk được thử tối đa `MAX_CHUNK_RETRIES` lần, và lần thử sau dùng peer khác (`failed_on[chunk]`).
  - Peer lỗi kết nối bị tạm bỏ qua `PEER_COOLDOWN` giây.
  - Gọi tracker có retry với exponential backoff (0.5s, 1s, 2s).
- **Không còn nguồn**: downloader chờ tối đa `STALL_TIMEOUT` giây để có peer mới, sau đó dừng. Chạy lại lệnh thì tải tiếp (resume).

## 6. Message trùng hoặc sai thứ tự

| Tình huống | Cách xử lý |
|---|---|
| Announce đến trễ hoặc gửi lại | `seq` tăng đơn điệu (`max(last+1, time_ms)`, vẫn tăng qua các lần restart). Tracker bỏ qua announce có `seq ≤ seq` đã nhận (log `ANNOUNCE_STALE`) |
| Chunk nhận trùng | `FileStore.write_chunk` idempotent, ghi lại cùng nội dung không đổi trạng thái. Downloader không bao giờ tải song song cùng một chunk (`busy_chunks`) |
| Chunk đến không theo thứ tự | Mỗi chunk lưu theo index, `assemble` ghép theo thứ tự 0..n-1 |
| Đăng ký file trùng | `POST /api/files` idempotent theo file_id |
| Response không khớp request | Kiểm tra `index` và `request_id`, lệch thì báo `ProtocolError` |

## 7. Đồng thời và nhất quán

**Các xung đột có thể xảy ra**

1. Nhiều peer announce cùng lúc lên tracker (đa luồng).
2. Announce của cùng một peer đến sai thứ tự.
3. Nhiều luồng tải cùng ghi vào kho chunk của một peer.
4. Uploader đọc chunk trong khi downloader của chính peer đó đang ghi chunk khác.
5. Nhiều downloader cùng tải từ một seeder và có thể làm nó quá tải.

**Mô hình nhất quán**

- Nội dung file: **nhất quán mạnh theo nội dung**. Một chunk chỉ được coi là "có" khi đúng hash, nên mọi peer giữ chunk *i* đều có cùng một bản.
- Trạng thái trên tracker: **eventual consistency**. Downloader tự chịu được thông tin cũ (xem mục 4).

**Cơ chế kiểm soát đồng thời**

| Xung đột | Cơ chế |
|---|---|
| 1 | `threading.RLock` bao toàn bộ `TrackerState` |
| 2 | Sequence number cho mỗi cặp (peer, file) |
| 3, 4 | Ghi file tạm rồi `os.replace` (atomic). Tập `_have` có khóa riêng. Chunk chỉ vào `_have` sau khi đã ghi xong |
| 5 | Semaphore `MAX_UPLOAD_CONNS` ở uploader (trả `BUSY`). Downloader giới hạn `MAX_REQUESTS_PER_PEER` và giãn cách khi gặp `BUSY` |

**Kiểm chứng**: `test_concurrent_announces`, `test_stale_announce_is_ignored`, `test_concurrent_chunk_writes`, `test_upload_while_downloading` (xem `docs/test-plan.md`).

## 8. Mô hình lỗi và phục hồi

**Mô hình lỗi**: crash-stop (tiến trình chết đột ngột), mất kết nối mạng, timeout, peer gửi dữ liệu hỏng hoặc độc hại (chỉ hỏng dữ liệu, không giả mạo được token), tracker restart. Không xét lỗi Byzantine của tracker.

| Lỗi | Phát hiện | Phục hồi | Test / demo |
|---|---|---|---|
| Nguồn chết khi đang tải | Lỗi kết nối hoặc timeout. Tracker phát hiện qua heartbeat | Tạm bỏ qua peer đó, chunk được thử lại ở nguồn khác | `test_source_killed_during_download` |
| Chunk hỏng | SHA-256 không khớp | Từ chối chunk, tải lại từ peer khác. Peer gửi hỏng ≥3 lần bị cấm | `test_corrupt_source_is_detected_and_avoided` |
| Downloader bị tắt giữa chừng | – | Khi chạy lại, quét và kiểm tra hash các chunk đã có rồi tải tiếp phần thiếu (resume, checkpoint theo từng chunk) | `test_resume_after_restart` |
| Tracker restart | Peer gặp lỗi kết nối (`TRACKER_DOWN`), sau đó heartbeat bị trả 404 | Metadata nạp lại từ `state.json`. Peer tự đăng ký và announce lại | `test_tracker_restart_recovers` |
| Peer quá tải | Nhận `BUSY` | Giãn cách 0.3s rồi chọn peer khác | – |
| Tracker tạm mất | `TrackerUnavailable` | Peer tiếp tục upload, dùng danh sách peer cũ, retry có backoff | – |

## 9. Bảo mật

| Yêu cầu | Cách đáp ứng |
|---|---|
| Xác thực peer | Để tham gia mạng phải có `JOIN_KEY`. Tracker cấp token HMAC-SHA256 (`peer_id.expiry.sig`) ký bằng `TRACKER_SECRET` |
| Phân quyền truy cập | API tracker cần token. Uploader chỉ phục vụ peer có token hợp lệ, và `peer_id` khai báo phải khớp với token |
| Chống mạo danh Peer ID | Đăng ký trùng ID với peer đang online mà không có token của ID đó bị trả `409 PEER_ID_IN_USE` |
| Không lưu mật khẩu dạng rõ | Tracker chỉ giữ bản băm SHA-256 của `JOIN_KEY` và so sánh bằng `hmac.compare_digest` |
| Bí mật trong biến môi trường | `.env` (đã nằm trong `.gitignore`), có mẫu `.env.example` |
| Kiểm tra dữ liệu đầu vào | Kiểm tra chặt metadata (kiểu, khoảng giá trị, `file_id` khớp nội dung, tên file chống path traversal), Peer ID theo regex, chỉ số chunk |
| Giới hạn kích thước | Header TCP ≤ 64 KB, payload ≤ chunk_size, body HTTP ≤ `MAX_BODY`, file ≤ 4 GB, ≤ 65536 chunk |
| Toàn vẹn dữ liệu | SHA-256 từng chunk và cả file |
| Ghi log | Mọi lần đăng ký, từ chối, announce, gửi/nhận chunk, cấm peer, lỗi |

**Hạn chế**: chưa mã hóa đường truyền (TLS). Có thể bổ sung bằng `ssl.wrap_socket` cho TCP và HTTPS cho tracker (xem mục 10).

## 10. Giới hạn và hướng phát triển

- **Tracker là điểm lỗi đơn lẻ (SPOF)** cho việc khám phá peer và xác thực token. Khi tracker chết, các kết nối đang mở vẫn tải tiếp được, nhưng peer mới không vào được mạng. Hướng phát triển: nhân bản tracker, hoặc dùng DHT (Kademlia).
- Uploader xử lý các request trên một kết nối theo thứ tự (không song song trong cùng kết nối). Song song đến từ việc tải nhiều peer cùng lúc.
- Mỗi lần announce gửi cả danh sách chunk. Có thể gửi bitfield nén hoặc chỉ gửi phần thay đổi.
- Chưa có choking/tit-for-tat, magnet link, TLS.
