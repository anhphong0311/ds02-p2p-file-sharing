# Kịch bản demo (7–10 phút)

Chuẩn bị: 3 máy trong cùng LAN (hoặc 1 máy chạy nhiều cửa sổ, hoặc Docker). Trên mỗi máy, tạo `.env` từ `.env.example` với cùng `JOIN_KEY`, và đặt `TRACKER_URL=http://<IP máy tracker>:8000`.

Mở sẵn một cửa sổ chạy `tail -f logs/<node>.log` (hoặc `Get-Content logs\peerC.log -Wait`) để giải thích log.

| # | Yêu cầu của đề | Thao tác | Log cần chỉ ra |
|---|---|---|---|
| 1 | Tạo metadata, chia sẻ file | `python -m src.peer.main make-meta samples/demo.bin`, sau đó `... --peer-id peerA seed samples/demo.bin` | `SEEDING file_id=...`; trên tracker: `FILE_REGISTERED`, `ANNOUNCE have=120` |
| 2 | Tải song song từ ≥2 peer | Chạy seeder thứ hai `peerB` (cùng file), sau đó `peerC download demo.bin` | `CHUNK_OK ... source=peerA` xen kẽ `source=peerB`; `DOWNLOAD_COMPLETE sources=peerA:61,peerB:59` |
| 3 | Upload trước khi tải xong | Khi peerC đang tải: `fault_inject.ps1 late-leecher` (peerD) | Trên peerC xuất hiện `CHUNK_SENT to=peerD` **trước** `DOWNLOAD_COMPLETE` |
| 4 | Dừng một nguồn khi đang tải | `fault_inject.ps1 kill-peerA` (hoặc đóng cửa sổ peerA) | peerC: `CHUNK_FAILED source=peerA error=ConnectionRefusedError`, rồi chỉ còn `CHUNK_OK source=peerB`. Tracker sau ~10s: `PEER_EXPIRED peer=peerA` |
| 5 | Chuyển sang nguồn khác | (tiếp tục bước 4) | `DOWNLOAD_COMPLETE` |
| 6 | Phát hiện chunk hỏng | `fault_inject.ps1 bad-seeder`, sau đó cho peer mới tải | `CHUNK_REJECTED source=peerBad reason=hash_mismatch`, `PEER_BANNED peer=peerBad` |
| 7 | Ghép file và kiểm tra hash | – | `DOWNLOAD_COMPLETE ... file_hash=verified`; so sánh bằng `certutil -hashfile downloads\peerC\demo.bin SHA256` (Windows) hoặc `sha256sum` (Linux) |
| + | Resume | Nhấn Ctrl+C khi peerC đang tải, chạy lại cùng lệnh | `STORE_RESUMED have=57`, `DOWNLOAD_START already_have=57` |
| + | Tracker restart | Ctrl+C tracker rồi chạy lại | peer: `TRACKER_DOWN`, sau đó `HEARTBEAT_REJECTED`, `REGISTERED`; tracker: `STATE_RESTORED files=1` |
| + | Bảo mật | Chạy peer với `JOIN_KEY` sai | tracker: `REGISTER_REJECTED reason=bad_join_key` |

**Phân vai khi demo** (mọi thành viên đều phải tham gia): TV1 trình bày tracker và discovery (bước 1), TV2 trình bày tải song song và lỗi (bước 2, 4–6), TV3 trình bày upload khi đang tải, resume, hiệu năng (bước 3, 7, +).

Đặt `--upload-delay 0.05` cho seeder để quá trình tải kéo dài vài giây, đủ thời gian thao tác.
