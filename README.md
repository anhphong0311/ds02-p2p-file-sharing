# DS02 – Hệ thống chia sẻ file P2P mô phỏng BitTorrent

Bài tập lớn môn **Hệ thống phân tán**.

File được chia thành các chunk có hash SHA-256 và được tải **song song từ nhiều peer**. Peer đang tải vẫn **upload** những chunk mình đã có cho peer khác. **Tracker chỉ quản lý metadata và danh sách peer, không bao giờ gửi nội dung file.**

## Tính năng

- Tạo metadata: file_id, tên, kích thước, chunk_size, số chunk, hash từng chunk, hash cả file, địa chỉ tracker
- Seeder đăng ký file lên tracker. Peer announce danh sách chunk đang có
- Tải song song nhiều chunk từ ≥2 peer, chọn chunk theo **rarest-first**, có giới hạn số luồng và số kết nối
- Upload trong khi đang tải
- Kiểm tra hash từng chunk, từ chối chunk hỏng và tự **cấm peer gửi dữ liệu hỏng**
- Ghép chunk theo thứ tự và kiểm tra hash cả file
- Nguồn rời mạng thì tự chuyển sang nguồn khác (timeout, bounded retry, cooldown)
- **Resume** sau khi tắt chương trình. **Tracker restart** thì peer tự đăng ký lại
- Heartbeat, phát hiện peer offline
- Bảo mật: join key, token HMAC, chống mạo danh Peer ID, kiểm tra input, giới hạn kích thước, bí mật đặt trong `.env`

Chi tiết thiết kế: [docs/design.md](docs/design.md). Kiểm thử: [docs/test-plan.md](docs/test-plan.md). Kịch bản demo: [docs/demo-script.md](docs/demo-script.md).

## Kiến trúc

```
              TRACKER (HTTP/JSON)  -- chỉ metadata + danh sách peer
             /        |         \
   register/heartbeat/announce/get-peers
           /          |           \
      PEER A <----> PEER B <----> PEER C      (TCP: HELLO / GET_CHUNK / CHUNK)
     (seeder)       (seeder)     (leecher, vừa tải vừa upload)
```

## Cấu trúc thư mục

```
src/common/    protocol, metadata, auth (token), config, log
src/tracker/   tracker server + trạng thái
src/peer/      storage, uploader, downloader, node, CLI (main.py)
tests/         unit test + test tích hợp nhiều tiến trình
scripts/       tạo file mẫu, demo, chèn lỗi, benchmark
deploy/        Dockerfile, docker-compose.yml
docs/          thiết kế, kế hoạch test, kịch bản demo, report.pdf, slides.pdf
```

## Yêu cầu môi trường

- Python ≥ 3.10. Chương trình **chỉ dùng thư viện chuẩn**
- `pytest` (chỉ để chạy test): `pip install -r requirements.txt`
- Docker (tùy chọn)

## Cài đặt

```bash
git clone <repo-url> && cd <repo>
cp .env.example .env          # Windows: copy .env.example .env  — rồi đổi JOIN_KEY, TRACKER_SECRET
pip install -r requirements.txt
```

## Chạy thủ công (mỗi lệnh một terminal, chạy từ thư mục gốc repo)

```bash
# 0. Tạo file mẫu 30 MB
python scripts/gen_file.py samples/demo.bin --mb 30 --seed 42

# 1. Tracker
python -m src.tracker.server --port 8000

# 2. Hai seeder (cùng một file => cùng file_id)
python -m src.peer.main --peer-id peerA --port 7001 seed samples/demo.bin
python -m src.peer.main --peer-id peerB --port 7002 seed samples/demo.bin

# 3. Leecher: tải theo tên file (hoặc file_id), sau khi tải xong sẽ tiếp tục seed
python -m src.peer.main --peer-id peerC --port 7003 download demo.bin --out downloads/peerC

# Khác
python -m src.peer.main --peer-id viewer list                     # liệt kê file trên tracker
python -m src.peer.main make-meta samples/demo.bin                # chỉ tạo samples/demo.bin.meta.json
python -m src.peer.main --peer-id peerB --port 7002 serve         # seed lại những gì đã có sau khi restart
```

**Chạy trên nhiều máy (LAN/Internet):** đặt `TRACKER_URL=http://<IP-tracker>:8000` trong `.env` của mỗi máy, thêm `--advertise-host <IP-của-máy-đó>` khi chạy peer, và mở cổng 8000 (tracker) cùng cổng upload của peer trên firewall.

**Script demo trên Windows:** `powershell -ExecutionPolicy Bypass -File scripts\demo_local.ps1` mở sẵn 4 cửa sổ. Chèn lỗi bằng `scripts\fault_inject.ps1 kill-peerA | bad-seeder | late-leecher`.

**Tham số chèn lỗi của peer:**

| Tham số | Tác dụng |
|---|---|
| `--upload-delay 0.05` | Giả lập băng thông thấp |
| `--corrupt-chunks all` | Gửi chunk hỏng |
| `--corrupt-chunks 1,5` | Chỉ làm hỏng các chunk được liệt kê |

## Chạy bằng Docker

```bash
docker compose -f deploy/docker-compose.yml --env-file .env up --build
docker compose -f deploy/docker-compose.yml logs -f leecher
docker compose -f deploy/docker-compose.yml stop seeder1                      # demo mất nguồn
docker compose -f deploy/docker-compose.yml --env-file .env --profile extra up leecher2
docker compose -f deploy/docker-compose.yml down -v
```

## Kiểm thử

```bash
python -m pytest                                   # 36 test: chức năng, đồng thời, lỗi/phục hồi, bảo mật
python scripts/benchmark.py --mb 4 --delay 0.03    # thí nghiệm hiệu năng
```

## Log

Mỗi node ghi log ra màn hình và vào `logs/<node>.log`, mỗi dòng có dạng:

```
2026-10-24T23:38:40.120+07:00 node=peerC level=INFO event=CHUNK_OK file=55f0de80dd68 index=17 source=peerA progress=18/48
```

Các sự kiện chính:

| Nơi | Sự kiện |
|---|---|
| Tracker | `PEER_REGISTERED`, `PEER_EXPIRED`, `FILE_REGISTERED`, `ANNOUNCE`, `ANNOUNCE_STALE`, `REGISTER_REJECTED` |
| Peer | `CHUNK_OK`, `CHUNK_SENT`, `CHUNK_FAILED`, `CHUNK_REJECTED`, `PEER_BANNED`, `PEER_BUSY`, `DOWNLOAD_COMPLETE`, `STORE_RESUMED`, `TRACKER_DOWN`, `UPLOAD_AUTH_FAILED` |

## Kịch bản demo bắt buộc

| Yêu cầu của đề | Cách demo |
|---|---|
| Tạo metadata và chia sẻ file | `make-meta`, `seed` |
| Tải song song từ ≥2 peer | 2 seeder + `download`, log `sources=peerA:..,peerB:..` |
| Upload trước khi tải xong | Thêm leecher thứ 2 khi leecher 1 đang tải, log `CHUNK_SENT` |
| Dừng một nguồn khi đang tải | Kill peerA |
| Chuyển sang nguồn khác | Tải vẫn hoàn tất từ peerB |
| Phát hiện chunk hỏng | Seeder `--corrupt-chunks all`, log `CHUNK_REJECTED` |
| Ghép file và kiểm tra hash cuối | `DOWNLOAD_COMPLETE file_hash=verified` |

## Phân công công việc

Nhóm 7 – Đề tài DS02

| Thành viên | MSSV | Vai trò | Phụ trách | File chính | Báo cáo |
|---|---|---|---|---|---|
| Trần Anh Phong | B23DCCE077 | **Trưởng nhóm** | Downloader (tải song song, rarest-first, retry, chuyển nguồn), lưu trữ và resume, PeerNode, giao thức P2P; nâng cao: kết nối TCP lâu dài + pipeline, choking/tit-for-tat; quản lý repo | `src/peer/downloader.py`, `src/peer/storage.py`, `src/peer/node.py`, `src/common/protocol.py` | Chương 6, 7, 10, 11, 15 |
| Lỗ Quốc Khánh | B23DCCE050 | Thành viên | Tracker (API, heartbeat, announce, phát hiện peer offline), xác thực token, tracker client; nâng cao: nhân bản tracker | `src/tracker/`, `src/common/auth.py`, `src/peer/tracker_client.py` | Chương 9, 12 |
| Nguyễn Thanh Sơn | B23DCCE080 | Thành viên | Uploader, metadata, CLI, Docker, script, benchmark; nâng cao: mã hóa TLS | `src/peer/uploader.py`, `src/common/metadata.py`, `src/peer/main.py`, `deploy/`, `scripts/` | Chương 1–5, 8, 13, 14, 16–19; ghép báo cáo, slide |

## Tài liệu tham khảo

- B. Cohen, *The BitTorrent Protocol Specification* (BEP 3), bittorrent.org
- M. van Steen, A. S. Tanenbaum, *Distributed Systems*, 4th ed.
- Tài liệu Python: `socket`, `http.server`, `concurrent.futures`, `hashlib`, `hmac`
