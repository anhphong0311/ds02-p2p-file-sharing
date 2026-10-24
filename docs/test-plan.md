# Kế hoạch kiểm thử và kết quả

Chạy toàn bộ: `python -m pytest`. Có 31 test, trong đó test end-to-end chạy tracker và từng peer thành **tiến trình riêng** giao tiếp qua TCP/HTTP.

Đề yêu cầu tối thiểu: 8 test chức năng, 2 test đồng thời, 2 test lỗi và phục hồi, 2 test bảo mật, 1 thí nghiệm hiệu năng.

## Chức năng (8)

| Mã | Test | Nội dung |
|---|---|---|
| F1 | `test_metadata.py::test_metadata_has_chunk_hashes_and_file_hash` | Metadata có đủ trường, hash từng chunk và hash cả file |
| F2 | `test_metadata.py::test_file_id_is_deterministic` | Cùng file thì cùng file_id, nên nhiều seeder độc lập dùng chung được |
| F3 | `test_protocol.py::test_frame_roundtrip` | Đóng và mở frame header + payload |
| F4 | `test_storage.py::test_corrupt_chunk_is_rejected` | Chunk sai hash bị từ chối |
| F5 | `test_storage.py::test_assemble_out_of_order_and_verify` | Ghép đúng thứ tự, kiểm tra hash cuối |
| F6 | `test_tracker_state.py::test_announce_and_peer_list` | Announce và lấy danh sách peer |
| F7 | `test_e2e.py::test_parallel_download_from_two_seeders` | **Tải song song từ 2 seeder**, file tải về giống hệt file gốc, tracker không gửi nội dung |
| F8 | `test_e2e.py::test_upload_while_downloading` | **Peer upload khi chưa tải xong** |

## Đồng thời / xung đột (3)

| Mã | Test | Nội dung |
|---|---|---|
| C1 | `test_storage.py::test_concurrent_chunk_writes` | Nhiều luồng ghi chunk cùng lúc, trạng thái cuối vẫn nhất quán |
| C2 | `test_tracker_state.py::test_stale_announce_is_ignored` | Announce đến trễ hoặc trùng không ghi đè trạng thái mới |
| C3 | `test_tracker_state.py::test_concurrent_announces` | 20 peer × 50 announce đồng thời, trạng thái cuối bằng announce có seq lớn nhất |

## Lỗi và phục hồi (6)

| Mã | Test | Nội dung |
|---|---|---|
| R1 | `test_storage.py::test_resume_after_restart` | Quét lại chunk khi restart, loại bỏ chunk hỏng và file tạm |
| R2 | `test_tracker_state.py::test_dead_peer_expires` | Peer mất heartbeat bị xóa khỏi tracker |
| R3 | `test_e2e.py::test_corrupt_source_is_detected_and_avoided` | Seeder gửi chunk hỏng: chunk bị từ chối, seeder bị cấm, tải lại từ nguồn tốt |
| R4 | `test_e2e.py::test_source_killed_during_download` | **Kill một nguồn giữa chừng**, chuyển sang nguồn còn lại |
| R5 | `test_e2e.py::test_resume_after_restart` | Kill downloader rồi chạy lại, tải tiếp từ các chunk đã có |
| R6 | `test_e2e.py::test_tracker_restart_recovers` | Tracker restart: metadata được khôi phục, peer tự đăng ký lại |

## Bảo mật / dữ liệu không hợp lệ (7)

| Mã | Test | Nội dung |
|---|---|---|
| S1 | `test_metadata.py::test_tampered_metadata_is_rejected` | Metadata bị sửa |
| S2 | `test_metadata.py::test_unsafe_file_name_is_rejected` | Path traversal `../evil.bin` |
| S3 | `test_protocol.py::test_oversized_payload_is_rejected` | Vượt giới hạn kích thước |
| S4 | `test_protocol.py::test_malformed_header_is_rejected` | Header sai định dạng |
| S5 | `test_tracker_state.py::test_peer_id_impersonation_rejected` | Mạo danh Peer ID |
| S6 | `test_tracker_state.py::test_forged_or_expired_token_rejected` | Token giả hoặc hết hạn |
| S7 | `test_e2e.py::test_bad_join_key_and_unauthenticated_peer` | Sai join key bị trả 403; peer không có token bị uploader từ chối |

## Hiệu năng

`python scripts/benchmark.py --mb 4 --delay 0.03 --seeders 1 2 3 --workers 1 4 8`

Mỗi seeder bị giới hạn băng thông giả lập ở mức 1 chunk 64 KB mỗi 0,03 giây (khoảng 2 MB/s). Đo trên 1 máy Windows, Python 3.13:

| seeders | workers | download time (s) | throughput (MB/s) |
|---|---|---|---|
| 1 | 1 | 3.25 | 1.23 |
| 1 | 4 | 1.99 | 2.01 |
| 1 | 8 | 2.00 | 2.00 |
| 2 | 1 | 3.31 | 1.21 |
| 2 | 4 | 1.02 | 3.92 |
| 2 | 8 | 1.01 | 3.96 |
| 3 | 1 | 3.20 | 1.25 |
| 3 | 4 | 0.92 | 4.35 |
| 3 | 8 | 0.70 | 5.71 |

**Nhận xét**
- Với 1 luồng tải, thời gian bị chặn bởi độ trễ của từng request, nên thêm seeder không giúp gì.
- Với nhiều luồng, throughput tăng gần tuyến tính theo số seeder (2,0 → 3,9 → 5,7 MB/s). Đây là lợi ích chính của tải song song nhiều nguồn.
- Với 1 seeder, tăng số luồng quá 4 không còn tác dụng vì băng thông của seeder đã bão hòa.

Gợi ý mở rộng khi viết báo cáo: chạy trên nhiều máy thật trong LAN, đo CPU/RAM bằng Task Manager hoặc `docker stats`, và đo thời gian phát hiện peer chết (khoảng `PEER_TIMEOUT` + 1s).
