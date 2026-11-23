"""CLI của peer.

Ví dụ:
  python -m src.peer.main --peer-id peerA --port 7001 seed samples/video.bin
  python -m src.peer.main --peer-id peerC --port 7003 download <file_id>
  python -m src.peer.main --peer-id peerB --port 7002 serve          # tiếp tục chia sẻ những gì đã có
  python -m src.peer.main --peer-id peerX --port 7009 list           # xem các file trên tracker
  python -m src.peer.main make-meta samples/video.bin                 # chỉ tạo file metadata

Chèn lỗi (demo): --corrupt-chunks 0,3,5 | --corrupt-chunks all | --upload-delay 0.2
"""
import argparse
import os
import re
import sys
import time

from src.common.config import Settings
from src.common.metadata import build_metadata, load_metadata, save_metadata
from src.peer.downloader import DownloadError
from src.peer.node import Faults, PeerNode
from src.peer.tracker_client import TrackerError


def parse_args(argv):
    p = argparse.ArgumentParser(description="DS02 peer")
    p.add_argument("--peer-id")
    p.add_argument("--port", type=int, default=0, help="cổng upload (0 = tự chọn)")
    p.add_argument("--advertise-host", default=None, help="địa chỉ để peer khác kết nối tới")
    p.add_argument("--tracker", default=None, help="URL tracker (nhiều tracker: ngăn cách bằng dấu phẩy), ghi đè TRACKER_URL")
    p.add_argument("--data-dir", default=None)
    p.add_argument("--chunk-size", type=int, default=None)
    p.add_argument("--workers", type=int, default=None, help="số luồng tải song song")
    p.add_argument("--corrupt-chunks", default="", help="danh sách index hoặc 'all'")
    p.add_argument("--upload-delay", type=float, default=0.0, help="giả lập băng thông: mỗi chunk gửi đi chiếm N giây của cả peer")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("path")
    d = sub.add_parser("download")
    d.add_argument("target", help="file_id, tên file trên tracker, hoặc đường dẫn file .meta.json")
    d.add_argument("--out", default="downloads")
    d.add_argument("--exit-after-download", action="store_true")
    sub.add_parser("serve")
    sub.add_parser("list")
    m = sub.add_parser("make-meta")
    m.add_argument("path")
    m.add_argument("--output", default=None)
    return p.parse_args(argv)


def resolve_target(node, target, wait_seconds=30):
    """Nhận file_id hoặc tên file; tên file được tra trên tracker (chờ tối đa wait_seconds)."""
    if re.fullmatch(r"[0-9a-f]{64}", target):
        return target
    deadline = time.monotonic() + wait_seconds
    while True:
        matches = [f["file_id"] for f in node.tracker.list_files() if f["name"] == target]
        if matches:
            return matches[0]
        if time.monotonic() > deadline:
            raise ValueError(f"no file named {target!r} on tracker")
        time.sleep(1)


def wait_forever(node):
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()


def main(argv=None):
    args = parse_args(argv)
    settings = Settings.from_env()
    if args.tracker:
        settings.tracker_url = args.tracker
    if args.data_dir:
        settings.data_dir = args.data_dir
    if args.chunk_size:
        settings.chunk_size = args.chunk_size
    if args.workers:
        settings.max_download_workers = args.workers

    if args.cmd == "make-meta":
        meta = build_metadata(args.path, settings.chunk_size, settings.tracker_url)
        out = args.output or args.path + ".meta.json"
        save_metadata(meta, out)
        print(f"file_id={meta['file_id']} chunks={meta['num_chunks']} -> {out}")
        return 0

    if not args.peer_id:
        print("--peer-id is required", file=sys.stderr)
        return 2
    faults = Faults(
        corrupt_all=args.corrupt_chunks.strip().lower() == "all",
        corrupt_chunks={int(x) for x in args.corrupt_chunks.split(",") if x.strip().isdigit()},
        upload_delay=args.upload_delay,
    )
    node = PeerNode(settings, args.peer_id, args.port, args.advertise_host, faults)
    try:
        node.start()
    except TrackerError as e:
        node.log.error("REGISTER_FAILED", error=str(e))
        node.uploader.stop()
        return 1

    if args.cmd == "list":
        for f in node.tracker.list_files():
            print(f"{f['file_id']}  {f['name']}  {f['size']}B  chunks={f['num_chunks']}  "
                  f"peers={f['peers']} seeders={f['seeders']}")
        node.stop()
        return 0

    if args.cmd == "seed":
        node.seed(args.path)
        wait_forever(node)
        return 0

    if args.cmd == "download":
        try:
            if os.path.isfile(args.target):
                node.download(meta=load_metadata(args.target), out_dir=args.out)
            else:
                node.download(file_id=resolve_target(node, args.target), out_dir=args.out)
        except (DownloadError, TrackerError, ValueError) as e:
            node.log.error("DOWNLOAD_FAILED", error=str(e))
            node.stop()
            return 1
        except KeyboardInterrupt:
            node.log.warn("DOWNLOAD_INTERRUPTED", hint="run again to resume")
            node.stop()
            return 130
        if args.exit_after_download:
            node.stop()
            return 0
        node.log.info("NOW_SEEDING")
        wait_forever(node)
        return 0

    if args.cmd == "serve":
        wait_forever(node)
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
