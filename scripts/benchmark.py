"""Thí nghiệm hiệu năng: thời gian tải theo số seeder và số luồng tải song song.

Mỗi seeder bị giới hạn băng thông giả lập bằng --upload-delay (giây/chunk),
để thấy rõ lợi ích của việc tải song song từ nhiều nguồn.

python scripts/benchmark.py --mb 8 --delay 0.02 --seeders 1 2 3 --workers 1 4 8
"""
import argparse
import os
import re
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.common.metadata import build_metadata  # noqa: E402
from tests.cluster import Cluster  # noqa: E402

CHUNK = 64 * 1024


def run_once(n_seeders, workers, size_mb, delay):
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "bench.bin")
        with open(path, "wb") as f:
            f.write(os.urandom(int(size_mb * 1024 * 1024)))
        fid = build_metadata(path, CHUNK, "")["file_id"]
        c = Cluster(tmp, MAX_DOWNLOAD_WORKERS=workers, MAX_REQUESTS_PER_PEER=workers)
        try:
            c.start_tracker()
            for i in range(n_seeders):
                c.start_peer(f"seed{i}", "seed", path, extra=["--upload-delay", str(delay)])
            for i in range(n_seeders):
                c.wait_for(f"seed{i}", "event=SEEDING")
            t0 = time.monotonic()
            c.start_peer("leech", "download", fid, "--out", os.path.join(tmp, "dl"), "--exit-after-download")
            c.wait_for("leech", "event=DOWNLOAD_COMPLETE", timeout=600)
            wall = time.monotonic() - t0
            inner = float(re.search(r"DOWNLOAD_COMPLETE .*seconds=([\d.]+)", c.log("leech")).group(1))
            return wall, inner
        finally:
            c.stop_all()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mb", type=float, default=8)
    p.add_argument("--delay", type=float, default=0.02, help="giây/chunk mỗi seeder (giả lập băng thông)")
    p.add_argument("--seeders", type=int, nargs="+", default=[1, 2, 3])
    p.add_argument("--workers", type=int, nargs="+", default=[1, 4, 8])
    args = p.parse_args()

    print(f"File {args.mb} MB, chunk {CHUNK // 1024} KB, upload delay {args.delay}s/chunk\n")
    print("| seeders | workers | download time (s) | throughput (MB/s) |")
    print("|---|---|---|---|")
    for s in args.seeders:
        for w in args.workers:
            _, inner = run_once(s, w, args.mb, args.delay)
            print(f"| {s} | {w} | {inner:.2f} | {args.mb / inner:.2f} |", flush=True)


if __name__ == "__main__":
    main()
