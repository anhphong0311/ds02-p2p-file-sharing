"""Benchmark kết nối: thời gian tải và số kết nối TCP mà leecher phải mở.

Dùng để so sánh trước/sau khi bật kết nối lâu dài + pipeline.

python scripts/bench_pipeline.py --mb 16 --chunk-kb 32 --seeders 2 --workers 4 --repeat 3
"""
import argparse
import os
import re
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.common.metadata import build_metadata  # noqa: E402
from tests.cluster import Cluster  # noqa: E402

# Mỗi dòng log này tương ứng với một lần gửi GET_CHUNK
REQUEST_EVENTS = r"event=(CHUNK_OK|CHUNK_FAILED|CHUNK_REJECTED|PEER_BUSY) "


def run_once(size_mb, chunk_kb, n_seeders, workers):
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "bench.bin")
        with open(path, "wb") as f:
            f.write(os.urandom(int(size_mb * 1024 * 1024)))
        fid = build_metadata(path, chunk_kb * 1024, "")["file_id"]
        c = Cluster(tmp, CHUNK_SIZE=chunk_kb * 1024, MAX_DOWNLOAD_WORKERS=workers)
        try:
            c.start_tracker()
            for i in range(n_seeders):
                c.start_peer(f"seed{i}", "seed", path)
            for i in range(n_seeders):
                c.wait_for(f"seed{i}", "event=SEEDING")
            c.start_peer("leech", "download", fid, "--out", os.path.join(tmp, "dl"), "--exit-after-download")
            c.wait_for("leech", "event=DOWNLOAD_COMPLETE", timeout=600)
            log = c.log("leech")
            seconds = float(re.search(r"DOWNLOAD_COMPLETE .*seconds=([\d.]+)", log).group(1))
            requests = len(re.findall(REQUEST_EVENTS, log))
            # Bản có kết nối lâu dài ghi CONN_OPEN; bản cũ mở 1 kết nối cho mỗi request
            conns = log.count("event=CONN_OPEN") or requests
            return seconds, conns, requests
        finally:
            c.stop_all()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mb", type=float, default=16)
    p.add_argument("--chunk-kb", type=int, default=32)
    p.add_argument("--seeders", type=int, default=2)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--repeat", type=int, default=3)
    args = p.parse_args()

    times, conns, reqs = [], [], []
    for _ in range(args.repeat):
        t, c, r = run_once(args.mb, args.chunk_kb, args.seeders, args.workers)
        times.append(t)
        conns.append(c)
        reqs.append(r)
    t = statistics.median(times)
    print(f"file={args.mb}MB chunk={args.chunk_kb}KB seeders={args.seeders} workers={args.workers} "
          f"repeat={args.repeat}")
    print(f"median_seconds={t:.2f} throughput_MBps={args.mb / t:.2f} "
          f"tcp_connections={statistics.median(conns):.0f} chunk_requests={statistics.median(reqs):.0f} "
          f"all_seconds={','.join(f'{x:.2f}' for x in times)}")


if __name__ == "__main__":
    main()
