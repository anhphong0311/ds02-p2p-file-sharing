"""Tải file song song từ nhiều peer.

Thuật toán:
1. Hỏi tracker danh sách peer đang giữ chunk của file (làm mới định kỳ).
2. Chọn chunk theo "rarest-first": chunk ít peer giữ nhất được tải trước.
   Với mỗi chunk chọn peer đang bận ít nhất (cân bằng tải giữa các nguồn).
3. Tối đa `max_download_workers` chunk được tải song song.
4. Nhận chunk -> kiểm tra hash. Sai hash -> từ chối, tải lại từ peer khác,
   peer gửi sai quá `max_peer_strikes` lần bị cấm.
5. Lỗi kết nối/timeout -> peer bị tạm bỏ qua (`peer_cooldown`), chunk được thử lại ở peer khác.
   Mỗi chunk được thử tối đa `max_chunk_retries` lần (bounded retry).
6. Mỗi chunk tải xong được announce lên tracker -> peer khác tải được ngay từ mình.
7. Đủ chunk -> ghép theo thứ tự và kiểm tra hash cả file.

Mỗi peer nguồn chỉ dùng 1 kết nối TCP lâu dài (PeerConnection); các luồng tải
gửi request song song trên cùng kết nối đó (pipeline, tối đa `max_requests_per_peer`).
"""
import random
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from src.common import protocol as P
from src.peer.connection import PeerConnection, PeerError
from src.peer.storage import IntegrityError
from src.peer.tracker_client import TrackerError


class DownloadError(Exception):
    pass


class Downloader:
    def __init__(self, node, meta):
        self.node = node
        self.meta = meta
        self.file_id = meta["file_id"]
        self.short = self.file_id[:12]
        self.store = node.add_store(meta)
        self.s = node.settings
        self.log = node.log
        self._conns = {}                 # peer_id -> PeerConnection
        self._conn_locks = {}            # peer_id -> Lock (tránh 2 luồng cùng mở kết nối tới 1 peer)
        self._conns_lock = threading.Lock()
        self.conns_opened = 0

    # ---------- kết nối lâu dài ----------
    def _connection(self, peer):
        pid = peer["peer_id"]
        with self._conns_lock:
            lock = self._conn_locks.setdefault(pid, threading.Lock())
        with lock:
            conn = self._conns.get(pid)
            if conn is None or conn.closed:
                conn = PeerConnection(peer["host"], peer["port"], self.node.peer_id, self.node.tracker.token,
                                      self.meta["chunk_size"], self.s.connect_timeout, self.s.request_timeout,
                                      remote_id=pid, logger=self.log)
                self._conns[pid] = conn
                self.conns_opened += 1
            return conn

    def _close_connections(self):
        for conn in list(self._conns.values()):
            conn.close()

    # ---------- một request lấy chunk ----------
    def _fetch(self, peer, index):
        conn = self._connection(peer)
        fut = conn.request(self.file_id, index)
        try:
            return fut.result(timeout=self.s.request_timeout)
        except TimeoutError:
            conn.close()  # peer không trả lời: bỏ kết nối, các request khác trên đó cũng thất bại
            raise

    def _fetch_peers(self, known):
        try:
            peers = self.node.tracker.get_peers(self.file_id)
        except TrackerError as e:
            self.log.warn("PEER_LIST_FAILED", file=self.short, error=e.code)
            return known
        return {p["peer_id"]: {**p, "chunks": set(p["chunks"])} for p in peers}

    def _pick(self, pending, busy_chunks, peers, failed_on, cooldown, banned, active, now):
        """Chọn (chunk, peer) tiếp theo theo rarest-first."""
        usable = {
            pid: p for pid, p in peers.items()
            if pid not in banned and cooldown.get(pid, 0) <= now and active[pid] < self.s.max_requests_per_peer
        }
        if not usable:
            return None
        best = None
        candidates = [i for i in pending if i not in busy_chunks]
        random.shuffle(candidates)
        for i in candidates:
            holders = [pid for pid, p in usable.items() if i in p["chunks"] and pid not in failed_on[i]]
            if not holders:
                continue
            rarity = sum(1 for p in peers.values() if i in p["chunks"])
            pid = min(holders, key=lambda h: (active[h], random.random()))
            key = (rarity, active[pid])
            if best is None or key < best[0]:
                best = (key, i, pid)
        return (best[1], best[2]) if best else None

    # ---------- vòng lặp chính ----------
    def run(self, out_path):
        try:
            started, sources = self._download_chunks()
        finally:
            self._close_connections()
        path = self.store.assemble(out_path)
        elapsed = time.monotonic() - started
        self.log.info("DOWNLOAD_COMPLETE", file=self.short, out=path, seconds=f"{elapsed:.2f}",
                      sources=",".join(f"{p}:{n}" for p, n in sorted(sources.items())) or "-",
                      connections=self.conns_opened, file_hash="verified")
        self.node.announce_now(self.file_id)
        return path

    def _download_chunks(self):
        s, store = self.s, self.store
        pending = set(store.missing())
        self.log.info("DOWNLOAD_START", file=self.short, name=self.meta["name"], size=self.meta["size"],
                      chunks=self.meta["num_chunks"], already_have=self.meta["num_chunks"] - len(pending))
        self.node.mark_dirty(self.file_id)
        started = time.monotonic()

        attempts = Counter()            # chunk -> số lần thất bại
        failed_on = defaultdict(set)    # chunk -> các peer đã thất bại với chunk này
        strikes = Counter()             # peer -> số lần gửi chunk hỏng
        banned = set()
        cooldown = {}                   # peer -> thời điểm được dùng lại
        active = Counter()              # peer -> số request đang chạy
        sources = Counter()             # peer -> số chunk đã tải từ peer đó
        in_flight = {}                  # future -> (chunk, peer)
        peers, last_refresh, stall_since = {}, 0.0, None

        with ThreadPoolExecutor(max_workers=s.max_download_workers, thread_name_prefix="dl") as pool:
            while pending or in_flight:
                now = time.monotonic()
                if now - last_refresh >= s.peer_refresh_interval:
                    peers = self._fetch_peers(peers)
                    last_refresh = now

                busy_chunks = {c for c, _ in in_flight.values()}
                while len(in_flight) < s.max_download_workers:
                    choice = self._pick(pending, busy_chunks, peers, failed_on, cooldown, banned, active, now)
                    if not choice:
                        break
                    idx, pid = choice
                    fut = pool.submit(self._fetch, peers[pid], idx)
                    in_flight[fut] = (idx, pid)
                    busy_chunks.add(idx)
                    active[pid] += 1

                if not in_flight:
                    # Không có nguồn nào dùng được: chờ peer mới / peer hết cooldown
                    if stall_since is None:
                        stall_since = now
                        self.log.warn("DOWNLOAD_STALLED", file=self.short, missing=len(pending),
                                      known_peers=len(peers), banned=len(banned))
                    if now - stall_since > s.stall_timeout:
                        raise DownloadError(f"no source for {len(pending)} chunks after {s.stall_timeout}s")
                    failed_on.clear()
                    time.sleep(0.5)
                    last_refresh = 0.0
                    continue
                stall_since = None

                done, _ = wait(list(in_flight), timeout=0.5, return_when=FIRST_COMPLETED)
                for fut in done:
                    idx, pid = in_flight.pop(fut)
                    active[pid] -= 1
                    t = time.monotonic()
                    try:
                        store.write_chunk(idx, fut.result())
                    except IntegrityError:
                        attempts[idx] += 1
                        failed_on[idx].add(pid)
                        strikes[pid] += 1
                        self.log.warn("CHUNK_REJECTED", file=self.short, index=idx, source=pid, reason="hash_mismatch",
                                      attempt=attempts[idx])
                        if strikes[pid] >= s.max_peer_strikes and pid not in banned:
                            banned.add(pid)
                            self.log.warn("PEER_BANNED", peer=pid, reason="too_many_bad_chunks")
                            if pid in self._conns:
                                self._conns[pid].close()
                    except PeerError as e:
                        if e.code == P.E_BUSY:
                            cooldown[pid] = t + 0.3
                            self.log.info("PEER_BUSY", peer=pid, index=idx)
                            continue
                        failed_on[idx].add(pid)
                        if e.code != P.E_NOT_HAVE:
                            attempts[idx] += 1
                        last_refresh = 0.0
                        self.log.warn("CHUNK_FAILED", file=self.short, index=idx, source=pid, error=e.code)
                    except (P.ProtocolError, OSError) as e:
                        attempts[idx] += 1
                        failed_on[idx].add(pid)
                        cooldown[pid] = t + s.peer_cooldown
                        self.log.warn("CHUNK_FAILED", file=self.short, index=idx, source=pid,
                                      error=type(e).__name__, attempt=attempts[idx])
                    else:
                        pending.discard(idx)
                        sources[pid] += 1
                        self.node.mark_dirty(self.file_id)
                        self.log.info("CHUNK_OK", file=self.short, index=idx, source=pid,
                                      progress=f"{self.meta['num_chunks'] - len(pending)}/{self.meta['num_chunks']}")
                        continue
                    if attempts[idx] > s.max_chunk_retries:
                        raise DownloadError(f"chunk {idx} failed {attempts[idx]} times")
        return started, sources
