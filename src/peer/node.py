"""PeerNode: một peer độc lập gồm upload server + tracker client + kho chunk.

Luồng nền (maintenance) chạy mỗi 0.5s:
- gửi heartbeat theo chu kỳ; tracker báo "UNKNOWN_PEER" (vd. tracker vừa restart)
  -> tự đăng ký lại và announce lại toàn bộ chunk;
- gom các thay đổi chunk ("dirty") và announce lên tracker.
"""
import os
import threading
import time
from dataclasses import dataclass, field

from src.common.auth import valid_peer_id
from src.common.log import NodeLogger
from src.common.metadata import build_metadata, validate_metadata
from src.peer.downloader import Downloader
from src.peer.storage import FileStore
from src.peer.tracker_client import TrackerClient, TrackerError, TrackerUnavailable
from src.peer.uploader import UploadServer


@dataclass
class Faults:
    """Tham số chèn lỗi phục vụ demo/kiểm thử."""
    corrupt_chunks: set = field(default_factory=set)
    corrupt_all: bool = False
    upload_delay: float = 0.0

    def corrupts(self, index):
        return self.corrupt_all or index in self.corrupt_chunks


class PeerNode:
    def __init__(self, settings, peer_id, port, advertise_host=None, faults=None):
        if not valid_peer_id(peer_id):
            raise ValueError("peer id must match [A-Za-z0-9_-]{1,64}")
        self.settings = settings
        self.peer_id = peer_id
        self.advertise_host = advertise_host
        self.faults = faults or Faults()
        self.root = os.path.join(settings.data_dir, peer_id)
        os.makedirs(self.root, exist_ok=True)
        self.log = NodeLogger(peer_id, settings.log_dir)
        self.tracker = TrackerClient(settings.tracker_url, settings.request_timeout, settings.tracker_retries, self.log)
        self.stores = {}
        self._stores_lock = threading.Lock()
        self._dirty = set()
        self._dirty_lock = threading.Lock()
        self._last_seq = 0
        self._seq_lock = threading.Lock()
        self._verified = {}  # token -> (peer_id, hết hạn cache)
        self._stop = threading.Event()
        self.uploader = UploadServer(self, port)
        self.port = self.uploader.port

    # ---------- vòng đời ----------
    def start(self):
        for store in FileStore.load_all(self.root):
            self.stores[store.file_id] = store
            self.log.info("STORE_RESUMED", file=store.file_id[:12], name=store.meta["name"],
                          have=len(store.have_list()), total=store.meta["num_chunks"])
        self.uploader.start()
        self.log.info("PEER_START", port=self.port, tracker=self.settings.tracker_url, data=self.root)
        self.register()
        threading.Thread(target=self._maintenance_loop, daemon=True, name="maintenance").start()

    def stop(self):
        self._stop.set()
        try:
            self.tracker.leave()
        except TrackerError:
            pass
        self.uploader.stop()
        self.log.info("PEER_STOP")

    def _token_path(self):
        return os.path.join(self.root, "token")

    def register(self):
        # Dùng lại token cũ (nếu có) để được phép đăng ký lại cùng Peer ID ngay sau khi restart
        if self.tracker.token is None and os.path.isfile(self._token_path()):
            with open(self._token_path(), encoding="utf-8") as f:
                self.tracker.token = f.read().strip() or None
        self.tracker.register(self.peer_id, self.port, self.settings.join_key, self.advertise_host)
        with open(self._token_path(), "w", encoding="utf-8") as f:
            f.write(self.tracker.token)
        self.log.info("REGISTERED", tracker=self.settings.tracker_url)
        for fid in list(self.stores):
            self.mark_dirty(fid)

    def _maintenance_loop(self):
        next_hb = time.monotonic() + self.settings.heartbeat_interval
        tracker_down = False
        while not self._stop.wait(0.5):
            try:
                if time.monotonic() >= next_hb:
                    next_hb = time.monotonic() + self.settings.heartbeat_interval
                    try:
                        self.tracker.heartbeat()
                    except TrackerError as e:
                        if isinstance(e, TrackerUnavailable):
                            raise
                        self.log.warn("HEARTBEAT_REJECTED", code=e.code)
                        self.register()
                    if tracker_down:
                        tracker_down = False
                        self.log.info("TRACKER_RECONNECTED")
                self._flush_announces()
            except TrackerUnavailable:
                if not tracker_down:
                    tracker_down = True
                    self.log.warn("TRACKER_DOWN", action="keep_serving_and_retry")
            except TrackerError as e:
                self.log.warn("MAINTENANCE_ERROR", code=e.code)

    # ---------- announce ----------
    def _next_seq(self):
        # seq tăng dần kể cả qua các lần restart (dựa trên thời gian, ms)
        with self._seq_lock:
            self._last_seq = max(self._last_seq + 1, int(time.time() * 1000))
            return self._last_seq

    def mark_dirty(self, file_id):
        with self._dirty_lock:
            self._dirty.add(file_id)

    def _flush_announces(self):
        with self._dirty_lock:
            dirty, self._dirty = self._dirty, set()
        for fid in dirty:
            try:
                self.announce_now(fid)
            except TrackerError:
                self.mark_dirty(fid)
                raise

    def announce_now(self, file_id):
        store = self.stores[file_id]
        try:
            self.tracker.announce(file_id, store.have_list(), self._next_seq())
        except TrackerError as e:
            if e.code != "UNKNOWN_FILE":
                raise
            # Tracker mất metadata (vd. mất file trạng thái) -> đăng ký lại file rồi announce
            self.tracker.add_file(store.meta)
            self.tracker.announce(file_id, store.have_list(), self._next_seq())

    # ---------- kho chunk ----------
    def add_store(self, meta):
        validate_metadata(meta)
        with self._stores_lock:
            store = self.stores.get(meta["file_id"])
            if store is None:
                store = FileStore(self.root, meta)
                self.stores[meta["file_id"]] = store
        return store

    def verify_peer_token(self, token):
        """Hỏi tracker token của peer gọi tới có hợp lệ không (có cache 30s)."""
        if not isinstance(token, str) or len(token) > 512:
            return None
        cached = self._verified.get(token)
        if cached and cached[1] > time.monotonic():
            return cached[0]
        try:
            resp = self.tracker.verify(token)
        except TrackerError as e:
            self.log.warn("TOKEN_VERIFY_FAILED", error=e.code)
            return None
        peer_id = resp.get("peer_id") if resp.get("valid") else None
        if peer_id:
            self._verified[token] = (peer_id, time.monotonic() + 30)
        return peer_id

    # ---------- chức năng chính ----------
    def seed(self, path, chunk_size=None):
        meta = build_metadata(path, chunk_size or self.settings.chunk_size, self.settings.tracker_url)
        store = self.add_store(meta)
        store.import_file(path)
        self.tracker.add_file(meta)
        self.announce_now(meta["file_id"])
        self.log.info("SEEDING", file=meta["file_id"][:12], file_id=meta["file_id"], name=meta["name"],
                      size=meta["size"], chunks=meta["num_chunks"])
        return meta

    def download(self, file_id=None, meta=None, out_dir="downloads"):
        if meta is None:
            meta = validate_metadata(self.tracker.get_file(file_id))
        out_path = os.path.join(out_dir, meta["name"])
        return Downloader(self, meta).run(out_path)
