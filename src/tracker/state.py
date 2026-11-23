"""Trạng thái của tracker: peer đang online, metadata các file, peer nào giữ chunk nào.

Tracker CHỈ lưu metadata và danh sách peer, không bao giờ lưu nội dung file.
Mọi thao tác được bảo vệ bởi một khóa (lock) vì HTTP server xử lý request đa luồng.

- Metadata file được ghi xuống đĩa (checkpoint) để còn nguyên sau khi tracker khởi động lại.
- Danh sách peer/chunk là "soft state": mất khi restart, peer sẽ tự đăng ký và announce lại.
- Mỗi announce có số thứ tự `seq`; announce có seq <= seq đã nhận bị bỏ qua
  (chống message đến trễ/sai thứ tự hoặc bị gửi trùng).
"""
import json
import os
import threading
import time

from src.common.metadata import validate_metadata


class StateError(Exception):
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


class TrackerState:
    def __init__(self, peer_timeout, state_file=None, logger=None):
        self.peer_timeout = peer_timeout
        self.state_file = state_file
        self.log = logger
        self._lock = threading.RLock()
        self.peers = {}   # peer_id -> {host, port, last_seen, joined_at}
        self.files = {}   # file_id -> metadata
        self.swarms = {}  # file_id -> {peer_id: {"chunks": set[int], "seq": int}}
        self._load()

    # ---------- lưu / nạp metadata ----------
    def _load(self):
        if not self.state_file or not os.path.isfile(self.state_file):
            return
        with open(self.state_file, encoding="utf-8") as f:
            data = json.load(f)
        for meta in data.get("files", []):
            try:
                validate_metadata(meta)
            except ValueError:
                continue
            self.files[meta["file_id"]] = meta
            self.swarms.setdefault(meta["file_id"], {})
        if self.log:
            self.log.info("STATE_RESTORED", files=len(self.files))

    def _persist(self):
        if not self.state_file:
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.state_file)), exist_ok=True)
        tmp = self.state_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"files": list(self.files.values())}, f)
        os.replace(tmp, self.state_file)

    # ---------- peer ----------
    def _alive(self, info, now):
        return now - info["last_seen"] <= self.peer_timeout

    def register_peer(self, peer_id, host, port, auth_peer_id=None):
        """Đăng ký peer. Nếu Peer ID đang được một peer còn sống dùng thì chỉ
        cho phép khi request mang token hợp lệ của chính Peer ID đó (chống mạo danh)."""
        now = time.time()
        with self._lock:
            existing = self.peers.get(peer_id)
            if existing and self._alive(existing, now) and auth_peer_id != peer_id:
                raise StateError(409, "PEER_ID_IN_USE", "peer id is used by another online peer")
            rejoin = existing is not None
            self.peers[peer_id] = {
                "host": host,
                "port": port,
                "last_seen": now,
                "joined_at": existing["joined_at"] if existing else now,
            }
        return rejoin

    def heartbeat(self, peer_id):
        with self._lock:
            info = self.peers.get(peer_id)
            if not info:
                raise StateError(404, "UNKNOWN_PEER", "peer not registered, register again")
            info["last_seen"] = time.time()

    def remove_peer(self, peer_id):
        with self._lock:
            existed = self.peers.pop(peer_id, None) is not None
            for swarm in self.swarms.values():
                swarm.pop(peer_id, None)
        return existed

    def expire_peers(self):
        """Xóa peer quá hạn heartbeat. Trả về danh sách peer bị xóa."""
        now = time.time()
        with self._lock:
            dead = [pid for pid, info in self.peers.items() if not self._alive(info, now)]
            for pid in dead:
                self.remove_peer(pid)
        return dead

    # ---------- file ----------
    def add_file(self, meta):
        validate_metadata(meta)
        with self._lock:
            fid = meta["file_id"]
            created = fid not in self.files
            if created:
                self.files[fid] = meta
                self.swarms[fid] = {}
                self._persist()
        return created

    def get_file(self, file_id):
        with self._lock:
            meta = self.files.get(file_id)
        if meta is None:
            raise StateError(404, "UNKNOWN_FILE", "file not found")
        return meta

    def list_files(self):
        with self._lock:
            return [
                {
                    "file_id": fid,
                    "name": m["name"],
                    "size": m["size"],
                    "num_chunks": m["num_chunks"],
                    "peers": len(self.swarms.get(fid, {})),
                    "seeders": sum(
                        1 for h in self.swarms.get(fid, {}).values() if len(h["chunks"]) == m["num_chunks"]
                    ),
                }
                for fid, m in self.files.items()
            ]

    # ---------- announce ----------
    def announce(self, peer_id, file_id, chunks, seq):
        """Peer báo danh sách chunk đang có. Trả về True nếu được chấp nhận, False nếu cũ (stale)."""
        with self._lock:
            if peer_id not in self.peers:
                raise StateError(404, "UNKNOWN_PEER", "peer not registered, register again")
            meta = self.files.get(file_id)
            if meta is None:
                raise StateError(404, "UNKNOWN_FILE", "file not found")
            n = meta["num_chunks"]
            if not isinstance(chunks, list) or len(chunks) > n:
                raise StateError(400, "BAD_CHUNKS", "chunks must be a list of indices")
            if not all(isinstance(i, int) and not isinstance(i, bool) and 0 <= i < n for i in chunks):
                raise StateError(400, "BAD_CHUNKS", "chunk index out of range")
            swarm = self.swarms.setdefault(file_id, {})
            entry = swarm.get(peer_id)
            if entry and seq <= entry["seq"]:
                return False
            swarm[peer_id] = {"chunks": set(chunks), "seq": seq}
            self.peers[peer_id]["last_seen"] = time.time()
        return True

    def peers_for(self, file_id, exclude=None):
        now = time.time()
        with self._lock:
            if file_id not in self.files:
                raise StateError(404, "UNKNOWN_FILE", "file not found")
            result = []
            for pid, holding in self.swarms[file_id].items():
                info = self.peers.get(pid)
                if pid == exclude or not info or not self._alive(info, now) or not holding["chunks"]:
                    continue
                result.append(
                    {"peer_id": pid, "host": info["host"], "port": info["port"], "chunks": sorted(holding["chunks"])}
                )
        return result

    def stats(self):
        with self._lock:
            return {"peers": len(self.peers), "files": len(self.files)}
