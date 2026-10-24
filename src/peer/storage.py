"""Lưu chunk trên đĩa của một peer.

Cấu trúc:  <root>/<file_id>/meta.json
           <root>/<file_id>/chunks/<index>.chunk

- Chunk chỉ được ghi khi đúng độ dài và đúng hash -> chunk hỏng bị từ chối.
- Ghi theo kiểu atomic (file tạm rồi os.replace) -> chương trình chết giữa chừng không để lại chunk dở.
- Khi khởi động, quét lại các chunk đã có và kiểm tra hash -> hỗ trợ resume sau khi restart.
"""
import hashlib
import os
import threading

from src.common.metadata import chunk_length, load_metadata, save_metadata, sha256_hex


class IntegrityError(Exception):
    pass


class FileStore:
    def __init__(self, root, meta):
        self.meta = meta
        self.file_id = meta["file_id"]
        self.dir = os.path.join(root, self.file_id)
        self.chunks_dir = os.path.join(self.dir, "chunks")
        os.makedirs(self.chunks_dir, exist_ok=True)
        meta_path = os.path.join(self.dir, "meta.json")
        if not os.path.isfile(meta_path):
            save_metadata(meta, meta_path)
        self._lock = threading.Lock()
        self._have = set()
        self._scan()

    def _path(self, index):
        return os.path.join(self.chunks_dir, f"{index}.chunk")

    def _scan(self):
        for name in os.listdir(self.chunks_dir):
            if name.endswith(".tmp"):
                os.remove(os.path.join(self.chunks_dir, name))
        for i in range(self.meta["num_chunks"]):
            p = self._path(i)
            if not os.path.isfile(p):
                continue
            with open(p, "rb") as f:
                data = f.read()
            if self._valid(i, data):
                self._have.add(i)
            else:
                os.remove(p)

    def _valid(self, index, data):
        return len(data) == chunk_length(self.meta, index) and sha256_hex(data) == self.meta["chunk_hashes"][index]

    # ---------- truy vấn ----------
    def has(self, index):
        with self._lock:
            return index in self._have

    def have_list(self):
        with self._lock:
            return sorted(self._have)

    def missing(self):
        with self._lock:
            return [i for i in range(self.meta["num_chunks"]) if i not in self._have]

    def is_complete(self):
        with self._lock:
            return len(self._have) == self.meta["num_chunks"]

    # ---------- đọc / ghi ----------
    def write_chunk(self, index, data):
        if not 0 <= index < self.meta["num_chunks"]:
            raise IntegrityError(f"chunk index {index} out of range")
        if not self._valid(index, data):
            raise IntegrityError(f"chunk {index} hash mismatch")
        tmp = f"{self._path(index)}.{threading.get_ident()}.tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, self._path(index))
        with self._lock:
            self._have.add(index)

    def read_chunk(self, index):
        if not self.has(index):
            raise KeyError(index)
        with open(self._path(index), "rb") as f:
            return f.read()

    def import_file(self, path):
        """Seeder: cắt file gốc thành các chunk."""
        with open(path, "rb") as f:
            for i in range(self.meta["num_chunks"]):
                data = f.read(self.meta["chunk_size"])
                if not self.has(i):
                    self.write_chunk(i, data)

    def assemble(self, out_path):
        """Ghép các chunk theo đúng thứ tự và kiểm tra hash cả file."""
        if not self.is_complete():
            raise IntegrityError("cannot assemble: missing chunks")
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        whole = hashlib.sha256()
        tmp = out_path + ".part"
        with open(tmp, "wb") as out:
            for i in range(self.meta["num_chunks"]):
                data = self.read_chunk(i)
                whole.update(data)
                out.write(data)
        if whole.hexdigest() != self.meta["file_hash"]:
            os.remove(tmp)
            raise IntegrityError("final file hash mismatch")
        os.replace(tmp, out_path)
        return out_path

    @staticmethod
    def load_all(root):
        """Nạp lại mọi file đã có trong thư mục dữ liệu (dùng khi peer khởi động lại)."""
        stores = []
        if not os.path.isdir(root):
            return stores
        for name in os.listdir(root):
            meta_path = os.path.join(root, name, "meta.json")
            if os.path.isfile(meta_path):
                try:
                    stores.append(FileStore(root, load_metadata(meta_path)))
                except (ValueError, OSError):
                    continue
        return stores
