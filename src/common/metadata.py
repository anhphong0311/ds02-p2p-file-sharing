"""Tạo và kiểm tra metadata của file (tương tự file .torrent).

file_id = SHA-256 của phần "info" (tên, kích thước, chunk_size, hash từng chunk, hash cả file)
=> cùng một file và cùng chunk_size luôn cho ra cùng file_id ở mọi peer.
"""
import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone

INFO_KEYS = ("name", "size", "chunk_size", "num_chunks", "chunk_hashes", "file_hash")

MIN_CHUNK_SIZE = 16 * 1024
MAX_CHUNK_SIZE = 4 * 1024 * 1024
MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024
MAX_CHUNKS = 65536

HEX64 = re.compile(r"^[0-9a-f]{64}$")
SAFE_NAME = re.compile(r"^[A-Za-z0-9._()\- ]{1,200}$")


class MetadataError(ValueError):
    pass


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def compute_file_id(meta):
    info = {k: meta[k] for k in INFO_KEYS}
    canonical = json.dumps(info, sort_keys=True, separators=(",", ":"))
    return sha256_hex(canonical.encode("utf-8"))


def _is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def validate_name(name):
    if not isinstance(name, str) or not SAFE_NAME.match(name) or name in (".", ".."):
        raise MetadataError(f"unsafe file name: {name!r}")
    return name


def validate_metadata(meta):
    """Kiểm tra chặt metadata nhận từ mạng. Ném MetadataError nếu sai."""
    if not isinstance(meta, dict):
        raise MetadataError("metadata must be an object")
    for key in ("file_id",) + INFO_KEYS:
        if key not in meta:
            raise MetadataError(f"missing field {key}")
    validate_name(meta["name"])
    size, chunk_size, num = meta["size"], meta["chunk_size"], meta["num_chunks"]
    if not _is_int(size) or not 0 < size <= MAX_FILE_SIZE:
        raise MetadataError("invalid size")
    if not _is_int(chunk_size) or not MIN_CHUNK_SIZE <= chunk_size <= MAX_CHUNK_SIZE:
        raise MetadataError("invalid chunk_size")
    if not _is_int(num) or num != math.ceil(size / chunk_size) or num > MAX_CHUNKS:
        raise MetadataError("num_chunks does not match size/chunk_size")
    hashes = meta["chunk_hashes"]
    if not isinstance(hashes, list) or len(hashes) != num:
        raise MetadataError("chunk_hashes length mismatch")
    if not all(isinstance(h, str) and HEX64.match(h) for h in hashes):
        raise MetadataError("invalid chunk hash")
    if not isinstance(meta["file_hash"], str) or not HEX64.match(meta["file_hash"]):
        raise MetadataError("invalid file_hash")
    if meta["file_id"] != compute_file_id(meta):
        raise MetadataError("file_id does not match metadata content")
    if "tracker" in meta and not isinstance(meta["tracker"], str):
        raise MetadataError("invalid tracker")
    return meta


def build_metadata(path, chunk_size, tracker_url):
    """Đọc file, chia chunk, tính hash -> metadata."""
    if not MIN_CHUNK_SIZE <= chunk_size <= MAX_CHUNK_SIZE:
        raise MetadataError("chunk_size out of range")
    name = validate_name(os.path.basename(path))
    size = os.path.getsize(path)
    hashes = []
    whole = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(chunk_size)
            if not block:
                break
            hashes.append(sha256_hex(block))
            whole.update(block)
    meta = {
        "name": name,
        "size": size,
        "chunk_size": chunk_size,
        "num_chunks": len(hashes),
        "chunk_hashes": hashes,
        "file_hash": whole.hexdigest(),
    }
    meta = {
        "file_id": compute_file_id(meta),
        **meta,
        "tracker": tracker_url,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    return validate_metadata(meta)


def chunk_length(meta, index):
    if index < meta["num_chunks"] - 1:
        return meta["chunk_size"]
    return meta["size"] - meta["chunk_size"] * (meta["num_chunks"] - 1)


def save_metadata(meta, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    os.replace(tmp, path)


def load_metadata(path):
    with open(path, encoding="utf-8") as f:
        return validate_metadata(json.load(f))
