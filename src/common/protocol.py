"""Giao thức peer-to-peer trên TCP.

Mỗi frame:  [4 byte độ dài header, big-endian] [header JSON UTF-8] [payload nhị phân]
Header luôn có "type" và "size" (= độ dài payload, 0 nếu không có).

Luồng trao đổi:
    downloader -> uploader : HELLO {peer_id, token}
    uploader   -> downloader: HELLO_OK {peer_id} | ERROR {code}
    downloader -> uploader : GET_CHUNK {file_id, index, request_id}
    uploader   -> downloader: CHUNK {file_id, index, request_id} + <bytes> | ERROR {code}
    downloader -> uploader : GET_BITFIELD {file_id}  -> BITFIELD {chunks}
    downloader -> uploader : BYE
"""
import json
import struct

HELLO = "HELLO"
HELLO_OK = "HELLO_OK"
GET_CHUNK = "GET_CHUNK"
CHUNK = "CHUNK"
GET_BITFIELD = "GET_BITFIELD"
BITFIELD = "BITFIELD"
ERROR = "ERROR"
BYE = "BYE"

# Mã lỗi
E_AUTH = "AUTH_FAILED"
E_BUSY = "BUSY"
E_NOT_HAVE = "NOT_HAVE"
E_BAD_REQUEST = "BAD_REQUEST"

MAX_HEADER = 64 * 1024
_LEN = struct.Struct("!I")


class ProtocolError(Exception):
    pass


def send_msg(sock, header, payload=b""):
    header = dict(header)
    header["size"] = len(payload)
    raw = json.dumps(header, separators=(",", ":")).encode("utf-8")
    if len(raw) > MAX_HEADER:
        raise ProtocolError("header too large")
    sock.sendall(_LEN.pack(len(raw)) + raw + payload)


def _recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        part = sock.recv(min(n - len(buf), 1024 * 1024))
        if not part:
            raise ConnectionError("connection closed by remote")
        buf.extend(part)
    return bytes(buf)


def recv_msg(sock, max_payload):
    """Đọc 1 frame. Kiểm tra giới hạn kích thước TRƯỚC khi đọc dữ liệu."""
    (hlen,) = _LEN.unpack(_recv_exact(sock, _LEN.size))
    if hlen == 0 or hlen > MAX_HEADER:
        raise ProtocolError(f"invalid header length {hlen}")
    try:
        header = json.loads(_recv_exact(sock, hlen).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ProtocolError(f"malformed header: {e}") from None
    if not isinstance(header, dict) or not isinstance(header.get("type"), str):
        raise ProtocolError("header must be an object with a 'type'")
    size = header.get("size", 0)
    if not isinstance(size, int) or isinstance(size, bool) or size < 0 or size > max_payload:
        raise ProtocolError(f"invalid payload size {size!r}")
    payload = _recv_exact(sock, size) if size else b""
    return header, payload
