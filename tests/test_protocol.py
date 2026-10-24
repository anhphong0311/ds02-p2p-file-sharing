import json
import socket
import struct

import pytest

from src.common import protocol as P


@pytest.fixture
def pair():
    a, b = socket.socketpair()
    a.settimeout(2)
    b.settimeout(2)
    yield a, b
    a.close()
    b.close()


def test_frame_roundtrip(pair):
    """[F3] Gửi/nhận frame header JSON + payload nhị phân."""
    a, b = pair
    P.send_msg(a, {"type": P.CHUNK, "index": 3}, b"\x00\x01abc")
    header, payload = P.recv_msg(b, max_payload=1024)
    assert header == {"type": P.CHUNK, "index": 3, "size": 5}
    assert payload == b"\x00\x01abc"


def test_oversized_payload_is_rejected(pair):
    """[S3] Payload vượt giới hạn bị từ chối trước khi đọc dữ liệu."""
    a, b = pair
    P.send_msg(a, {"type": P.CHUNK}, b"x" * 2000)
    with pytest.raises(P.ProtocolError):
        P.recv_msg(b, max_payload=1000)


def test_malformed_header_is_rejected(pair):
    """[S4] Header không phải JSON hợp lệ hoặc thiếu 'type' bị từ chối."""
    a, b = pair
    a.sendall(struct.pack("!I", 5) + b"{bad}")
    with pytest.raises(P.ProtocolError):
        P.recv_msg(b, max_payload=10)
    raw = json.dumps({"size": 0}).encode()
    a.sendall(struct.pack("!I", len(raw)) + raw)
    with pytest.raises(P.ProtocolError):
        P.recv_msg(b, max_payload=10)


def test_huge_header_length_is_rejected(pair):
    a, b = pair
    a.sendall(struct.pack("!I", 10 * 1024 * 1024))
    with pytest.raises(P.ProtocolError):
        P.recv_msg(b, max_payload=10)
