"""Test PeerConnection với một uploader giả (fake server) để điều khiển chính xác thứ tự response."""
import socket
import threading

import pytest

from src.common import protocol as P
from src.peer.connection import PeerConnection, PeerError


class FakeUploader:
    """Server TCP 1 kết nối: bắt tay HELLO rồi chạy hàm `behavior(sock)` do test cung cấp."""

    def __init__(self, behavior):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(5)
        self.port = self.sock.getsockname()[1]
        self.accepted = 0
        self.behavior = behavior
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        conn, _ = self.sock.accept()
        self.accepted += 1
        conn.settimeout(5)
        P.recv_msg(conn, max_payload=0)  # HELLO
        P.send_msg(conn, {"type": P.HELLO_OK, "peer_id": "fake"})
        try:
            self.behavior(conn)
        finally:
            conn.close()
            self.sock.close()


def read_requests(conn, n):
    return [P.recv_msg(conn, max_payload=0)[0] for _ in range(n)]


def chunk_reply(conn, req):
    P.send_msg(conn, {"type": P.CHUNK, "file_id": req["file_id"], "index": req["index"],
                      "request_id": req["request_id"]}, f"chunk-{req['index']}".encode())


def connect(server):
    return PeerConnection("127.0.0.1", server.port, "me", "token", max_payload=1024,
                          connect_timeout=2, request_timeout=5, remote_id="fake")


def test_pipelined_requests_answered_out_of_order():
    """3 request được gửi liên tiếp trên 1 kết nối (server đọc đủ 3 trước khi trả lời),
    server trả lời theo thứ tự NGƯỢC -> mỗi request vẫn nhận đúng chunk nhờ request_id."""
    def behavior(conn):
        reqs = read_requests(conn, 3)
        for req in reversed(reqs):
            chunk_reply(conn, req)
        read_requests(conn, 1)  # BYE

    server = FakeUploader(behavior)
    conn = connect(server)
    futures = {i: conn.request("f", i) for i in (7, 8, 9)}
    assert {i: f.result(timeout=5) for i, f in futures.items()} == {
        7: b"chunk-7", 8: b"chunk-8", 9: b"chunk-9"}
    assert server.accepted == 1
    assert conn.requests_sent == 3
    conn.close()


def test_error_reply_keeps_connection_usable():
    """ERROR NOT_HAVE chỉ làm hỏng request đó; kết nối vẫn dùng tiếp được."""
    def behavior(conn):
        req = read_requests(conn, 1)[0]
        P.send_msg(conn, {"type": P.ERROR, "code": P.E_NOT_HAVE, "request_id": req["request_id"]})
        chunk_reply(conn, read_requests(conn, 1)[0])
        read_requests(conn, 1)

    server = FakeUploader(behavior)
    conn = connect(server)
    with pytest.raises(PeerError) as e:
        conn.request("f", 1).result(timeout=5)
    assert e.value.code == P.E_NOT_HAVE
    assert not conn.closed
    assert conn.request("f", 2).result(timeout=5) == b"chunk-2"
    conn.close()


def test_peer_crash_fails_all_pending_requests():
    """Peer chết khi còn request đang chờ -> tất cả request thất bại ngay, kết nối đóng."""
    server = FakeUploader(lambda conn: read_requests(conn, 2))  # đọc 2 request rồi đóng kết nối
    conn = connect(server)
    futures = [conn.request("f", 1), conn.request("f", 2)]
    for f in futures:
        with pytest.raises((ConnectionError, OSError)):
            f.result(timeout=5)
    assert conn.closed
    with pytest.raises(ConnectionError):
        conn.request("f", 3)


def test_unknown_request_id_is_protocol_error():
    """Response mang request_id lạ (trùng lặp/giả mạo) -> lỗi giao thức, kết nối bị đóng."""
    def behavior(conn):
        read_requests(conn, 1)
        P.send_msg(conn, {"type": P.CHUNK, "index": 1, "request_id": "bogus"}, b"x")

    server = FakeUploader(behavior)
    conn = connect(server)
    with pytest.raises(P.ProtocolError):
        conn.request("f", 1).result(timeout=5)
    assert conn.closed


def test_busy_peer_rejected_at_handshake():
    """Uploader hết slot trả BUSY ngay khi bắt tay."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    port = sock.getsockname()[1]

    def serve():
        c, _ = sock.accept()
        P.recv_msg(c, max_payload=0)  # HELLO
        P.send_msg(c, {"type": P.ERROR, "code": P.E_BUSY})
        c.close()

    threading.Thread(target=serve, daemon=True).start()
    with pytest.raises(PeerError) as e:
        PeerConnection("127.0.0.1", port, "me", "t", 1024, 2, 5)
    assert e.value.code == P.E_BUSY
    sock.close()
