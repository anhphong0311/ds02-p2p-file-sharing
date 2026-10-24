"""Kết nối TCP lâu dài tới một peer, hỗ trợ pipeline.

Trước đây mỗi chunk mở 1 kết nối mới (TCP handshake + HELLO + xác thực token).
PeerConnection giữ 1 kết nối cho mỗi peer và cho phép gửi nhiều GET_CHUNK liên tiếp
mà không cần chờ response của request trước (pipeline):

    luồng tải 1 --GET_CHUNK(rid=a)-->|
    luồng tải 2 --GET_CHUNK(rid=b)-->|  socket  --> uploader
    luồng tải 3 --GET_CHUNK(rid=c)-->|
                                     |
    reader thread <--CHUNK(rid=b)----|   response được ghép với request theo request_id,
                  <--CHUNK(rid=a)----|   nên đúng kể cả khi response về khác thứ tự.

Khi kết nối hỏng (peer chết, timeout, sai giao thức), MỌI request đang chờ trên kết nối
đều thất bại ngay để downloader chuyển sang peer khác.
"""
import socket
import threading
import uuid
from concurrent.futures import Future

from src.common import protocol as P


class PeerError(Exception):
    """Peer trả về ERROR {code} (vd. BUSY, NOT_HAVE, AUTH_FAILED)."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


class PeerConnection:
    def __init__(self, host, port, my_peer_id, token, max_payload, connect_timeout, request_timeout,
                 remote_id="?", logger=None):
        self.remote_id = remote_id
        self.max_payload = max_payload
        self.log = logger
        self._lock = threading.Lock()
        self._pending = {}  # request_id -> (Future, chunk index)
        self.closed = False
        self.requests_sent = 0

        self._sock = socket.create_connection((host, port), timeout=connect_timeout)
        try:
            self._sock.settimeout(request_timeout)
            P.send_msg(self._sock, {"type": P.HELLO, "peer_id": my_peer_id, "token": token})
            resp, _ = P.recv_msg(self._sock, max_payload=0)
            if resp["type"] == P.ERROR:
                raise PeerError(resp.get("code", "ERROR"))
            if resp["type"] != P.HELLO_OK:
                raise P.ProtocolError(f"expected HELLO_OK, got {resp['type']}")
        except BaseException:
            self._sock.close()
            raise
        self._reader = threading.Thread(target=self._read_loop, daemon=True, name=f"conn-{remote_id}")
        self._reader.start()
        if self.log:
            self.log.info("CONN_OPEN", peer=remote_id, addr=f"{host}:{port}")

    # ---------- gửi ----------
    def request(self, file_id, index):
        """Gửi GET_CHUNK, trả về Future sẽ nhận bytes của chunk (hoặc exception)."""
        fut = Future()
        rid = uuid.uuid4().hex[:12]
        with self._lock:
            if self.closed:
                raise ConnectionError("connection closed")
            self._pending[rid] = (fut, index)
            try:
                P.send_msg(self._sock, {"type": P.GET_CHUNK, "file_id": file_id, "index": index, "request_id": rid})
                self.requests_sent += 1
            except OSError as e:
                self._pending.pop(rid, None)
                err = e
            else:
                return fut
        self._fail(err)
        raise err

    # ---------- nhận ----------
    def _read_loop(self):
        while True:
            try:
                header, payload = P.recv_msg(self._sock, max_payload=self.max_payload)
            except socket.timeout:
                with self._lock:
                    idle = not self._pending
                if idle and not self.closed:
                    continue  # không có request nào đang chờ: im lặng là bình thường
                self._fail(TimeoutError(f"no response from {self.remote_id}"))
                return
            except (P.ProtocolError, OSError) as e:
                self._fail(e)
                return

            rid = header.get("request_id")
            with self._lock:
                entry = self._pending.pop(rid, None)
            if entry is None:
                self._fail(P.ProtocolError(f"response for unknown request_id {rid!r}"))
                return
            fut, index = entry
            if header["type"] == P.ERROR:
                fut.set_exception(PeerError(header.get("code", "ERROR")))
            elif header["type"] == P.CHUNK and header.get("index") == index:
                fut.set_result(payload)
            else:
                fut.set_exception(P.ProtocolError("unexpected response"))

    # ---------- đóng ----------
    def _fail(self, exc):
        with self._lock:
            if self.closed:
                return
            self.closed = True
            pending, self._pending = self._pending, {}
        for fut, _ in pending.values():
            if not fut.done():
                fut.set_exception(exc)
        try:
            self._sock.close()
        except OSError:
            pass
        if self.log:
            self.log.info("CONN_CLOSED", peer=self.remote_id, requests=self.requests_sent,
                          reason=type(exc).__name__, failed_pending=len(pending))

    def close(self):
        with self._lock:
            if not self.closed:
                try:
                    P.send_msg(self._sock, {"type": P.BYE})
                except OSError:
                    pass
        self._fail(ConnectionError("closed by us"))
