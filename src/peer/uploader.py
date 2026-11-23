"""Upload server của peer: phục vụ chunk cho các peer khác qua TCP.

- Mỗi kết nối được xử lý trên một luồng riêng.
- Số kết nối đồng thời bị giới hạn bằng semaphore; vượt giới hạn -> trả lỗi BUSY.
- Peer gọi tới phải gửi HELLO kèm token do tracker cấp (xác thực).
- Peer chỉ trả chunk mà nó đã có và đã kiểm tra hash -> có thể upload trong lúc vẫn đang tải.
"""
import socket
import ssl
import threading
import time

from src.common import protocol as P


class UploadServer(threading.Thread):
    def __init__(self, node, port, bind_host="0.0.0.0"):
        super().__init__(daemon=True, name="uploader")
        self.node = node
        self.log = node.log
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((bind_host, port))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self._slots = threading.BoundedSemaphore(node.settings.max_upload_conns)
        self._bandwidth = threading.Lock()  # dùng cho giả lập băng thông upload hữu hạn
        self._running = True

    def run(self):
        while self._running:
            try:
                conn, addr = self.sock.accept()
            except OSError:
                break
            threading.Thread(target=self._serve_conn, args=(conn, addr), daemon=True).start()

    def stop(self):
        self._running = False
        try:
            self.sock.close()
        except OSError:
            pass

    def _serve_conn(self, conn, addr):
        if self.node.server_tls is not None:
            try:
                conn.settimeout(self.node.settings.request_timeout)
                conn = self.node.server_tls.wrap_socket(conn, server_side=True)
            except (ssl.SSLError, OSError) as e:
                self.log.warn("UPLOAD_TLS_FAILED", client=addr[0], error=type(e).__name__)
                conn.close()
                return
        if not self._slots.acquire(blocking=False):
            try:
                # Đọc HELLO trước khi trả BUSY: đóng socket khi còn dữ liệu chưa đọc sẽ sinh RST
                # và làm mất thông báo BUSY ở phía downloader.
                conn.settimeout(2)
                P.recv_msg(conn, max_payload=0)
                P.send_msg(conn, {"type": P.ERROR, "code": P.E_BUSY})
            except (P.ProtocolError, OSError):
                pass
            conn.close()
            self.log.warn("UPLOAD_BUSY", client=f"{addr[0]}:{addr[1]}")
            return
        remote = "?"
        try:
            conn.settimeout(self.node.settings.request_timeout)
            hello, _ = P.recv_msg(conn, max_payload=0)
            remote = str(hello.get("peer_id"))[:64]
            if hello["type"] != P.HELLO or self.node.verify_peer_token(hello.get("token")) != hello.get("peer_id"):
                P.send_msg(conn, {"type": P.ERROR, "code": P.E_AUTH})
                self.log.warn("UPLOAD_AUTH_FAILED", claimed=remote, client=addr[0])
                return
            P.send_msg(conn, {"type": P.HELLO_OK, "peer_id": self.node.peer_id})
            while True:
                try:
                    msg, _ = P.recv_msg(conn, max_payload=0)
                except (ConnectionError, socket.timeout):
                    return
                if msg["type"] == P.BYE:
                    return
                if msg["type"] == P.GET_CHUNK:
                    self._send_chunk(conn, msg, remote)
                elif msg["type"] == P.GET_BITFIELD:
                    store = self.node.stores.get(msg.get("file_id"))
                    P.send_msg(conn, {"type": P.BITFIELD, "file_id": msg.get("file_id"),
                                      "chunks": store.have_list() if store else []})
                else:
                    P.send_msg(conn, {"type": P.ERROR, "code": P.E_BAD_REQUEST})
        except (P.ProtocolError, ConnectionError, OSError) as e:
            self.log.warn("UPLOAD_CONN_ERROR", peer=remote, error=f"{type(e).__name__}: {e}")
        finally:
            conn.close()
            self._slots.release()

    def _send_chunk(self, conn, msg, remote):
        file_id, index, req_id = msg.get("file_id"), msg.get("index"), msg.get("request_id")
        store = self.node.stores.get(file_id) if isinstance(file_id, str) else None
        if store is None or not isinstance(index, int) or not store.has(index):
            P.send_msg(conn, {"type": P.ERROR, "code": P.E_NOT_HAVE, "request_id": req_id})
            return
        data = store.read_chunk(index)
        faults = self.node.faults
        if faults.upload_delay:
            # Giả lập băng thông: cả peer chỉ gửi được 1 chunk mỗi upload_delay giây
            with self._bandwidth:
                time.sleep(faults.upload_delay)
        if faults.corrupts(index):
            # Mô phỏng peer gửi dữ liệu hỏng/độc hại
            data = bytes([data[0] ^ 0xFF]) + data[1:]
            self.log.warn("FAULT_CORRUPT_SENT", file=file_id[:12], index=index, to=remote)
        P.send_msg(conn, {"type": P.CHUNK, "file_id": file_id, "index": index, "request_id": req_id}, data)
        self.log.info("CHUNK_SENT", file=file_id[:12], index=index, to=remote, bytes=len(data))
