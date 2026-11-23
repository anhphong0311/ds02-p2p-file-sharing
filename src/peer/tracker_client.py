"""Client HTTP(S) gọi tracker, có timeout và retry giới hạn (exponential backoff).

MultiTrackerClient hỗ trợ nhiều tracker nhân bản (replicated tracker), xem docstring bên dưới.
"""
import json
import threading
import time
import urllib.error
import urllib.request


class TrackerError(Exception):
    def __init__(self, status, code, message):
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message = status, code, message


class TrackerUnavailable(TrackerError):
    pass


class TrackerClient:
    def __init__(self, base_url, timeout=5.0, retries=3, logger=None, ssl_context=None):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.log = logger
        self.token = None
        # Bỏ qua proxy hệ thống để gọi thẳng tracker; HTTPS dùng context TLS của nhóm (CA riêng)
        handlers = [urllib.request.ProxyHandler({})]
        if ssl_context is not None:
            handlers.append(urllib.request.HTTPSHandler(context=ssl_context))
        self._opener = urllib.request.build_opener(*handlers)

    def _request(self, method, path, body=None, retries=None):
        retries = self.retries if retries is None else retries
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        for attempt in range(retries + 1):
            req = urllib.request.Request(self.base_url + path, data=data, method=method, headers=headers)
            try:
                with self._opener.open(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read() or b"{}")
            except urllib.error.HTTPError as e:
                try:
                    err = json.loads(e.read() or b"{}")
                except json.JSONDecodeError:
                    err = {}
                if e.code >= 500 and attempt < retries:
                    time.sleep(0.5 * 2 ** attempt)
                    continue
                raise TrackerError(e.code, err.get("error", "HTTP_ERROR"), err.get("message", str(e))) from None
            except (urllib.error.URLError, OSError) as e:
                if attempt < retries:
                    if self.log:
                        self.log.warn("TRACKER_RETRY", path=path, attempt=attempt + 1, error=type(e).__name__)
                    time.sleep(0.5 * 2 ** attempt)
                    continue
                raise TrackerUnavailable(0, "UNAVAILABLE", f"tracker unreachable: {e}") from None

    # ---------- API ----------
    def register(self, peer_id, port, join_key, host=None):
        body = {"peer_id": peer_id, "port": port, "join_key": join_key}
        if host:
            body["host"] = host
        resp = self._request("POST", "/api/register", body)
        self.token = resp["token"]
        return resp

    def heartbeat(self):
        return self._request("POST", "/api/heartbeat", {}, retries=0)

    def leave(self):
        return self._request("POST", "/api/leave", {}, retries=0)

    def verify(self, token):
        return self._request("POST", "/api/verify", {"token": token})

    def add_file(self, meta):
        return self._request("POST", "/api/files", {"metadata": meta})

    def list_files(self):
        return self._request("GET", "/api/files")["files"]

    def get_file(self, file_id):
        return self._request("GET", f"/api/files/{file_id}")

    def get_peers(self, file_id):
        return self._request("GET", f"/api/files/{file_id}/peers")["peers"]

    def announce(self, file_id, chunks, seq):
        return self._request("POST", "/api/announce", {"file_id": file_id, "chunks": chunks, "seq": seq})


class MultiTrackerClient:
    """Gọi nhiều tracker nhân bản (replicated tracker) với failover.

    Mô hình nhân bản do client điều khiển (client-driven replication):
    - Ghi (register, heartbeat, announce, add_file): gửi tới MỌI tracker đang sống.
      Thành công nếu ít nhất 1 tracker nhận; lỗi logic (vd. 404 UNKNOWN_PEER) được
      báo lên để PeerNode tự đăng ký/announce lại.
    - Đọc (get_file, get_peers, list_files, verify): hỏi tracker ưu tiên (tracker
      trả lời thành công gần nhất), lỗi kết nối thì thử tracker kế tiếp (failover).
    - Tracker không liên lạc được bị tạm bỏ qua DOWN_COOLDOWN giây, sau đó thử lại.
    Mọi tracker dùng chung TRACKER_SECRET nên token do tracker nào cấp cũng hợp lệ ở tracker khác.
    Trạng thái giữa các tracker nhất quán cuối cùng (eventual consistency).
    """

    DOWN_COOLDOWN = 5.0

    def __init__(self, urls, timeout=5.0, retries=3, logger=None, ssl_context=None):
        if not urls:
            raise ValueError("at least one tracker url is required")
        # Có tracker dự phòng thì không cần retry lâu trên 1 tracker: chuyển sang tracker khác nhanh hơn
        per_tracker_retries = retries if len(urls) == 1 else 0
        self.clients = [TrackerClient(u, timeout, per_tracker_retries, logger, ssl_context) for u in urls]
        self.log = logger
        self._lock = threading.Lock()
        self._down_until = {}  # index -> thời điểm được thử lại
        self._preferred = 0

    @property
    def urls(self):
        return [c.base_url for c in self.clients]

    @property
    def token(self):
        return next((c.token for c in self.clients if c.token), None)

    @token.setter
    def token(self, value):
        for c in self.clients:
            c.token = value

    # ---------- trạng thái sống/chết của từng tracker ----------
    def _order(self):
        now = time.monotonic()
        with self._lock:
            idx = list(range(len(self.clients)))
            idx.sort(key=lambda i: (self._down_until.get(i, 0) > now, i != self._preferred, i))
            up = [i for i in idx if self._down_until.get(i, 0) <= now]
        return idx, up

    def _mark_down(self, i, error):
        with self._lock:
            was_up = i not in self._down_until
            self._down_until[i] = time.monotonic() + self.DOWN_COOLDOWN
            if self._preferred == i:
                self._preferred = (i + 1) % len(self.clients)
        if was_up and self.log and len(self.clients) > 1:
            self.log.warn("TRACKER_FAILOVER", down=self.clients[i].base_url, error=error.code)

    def _mark_up(self, i, preferred=False):
        with self._lock:
            was_down = self._down_until.pop(i, None) is not None
            if preferred:
                self._preferred = i
        if was_down and self.log and len(self.clients) > 1:
            self.log.info("TRACKER_BACK", url=self.clients[i].base_url)

    # ---------- đọc / ghi ----------
    def _read(self, fn):
        order, _ = self._order()
        last = None
        for i in order:
            try:
                result = fn(self.clients[i])
            except TrackerUnavailable as e:
                self._mark_down(i, e)
                last = e
                continue
            self._mark_up(i, preferred=True)
            return result
        raise last

    def _write(self, fn):
        order, up = self._order()
        targets = up or order  # mọi tracker đều đang "down" -> vẫn thử hết
        results, errors, last = [], [], None
        for i in targets:
            try:
                results.append(fn(self.clients[i]))
            except TrackerUnavailable as e:
                self._mark_down(i, e)
                last = e
            except TrackerError as e:
                errors.append(e)
            else:
                self._mark_up(i)
        if errors:
            raise errors[0]
        if not results:
            raise last
        return results[0]

    # ---------- API (giống TrackerClient) ----------
    def register(self, peer_id, port, join_key, host=None):
        return self._write(lambda c: c.register(peer_id, port, join_key, host))

    def heartbeat(self):
        return self._write(lambda c: c.heartbeat())

    def leave(self):
        return self._write(lambda c: c.leave())

    def add_file(self, meta):
        return self._write(lambda c: c.add_file(meta))

    def announce(self, file_id, chunks, seq):
        return self._write(lambda c: c.announce(file_id, chunks, seq))

    def verify(self, token):
        return self._read(lambda c: c.verify(token))

    def list_files(self):
        return self._read(lambda c: c.list_files())

    def get_file(self, file_id):
        return self._read(lambda c: c.get_file(file_id))

    def get_peers(self, file_id):
        return self._read(lambda c: c.get_peers(file_id))
