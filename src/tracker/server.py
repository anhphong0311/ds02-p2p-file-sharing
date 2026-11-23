"""Tracker: HTTP/JSON API, xử lý đa luồng.

API (trừ /api/health, mọi endpoint cần header "Authorization: Bearer <token>"):
    GET  /api/health
    POST /api/register        {peer_id, port, host?, join_key}  -> {token, heartbeat_interval}
    POST /api/heartbeat
    POST /api/leave
    POST /api/verify          {token}                          -> {valid, peer_id}
    GET  /api/files
    POST /api/files           {metadata}                       -> {file_id, created}
    GET  /api/files/<id>                                       -> metadata
    POST /api/announce        {file_id, chunks, seq}           -> {accepted}
    GET  /api/files/<id>/peers                                 -> {peers: [...]}

Chạy:  python -m src.tracker.server --port 8000
"""
import argparse
import json
import re
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src.common import tls
from src.common.auth import check_secret, hash_secret, issue_token, valid_peer_id, verify_token
from src.common.config import Settings
from src.common.log import NodeLogger
from src.common.metadata import MetadataError
from src.tracker.state import StateError, TrackerState

FILE_RE = re.compile(r"^/api/files/([0-9a-f]{64})$")
PEERS_RE = re.compile(r"^/api/files/([0-9a-f]{64})/peers$")


class ApiError(Exception):
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


class TrackerServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, addr, settings, state, logger, ssl_context=None):
        super().__init__(addr, TrackerHandler)
        self.settings = settings
        self.state = state
        self.log = logger
        self.join_key_hash = hash_secret(settings.join_key)
        if ssl_context is not None:
            # HTTPS: bắt tay TLS được làm trong luồng xử lý request (finish_request), không chặn accept
            self.socket = ssl_context.wrap_socket(self.socket, server_side=True, do_handshake_on_connect=False)

    def finish_request(self, request, client_address):
        if isinstance(request, ssl.SSLSocket):
            try:
                request.settimeout(10)
                request.do_handshake()
            except (ssl.SSLError, OSError) as e:
                self.log.warn("TLS_HANDSHAKE_FAILED", client=client_address[0], error=type(e).__name__)
                return
        super().finish_request(request, client_address)


class TrackerHandler(BaseHTTPRequestHandler):
    server_version = "DS02Tracker/1.0"

    def log_message(self, *args):  # dùng logger riêng thay cho log mặc định
        pass

    # ---------- helpers ----------
    def _send(self, status, obj):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise ApiError(400, "BAD_REQUEST", "invalid Content-Length") from None
        if length > self.server.settings.max_body:
            raise ApiError(413, "TOO_LARGE", "request body too large")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            data = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ApiError(400, "BAD_JSON", "body is not valid JSON") from None
        if not isinstance(data, dict):
            raise ApiError(400, "BAD_JSON", "body must be a JSON object")
        return data

    def _token(self):
        auth = self.headers.get("Authorization", "")
        return auth[7:] if auth.startswith("Bearer ") else None

    def _auth(self):
        peer_id = verify_token(self.server.settings.tracker_secret, self._token())
        if not peer_id:
            raise ApiError(401, "UNAUTHORIZED", "missing or invalid token")
        return peer_id

    def _handle(self, method):
        started = time.monotonic()
        path = self.path.split("?", 1)[0]
        peer = "-"
        status = 500
        try:
            status, result, peer = self._route(method, path)
            self._send(status, result)
        except (ApiError, StateError) as e:
            status = e.status
            self._send(e.status, {"error": e.code, "message": e.message})
            self.server.log.warn("API_ERROR", method=method, path=path, status=e.status, code=e.code,
                                 client=self.client_address[0])
        except MetadataError as e:
            status = 400
            self._send(400, {"error": "BAD_METADATA", "message": str(e)})
            self.server.log.warn("API_ERROR", method=method, path=path, status=400, code="BAD_METADATA",
                                 reason=str(e))
        except Exception as e:  # không để lỗi lạ làm sập server
            self._send(500, {"error": "INTERNAL", "message": "internal error"})
            self.server.log.error("API_EXCEPTION", method=method, path=path, error=repr(e))
        if path not in ("/api/heartbeat", "/api/health", "/api/verify") or status >= 400:
            self.server.log.info("HTTP", method=method, path=path, status=status, peer=peer,
                                 ms=int((time.monotonic() - started) * 1000))

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    # ---------- routes ----------
    def _route(self, method, path):
        srv, state, log = self.server, self.server.state, self.server.log

        if method == "GET" and path == "/api/health":
            return 200, {"status": "ok", **state.stats()}, "-"

        if method == "POST" and path == "/api/register":
            body = self._body()
            peer_id, port = body.get("peer_id"), body.get("port")
            if not valid_peer_id(peer_id):
                raise ApiError(400, "BAD_PEER_ID", "peer_id must match [A-Za-z0-9_-]{1,64}")
            if not isinstance(port, int) or not 1 <= port <= 65535:
                raise ApiError(400, "BAD_PORT", "invalid port")
            if not check_secret(body.get("join_key"), srv.join_key_hash):
                log.warn("REGISTER_REJECTED", peer=peer_id, reason="bad_join_key", client=self.client_address[0])
                raise ApiError(403, "BAD_JOIN_KEY", "join key is wrong")
            host = body.get("host") or self.client_address[0]
            if not isinstance(host, str) or len(host) > 253:
                raise ApiError(400, "BAD_HOST", "invalid host")
            auth_peer = verify_token(srv.settings.tracker_secret, self._token())
            rejoin = state.register_peer(peer_id, host, port, auth_peer)
            token = issue_token(srv.settings.tracker_secret, peer_id, srv.settings.token_ttl)
            log.info("PEER_REGISTERED", peer=peer_id, addr=f"{host}:{port}", rejoin=rejoin)
            return 200, {
                "token": token,
                "heartbeat_interval": srv.settings.heartbeat_interval,
                "peer_timeout": srv.settings.peer_timeout,
            }, peer_id

        peer_id = self._auth()

        if method == "POST" and path == "/api/heartbeat":
            state.heartbeat(peer_id)
            return 200, {"ok": True}, peer_id

        if method == "POST" and path == "/api/leave":
            if state.remove_peer(peer_id):
                log.info("PEER_LEFT", peer=peer_id)
            return 200, {"ok": True}, peer_id

        if method == "POST" and path == "/api/verify":
            other = verify_token(srv.settings.tracker_secret, self._body().get("token"))
            return 200, {"valid": other is not None, "peer_id": other}, peer_id

        if method == "GET" and path == "/api/files":
            return 200, {"files": state.list_files()}, peer_id

        if method == "POST" and path == "/api/files":
            meta = self._body().get("metadata")
            created = state.add_file(meta)
            log.info("FILE_REGISTERED", peer=peer_id, file=meta["file_id"][:12], name=meta["name"],
                     size=meta["size"], chunks=meta["num_chunks"], created=created)
            return 200, {"file_id": meta["file_id"], "created": created}, peer_id

        m = FILE_RE.match(path)
        if method == "GET" and m:
            return 200, state.get_file(m.group(1)), peer_id

        m = PEERS_RE.match(path)
        if method == "GET" and m:
            peers = state.peers_for(m.group(1), exclude=peer_id)
            return 200, {"peers": peers}, peer_id

        if method == "POST" and path == "/api/announce":
            body = self._body()
            file_id, chunks, seq = body.get("file_id"), body.get("chunks"), body.get("seq")
            if not isinstance(file_id, str) or not isinstance(seq, int):
                raise ApiError(400, "BAD_REQUEST", "file_id and seq are required")
            accepted = state.announce(peer_id, file_id, chunks, seq)
            if accepted:
                log.info("ANNOUNCE", peer=peer_id, file=file_id[:12], have=len(chunks), seq=seq)
            else:
                log.warn("ANNOUNCE_STALE", peer=peer_id, file=file_id[:12], seq=seq)
            return 200, {"accepted": accepted}, peer_id

        raise ApiError(404, "NOT_FOUND", f"no route {method} {path}")


def reaper_loop(state, logger, interval, stop_event):
    """Định kỳ xóa peer quá hạn heartbeat (phát hiện peer offline)."""
    while not stop_event.wait(interval):
        for pid in state.expire_peers():
            logger.warn("PEER_EXPIRED", peer=pid, reason="heartbeat_timeout")


def main(argv=None):
    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description="DS02 tracker")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--node-id", default="tracker")
    parser.add_argument("--state-file", default=None, help="mặc định: <DATA_DIR>/tracker/state.json")
    args = parser.parse_args(argv)

    logger = NodeLogger(args.node_id, settings.log_dir)
    state_file = args.state_file or f"{settings.data_dir}/{args.node_id}/state.json"
    state = TrackerState(settings.peer_timeout, state_file, logger)
    server = TrackerServer((args.host, args.port), settings, state, logger, tls.server_context(settings))

    stop = threading.Event()
    threading.Thread(target=reaper_loop, args=(state, logger, 1.0, stop), daemon=True).start()
    logger.info("TRACKER_START", addr=f"{args.host}:{args.port}", tls=settings.tls_enabled,
                peer_timeout=settings.peer_timeout,
                state_file=state_file)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
        logger.info("TRACKER_STOP")


if __name__ == "__main__":
    main()
