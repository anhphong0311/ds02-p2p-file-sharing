"""Dashboard web local cho demo DS02.

Chạy bằng:
    python -m src.web_demo.server

Dashboard chỉ bind 127.0.0.1. Nó gọi các script demo hiện có và đọc structured
log thật; không thay thế tracker, downloader hay giao thức P2P.
"""

import argparse
import hashlib
import json
import mimetypes
import os
import re
import socket
import subprocess
import threading
import time
import urllib.parse
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
STATIC = Path(__file__).resolve().parent / "static"
LOG_DIR = ROOT / "logs"
UPLOAD_DIR = ROOT / "web_uploads"
FAULTS = {"kill-peerA", "bad-seeder", "late-leecher", "kill-tracker", "start-tracker", "restart-peerC"}
PEER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
SAFE_FILE_RE = re.compile(r"[^A-Za-z0-9._ -]+")
EVENT_RE = re.compile(r"^(?P<ts>\S+) node=(?P<node>\S+) level=(?P<level>\S+) event=(?P<event>\S+)(?: (?P<fields>.*))?$")
FIELD_RE = re.compile(r'''([A-Za-z_][A-Za-z0-9_]*)=("[^"]*"|\S+)''')
ACTION_LOCK = threading.Lock()


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def _parse_line(line):
    match = EVENT_RE.match(line.strip())
    if not match:
        return None
    item = match.groupdict()
    fields = {}
    for key, value in FIELD_RE.findall(item.get("fields") or ""):
        fields[key] = value.strip('"')
    item["fields"] = fields
    return item


def _tail(path, max_lines=500):
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return handle.readlines()[-max_lines:]


def _all_events(limit=180):
    events = []
    if LOG_DIR.is_dir():
        for path in LOG_DIR.glob("*.log"):
            for line in _tail(path, 400):
                event = _parse_line(line)
                if event:
                    events.append(event)
    events.sort(key=lambda x: x["ts"], reverse=True)
    return events[:limit]


def _port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.12):
            return True
    except OSError:
        return False


def _sha256(path):
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _dashboard_state():
    events = _all_events()
    by_node = {}
    source_counts = {}
    progress = {"done": 0, "total": 0, "complete": False, "seconds": None}
    for event in events:
        by_node.setdefault(event["node"], event)
        if event["node"] == "peerC" and event["event"] == "CHUNK_OK":
            source = event["fields"].get("source", "unknown")
            source_counts[source] = source_counts.get(source, 0) + 1
            raw = event["fields"].get("progress", "0/0")
            try:
                done, total = (int(x) for x in raw.split("/", 1))
                if done > progress["done"]:
                    progress.update(done=done, total=total)
            except (ValueError, TypeError):
                pass
        if event["node"] == "peerC" and event["event"] == "DOWNLOAD_COMPLETE":
            progress["complete"] = True
            try:
                progress["seconds"] = float(event["fields"].get("seconds", "0"))
            except ValueError:
                pass
    known = {
        "tracker": 8000, "tracker2": 8001, "peerA": 7001, "peerB": 7002,
        "peerC": 7003, "peerD": 7004, "peerBad": 7005,
    }
    nodes = []
    for node in sorted(set(known) | set(by_node)):
        last = by_node.get(node, {})
        port = known.get(node)
        nodes.append({
            "id": node,
            "port": port,
            "online": bool(port and _port_open(port)),
            "event": last.get("event", "CHƯA CÓ LOG"),
            "level": last.get("level", "INFO"),
            "timestamp": last.get("ts"),
        })
    original = ROOT / "samples" / "demo.bin"
    downloaded = ROOT / "downloads" / "peerC" / "demo.bin"
    hash_a = _sha256(original)
    hash_b = _sha256(downloaded)
    return {
        "time": datetime.now().astimezone().isoformat(timespec="seconds"),
        "nodes": nodes,
        "progress": progress,
        "sources": source_counts,
        "events": events,
        "integrity": {
            "original": hash_a,
            "downloaded": hash_b,
            "match": bool(hash_a and hash_b and hash_a == hash_b),
        },
    }


def _powershell_file(script, args=()):
    command = [
        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
        str(ROOT / script), *[str(x) for x in args],
    ]
    return subprocess.Popen(command, cwd=ROOT)


def _start_peer_window(title, peer_id, port, command, target, output=None):
    safe_title = title.replace("'", "")
    safe_target = str(target).replace("'", "''")
    parts = [
        f"$host.UI.RawUI.WindowTitle='{safe_title}'",
        f"Set-Location '{str(ROOT).replace("'", "''")}'",
        r". .\.demo_env.ps1",
    ]
    peer_cmd = f"python -m src.peer.main --peer-id {peer_id} --port {port} {command} '{safe_target}'"
    if output:
        peer_cmd += f" --out '{str(output).replace("'", "''")}'"
    parts.append(peer_cmd)
    flags = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)
    subprocess.Popen(["powershell", "-NoExit", "-Command", "; ".join(parts)], cwd=ROOT, creationflags=flags)


class Handler(BaseHTTPRequestHandler):
    server_version = "DS02WebDemo/1.0"

    def log_message(self, fmt, *args):
        print("WEB", self.address_string(), fmt % args)

    def _send(self, status, body, content_type="application/json; charset=utf-8"):
        if isinstance(body, (dict, list)):
            body = _json_bytes(body)
        elif isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _body_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1024 * 1024:
            raise ValueError("request quá lớn")
        return json.loads(self.rfile.read(length) or b"{}")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/status":
            self._send(200, _dashboard_state())
            return
        path = parsed.path.lstrip("/") or "index.html"
        candidate = (STATIC / path).resolve()
        if STATIC.resolve() not in candidate.parents and candidate != STATIC.resolve():
            self._send(403, {"error": "forbidden"})
            return
        if not candidate.is_file():
            self._send(404, {"error": "not found"})
            return
        mime = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
        self._send(200, candidate.read_bytes(), mime)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        try:
            if parsed.path == "/api/start":
                payload = self._body_json()
                args = ["-TwoTrackers", "-Delay", str(float(payload.get("delay", 0.5)))]
                if payload.get("tls"):
                    args.append("-Tls")
                with ACTION_LOCK:
                    _powershell_file("scripts/demo_local.ps1", args)
                self._send(202, {"ok": True, "message": "Đang mở các node demo"})
                return
            if parsed.path == "/api/fault":
                payload = self._body_json()
                scenario = payload.get("scenario")
                if scenario not in FAULTS:
                    raise ValueError("kịch bản không hợp lệ")
                with ACTION_LOCK:
                    _powershell_file("scripts/fault_inject.ps1", [scenario])
                self._send(202, {"ok": True, "message": f"Đã gửi kịch bản {scenario}"})
                return
            if parsed.path == "/api/share":
                query = urllib.parse.parse_qs(parsed.query)
                peer_id = query.get("peer_id", ["webSeeder"])[0]
                filename = query.get("filename", ["shared.bin"])[0]
                port = int(query.get("port", ["7010"])[0])
                if not PEER_ID_RE.fullmatch(peer_id) or not 1024 <= port <= 65535:
                    raise ValueError("peer id hoặc port không hợp lệ")
                filename = SAFE_FILE_RE.sub("_", Path(filename).name).strip(" .") or "shared.bin"
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > 200 * 1024 * 1024:
                    raise ValueError("file phải từ 1 byte đến 200 MiB")
                folder = UPLOAD_DIR / peer_id
                folder.mkdir(parents=True, exist_ok=True)
                destination = folder / filename
                remaining = length
                with destination.open("wb") as handle:
                    while remaining:
                        block = self.rfile.read(min(1024 * 1024, remaining))
                        if not block:
                            raise ValueError("upload bị ngắt")
                        handle.write(block)
                        remaining -= len(block)
                if not (ROOT / ".demo_env.ps1").is_file():
                    raise ValueError("hãy khởi động demo trước khi thêm seeder")
                _start_peer_window(f"WEB SEEDER {peer_id}", peer_id, port, "seed", destination)
                self._send(202, {"ok": True, "message": f"Đã giao {filename} cho peer {peer_id}", "bytes": length})
                return
            if parsed.path == "/api/download":
                payload = self._body_json()
                peer_id = payload.get("peer_id", "webLeecher")
                target = str(payload.get("target", "demo.bin"))
                port = int(payload.get("port", 7011))
                if not PEER_ID_RE.fullmatch(peer_id) or not 1024 <= port <= 65535:
                    raise ValueError("peer id hoặc port không hợp lệ")
                if not (ROOT / ".demo_env.ps1").is_file():
                    raise ValueError("hãy khởi động demo trước")
                output = ROOT / "downloads" / peer_id
                _start_peer_window(f"WEB LEECHER {peer_id}", peer_id, port, "download", target, output)
                self._send(202, {"ok": True, "message": f"Đã khởi động {peer_id} tải {target}"})
                return
            self._send(404, {"error": "not found"})
        except (ValueError, json.JSONDecodeError) as exc:
            self._send(400, {"error": str(exc)})
        except Exception as exc:
            self._send(500, {"error": f"{type(exc).__name__}: {exc}"})


def main():
    parser = argparse.ArgumentParser(description="DS02 local web demo dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8088)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"DS02 Web Demo: http://{args.host}:{args.port}")
    print("Ctrl+C để dừng dashboard; các peer/tracker chạy ở process riêng.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
