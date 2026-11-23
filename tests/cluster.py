"""Helper dựng cụm tracker + peer bằng tiến trình thật, phục vụ test tích hợp.

Mỗi tracker / peer được chạy bằng `python -m ...` trong một tiến trình con riêng,
dùng chung JOIN_KEY và TRACKER_SECRET (để token do tracker nào cấp cũng hợp lệ ở
tracker khác). Log có cấu trúc mà node in ra stdout được một luồng đọc gom lại theo
tên node, nên test có thể `wait_for(name, "event=...")`.

Các tham số thời gian (heartbeat, timeout...) được rút ngắn để test chạy nhanh.
"""
import os
import re
import socket
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Bí mật dùng chung cho mọi tracker/peer trong cụm test.
JOIN_KEY = "test-join-key"
TRACKER_SECRET = "test-tracker-secret-0123456789abcdef"


def _free_port():
    """Xin một cổng TCP trống trên loopback (gán cho tracker trước khi chạy)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class _Proc:
    """Một tiến trình con + bộ đệm log của nó (gom theo tên node)."""

    def __init__(self, name, buffer, lock):
        self.name = name
        self.buffer = buffer        # list[str] dùng chung cho mọi lần chạy cùng tên
        self._lock = lock
        self.popen = None
        self._reader = None

    def start(self, argv, env, cwd):
        self.popen = subprocess.Popen(
            [sys.executable, "-u", *argv],
            cwd=cwd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        self._reader = threading.Thread(target=self._pump, daemon=True)
        self._reader.start()

    def _pump(self):
        for line in self.popen.stdout:
            with self._lock:
                self.buffer.append(line)

    def stop(self):
        if self.popen and self.popen.poll() is None:
            self.popen.terminate()
            try:
                self.popen.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.popen.kill()
                self.popen.wait(timeout=5)


class Cluster:
    """Dựng và điều khiển một cụm tracker + peer trong một thư mục tạm."""

    def __init__(self, tmp_path, trackers=1, tls_dir=None):
        self.base = str(tmp_path)
        self.n_trackers = trackers
        self.tls_dir = tls_dir
        self.scheme = "https" if tls_dir else "http"
        self.tracker_ports = [_free_port() for _ in range(trackers)]
        self.tracker_names = ["tracker" if i == 0 else f"tracker{i + 1}" for i in range(trackers)]

        self._procs = {}                         # name -> _Proc đang chạy
        self._buffers = {}                       # name -> list[str] (giữ qua các lần restart)
        self._buf_lock = threading.Lock()

    # ---------- URL tracker ----------
    @property
    def tracker_url(self):
        return self._url(0)

    def _url(self, index):
        return f"{self.scheme}://127.0.0.1:{self.tracker_ports[index]}"

    def _tracker_list(self):
        return ",".join(self._url(i) for i in range(self.n_trackers))

    # ---------- môi trường ----------
    def _env(self):
        env = dict(os.environ)
        env.update(
            JOIN_KEY=JOIN_KEY,
            TRACKER_SECRET=TRACKER_SECRET,
            TOKEN_TTL="86400",
            DATA_DIR=os.path.join(self.base, "data"),
            LOG_DIR=os.path.join(self.base, "logs"),
            # Thời gian rút ngắn cho test chạy nhanh.
            HEARTBEAT_INTERVAL="0.5",
            PEER_TIMEOUT="4",
            PEER_REFRESH_INTERVAL="0.5",
            PEER_COOLDOWN="1",
            CONNECT_TIMEOUT="3",
            REQUEST_TIMEOUT="5",
            STALL_TIMEOUT="30",
            # Các fixture tính file_id với chunk 64KiB -> peer phải dùng đúng cỡ này.
            CHUNK_SIZE=str(64 * 1024),
        )
        if self.tls_dir:
            env.update(
                TLS_ENABLED="1",
                TLS_CA=os.path.join(self.tls_dir, "ca.crt"),
                TLS_CERT=os.path.join(self.tls_dir, "node.crt"),
                TLS_KEY=os.path.join(self.tls_dir, "node.key"),
            )
        else:
            env["TLS_ENABLED"] = "0"
        return env

    def _buffer(self, name):
        with self._buf_lock:
            return self._buffers.setdefault(name, [])

    def _spawn(self, name, argv):
        proc = _Proc(name, self._buffer(name), self._buf_lock)
        proc.start(argv, self._env(), ROOT)
        self._procs[name] = proc
        return proc

    # ---------- khởi chạy node ----------
    def start_tracker(self, index=0):
        name = self.tracker_names[index]
        self._spawn(name, [
            "-m", "src.tracker.server",
            "--host", "127.0.0.1",
            "--port", str(self.tracker_ports[index]),
            "--node-id", name,
        ])
        return name

    def start_peer(self, name, command, target, *args, extra=None):
        """Chạy một peer.

        name     : peer-id, cũng là tên để tra log.
        command  : "seed" | "download" | "serve" | "list".
        target   : đường dẫn file (seed) hoặc file_id/tên file (download).
        *args    : tham số CON của lệnh con, đặt SAU target (vd. --out, --exit-after-download).
        extra    : tham số CHUNG, đặt TRƯỚC lệnh con (vd. --upload-delay, --port).
        """
        argv = ["-m", "src.peer.main", "--peer-id", name, "--tracker", self._tracker_list()]
        argv += [str(x) for x in (extra or [])]
        argv += [command, str(target)]
        argv += [str(a) for a in args]
        self._spawn(name, argv)
        return name

    # ---------- quan sát / điều khiển ----------
    def log(self, name):
        with self._buf_lock:
            return "".join(self._buffers.get(name, []))

    def wait_for(self, name, pattern, times=1, timeout=30):
        """Chờ tới khi log của `name` khớp regex `pattern` đủ `times` lần."""
        rx = re.compile(pattern)
        deadline = time.monotonic() + timeout
        while True:
            if len(rx.findall(self.log(name))) >= times:
                return
            if time.monotonic() > deadline:
                raise AssertionError(
                    f"timeout {timeout}s chờ {pattern!r} (>= {times} lần) ở node {name!r}.\n"
                    f"--- log {name} ---\n{self.log(name)[-4000:]}"
                )
            time.sleep(0.1)

    def kill(self, name):
        """Dừng đột ngột một node (mô phỏng sự cố)."""
        proc = self._procs.get(name)
        if proc and proc.popen and proc.popen.poll() is None:
            proc.popen.kill()
            proc.popen.wait(timeout=5)

    def stop_all(self):
        for proc in list(self._procs.values()):
            proc.stop()
