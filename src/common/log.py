"""Log có cấu trúc: thời gian, node, mức độ, loại sự kiện, các trường key=value.

Ví dụ:
2026-10-24T23:30:01.123+07:00 node=peerC level=INFO event=CHUNK_OK file=3fa1.. index=7 from=peerA ms=12
"""
import os
import threading
from datetime import datetime

_print_lock = threading.Lock()


def _fmt(value):
    s = str(value)
    if s == "" or " " in s:
        return '"' + s.replace('"', "'") + '"'
    return s


class NodeLogger:
    def __init__(self, node_id, log_dir=None):
        self.node_id = node_id
        self._file = None
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            self._file = open(os.path.join(log_dir, f"{node_id}.log"), "a", encoding="utf-8")

    def event(self, event, level="INFO", **fields):
        ts = datetime.now().astimezone().isoformat(timespec="milliseconds")
        kv = " ".join(f"{k}={_fmt(v)}" for k, v in fields.items())
        line = f"{ts} node={self.node_id} level={level} event={event} {kv}".rstrip()
        with _print_lock:
            print(line, flush=True)
            if self._file:
                self._file.write(line + "\n")
                self._file.flush()

    def info(self, event, **fields):
        self.event(event, "INFO", **fields)

    def warn(self, event, **fields):
        self.event(event, "WARN", **fields)

    def error(self, event, **fields):
        self.event(event, "ERROR", **fields)
