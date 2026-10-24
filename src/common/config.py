"""Cấu hình hệ thống, đọc từ biến môi trường (và file .env nếu có).

Mọi bí mật (JOIN_KEY, TRACKER_SECRET) chỉ lấy từ môi trường, không hard-code.
"""
import os
from dataclasses import dataclass


def load_dotenv(path=".env"):
    """Nạp file .env đơn giản (KEY=VALUE). Không ghi đè biến đã có sẵn."""
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _int(name, default):
    return int(os.environ.get(name, default))


def _float(name, default):
    return float(os.environ.get(name, default))


def _str(name, default):
    return os.environ.get(name, default)


@dataclass
class Settings:
    tracker_url: str = "http://127.0.0.1:8000"
    join_key: str = "change-me-join-key"
    tracker_secret: str = "change-me-tracker-secret"
    token_ttl: int = 24 * 3600

    heartbeat_interval: float = 3.0      # peer gửi heartbeat mỗi N giây
    peer_timeout: float = 10.0           # tracker coi peer chết nếu quá N giây không heartbeat

    chunk_size: int = 256 * 1024
    max_body: int = 8 * 1024 * 1024      # giới hạn body HTTP gửi lên tracker
    max_upload_conns: int = 8            # số kết nối upload đồng thời tối đa của 1 peer
    max_download_workers: int = 4        # số luồng tải song song
    max_requests_per_peer: int = 3       # số request đồng thời tối đa tới cùng 1 peer
    connect_timeout: float = 3.0
    request_timeout: float = 10.0
    max_chunk_retries: int = 5           # số lần thử lại tối đa cho mỗi chunk (bounded retry)
    max_peer_strikes: int = 3            # gửi chunk hỏng quá N lần -> cấm peer
    peer_cooldown: float = 3.0           # peer lỗi kết nối bị tạm bỏ qua N giây
    peer_refresh_interval: float = 2.0   # hỏi lại tracker danh sách peer mỗi N giây
    stall_timeout: float = 60.0          # không còn nguồn nào trong N giây -> dừng tải
    tracker_retries: int = 3

    data_dir: str = "data"
    log_dir: str = "logs"

    @classmethod
    def from_env(cls):
        load_dotenv()
        d = cls()
        return cls(
            tracker_url=_str("TRACKER_URL", d.tracker_url),
            join_key=_str("JOIN_KEY", d.join_key),
            tracker_secret=_str("TRACKER_SECRET", d.tracker_secret),
            token_ttl=_int("TOKEN_TTL", d.token_ttl),
            heartbeat_interval=_float("HEARTBEAT_INTERVAL", d.heartbeat_interval),
            peer_timeout=_float("PEER_TIMEOUT", d.peer_timeout),
            chunk_size=_int("CHUNK_SIZE", d.chunk_size),
            max_body=_int("MAX_BODY", d.max_body),
            max_upload_conns=_int("MAX_UPLOAD_CONNS", d.max_upload_conns),
            max_download_workers=_int("MAX_DOWNLOAD_WORKERS", d.max_download_workers),
            max_requests_per_peer=_int("MAX_REQUESTS_PER_PEER", d.max_requests_per_peer),
            connect_timeout=_float("CONNECT_TIMEOUT", d.connect_timeout),
            request_timeout=_float("REQUEST_TIMEOUT", d.request_timeout),
            max_chunk_retries=_int("MAX_CHUNK_RETRIES", d.max_chunk_retries),
            max_peer_strikes=_int("MAX_PEER_STRIKES", d.max_peer_strikes),
            peer_cooldown=_float("PEER_COOLDOWN", d.peer_cooldown),
            peer_refresh_interval=_float("PEER_REFRESH_INTERVAL", d.peer_refresh_interval),
            stall_timeout=_float("STALL_TIMEOUT", d.stall_timeout),
            tracker_retries=_int("TRACKER_RETRIES", d.tracker_retries),
            data_dir=_str("DATA_DIR", d.data_dir),
            log_dir=_str("LOG_DIR", d.log_dir),
        )
