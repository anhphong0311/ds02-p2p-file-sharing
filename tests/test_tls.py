"""Test mã hóa TLS hai chiều (mutual TLS)."""
import os
import socket
import ssl
import subprocess
import sys
import threading
import urllib.error
import urllib.request

import pytest

from src.common import protocol as P
from src.common.config import Settings
from src.common.metadata import build_metadata
from src.common.tls import client_context, server_context
from tests.cluster import ROOT, Cluster


def make_certs(directory):
    try:
        subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "gen_certs.py"), "--out", str(directory)],
                       check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        pytest.skip(f"cannot generate certs: {e.stderr.decode(errors='replace')}")
    return str(directory)


def settings_for(d):
    return Settings(tls_enabled=True, tls_ca=f"{d}/ca.crt", tls_cert=f"{d}/node.crt", tls_key=f"{d}/node.key")


@pytest.fixture(scope="module")
def certs(tmp_path_factory):
    return make_certs(tmp_path_factory.mktemp("certs"))


@pytest.fixture(scope="module")
def other_certs(tmp_path_factory):
    """Bộ chứng chỉ của một CA KHÁC (máy lạ, không thuộc nhóm)."""
    return make_certs(tmp_path_factory.mktemp("other"))


def handshake(server_ctx, client_ctx):
    a, b = socket.socketpair()
    a.settimeout(5)
    b.settimeout(5)
    errors = []

    def serve():
        try:
            s = server_ctx.wrap_socket(a, server_side=True)
            s.sendall(s.recv(4))
            s.close()
        except (ssl.SSLError, OSError) as e:
            errors.append(e)
            a.close()

    t = threading.Thread(target=serve)
    t.start()
    try:
        c = client_ctx.wrap_socket(b)
        c.sendall(b"ping")
        return c.recv(4)
    finally:
        b.close()
        t.join()


def test_tls_roundtrip_with_group_ca(certs):
    """Hai node cùng CA nhóm bắt tay TLS thành công và trao đổi dữ liệu."""
    s = settings_for(certs)
    assert handshake(server_context(s), client_context(s)) == b"ping"


def test_tls_rejects_certificate_from_other_ca(certs, other_certs):
    """Máy có chứng chỉ của CA khác bị từ chối (cả hai chiều)."""
    ours, theirs = settings_for(certs), settings_for(other_certs)
    with pytest.raises((ssl.SSLError, OSError)):
        handshake(server_context(ours), client_context(theirs))
    with pytest.raises((ssl.SSLError, OSError)):
        handshake(server_context(theirs), client_context(ours))


def test_tls_requires_client_certificate(certs):
    """Mutual TLS: client không trình chứng chỉ bị server từ chối."""
    s = settings_for(certs)
    anonymous = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    anonymous.check_hostname = False
    anonymous.load_verify_locations(s.tls_ca)
    with pytest.raises((ssl.SSLError, OSError)):
        handshake(server_context(s), anonymous)


def test_download_over_tls_and_plaintext_rejected(certs, tmp_path):
    """Toàn hệ thống chạy với TLS: tải thành công; client không TLS bị tracker và uploader từ chối."""
    path = tmp_path / "secret.bin"
    path.write_bytes(os.urandom(64 * 1024 * 20))
    fid = build_metadata(str(path), 64 * 1024, "")["file_id"]
    c = Cluster(tmp_path, tls_dir=certs)
    try:
        c.start_tracker()
        c.start_peer("seed", "seed", str(path), extra=["--port", "0"])
        c.wait_for("seed", "event=SEEDING")
        c.start_peer("leech", "download", fid, "--out", str(tmp_path / "dl"), "--exit-after-download")
        c.wait_for("leech", "event=DOWNLOAD_COMPLETE", timeout=60)
        assert open(tmp_path / "dl" / "secret.bin", "rb").read() == path.read_bytes()
        assert "tls=True" in c.log("seed")

        # HTTP thường tới tracker HTTPS -> bị từ chối
        plain_url = c.tracker_url.replace("https://", "http://") + "/api/health"
        with pytest.raises((urllib.error.URLError, OSError)):
            urllib.request.urlopen(plain_url, timeout=3).read()
        c.wait_for("tracker", "event=TLS_HANDSHAKE_FAILED")

        # Kết nối TCP thường tới uploader TLS -> bị từ chối
        import re
        port = int(re.search(r"event=PEER_START port=(\d+)", c.log("seed")).group(1))
        with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
            try:
                P.send_msg(s, {"type": P.HELLO, "peer_id": "x", "token": "y"})
                s.recv(1024)
            except OSError:
                pass
        c.wait_for("seed", "event=UPLOAD_TLS_FAILED")
    finally:
        c.stop_all()
