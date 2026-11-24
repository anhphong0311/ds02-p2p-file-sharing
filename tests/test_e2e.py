"""Test tích hợp: tracker và mỗi peer là một tiến trình riêng, giao tiếp qua mạng (localhost)."""
import json
import os
import re
import socket
import urllib.error
import urllib.request

import pytest

from src.common import protocol as P
from src.common.metadata import build_metadata
from tests.cluster import Cluster


@pytest.fixture
def cluster(tmp_path):
    c = Cluster(tmp_path)
    c.start_tracker()
    yield c
    c.stop_all()


@pytest.fixture
def sample(tmp_path):
    path = tmp_path / "movie.bin"
    path.write_bytes(os.urandom(64 * 1024 * 48 + 999))  # 49 chunk
    return str(path), build_metadata(str(path), 64 * 1024, "")["file_id"]


def downloaded(tmp_path, peer):
    return open(tmp_path / "dl" / peer / "movie.bin", "rb").read()


def download_cmd(tmp_path, peer, file_id, exit_after=True):
    cmd = ["download", file_id, "--out", str(tmp_path / "dl" / peer)]
    return cmd + (["--exit-after-download"] if exit_after else [])


def test_parallel_download_from_two_seeders(cluster, sample, tmp_path):
    """[F7] Tải song song từ 2 seeder, ghép file và kiểm tra hash cuối."""
    path, fid = sample
    cluster.start_peer("seedA", "seed", path)
    cluster.start_peer("seedB", "seed", path)
    cluster.wait_for("seedA", "event=SEEDING")
    cluster.wait_for("seedB", "event=SEEDING")
    cluster.start_peer("leech", *download_cmd(tmp_path, "leech", fid))
    cluster.wait_for("leech", "event=DOWNLOAD_COMPLETE", timeout=60)
    assert downloaded(tmp_path, "leech") == open(path, "rb").read()
    assert cluster.count("leech", r"event=CHUNK_OK .*source=seedA") > 0
    assert cluster.count("leech", r"event=CHUNK_OK .*source=seedB") > 0
    # tracker không bao giờ gửi nội dung file
    assert cluster.count("tracker", "CHUNK_SENT") == 0


def test_upload_while_downloading(cluster, sample, tmp_path):
    """[F8] Peer chưa tải xong vẫn upload các chunk đã có cho peer khác."""
    path, fid = sample
    cluster.start_peer("seed", "seed", path, extra=["--upload-delay", "0.15"])
    cluster.wait_for("seed", "event=SEEDING")
    cluster.start_peer("L1", *download_cmd(tmp_path, "L1", fid, exit_after=False))
    cluster.wait_for("L1", "event=CHUNK_OK", times=8)
    cluster.start_peer("L2", *download_cmd(tmp_path, "L2", fid))
    cluster.wait_for("L2", "event=DOWNLOAD_COMPLETE", timeout=60)
    cluster.wait_for("L1", "event=DOWNLOAD_COMPLETE", timeout=60)
    log = cluster.log("L1")
    first_sent = log.find("event=CHUNK_SENT")
    assert first_sent != -1, "L1 never uploaded"
    assert first_sent < log.find("event=DOWNLOAD_COMPLETE"), "L1 only uploaded after finishing"
    assert downloaded(tmp_path, "L2") == open(path, "rb").read()


def test_corrupt_source_is_detected_and_avoided(cluster, sample, tmp_path):
    """[R3] Một seeder gửi chunk hỏng: chunk bị từ chối, tải lại từ seeder khác, seeder hỏng bị cấm."""
    path, fid = sample
    cluster.start_peer("bad", "seed", path, extra=["--corrupt-chunks", "all"])
    cluster.start_peer("good", "seed", path)
    cluster.wait_for("bad", "event=SEEDING")
    cluster.wait_for("good", "event=SEEDING")
    cluster.start_peer("leech", *download_cmd(tmp_path, "leech", fid))
    cluster.wait_for("leech", "event=DOWNLOAD_COMPLETE", timeout=60)
    assert cluster.count("leech", "event=CHUNK_REJECTED .*source=bad") >= 1
    assert cluster.count("leech", "event=PEER_BANNED peer=bad") == 1
    assert cluster.count("leech", "event=CHUNK_OK .*source=bad") == 0
    assert downloaded(tmp_path, "leech") == open(path, "rb").read()


def test_source_killed_during_download(cluster, sample, tmp_path):
    """[R4] Dừng đột ngột một nguồn khi đang tải -> chuyển sang nguồn còn lại và tải xong."""
    path, fid = sample
    cluster.start_peer("s1", "seed", path, extra=["--upload-delay", "0.1"])
    cluster.start_peer("s2", "seed", path, extra=["--upload-delay", "0.1"])
    cluster.wait_for("s1", "event=SEEDING")
    cluster.wait_for("s2", "event=SEEDING")
    cluster.start_peer("leech", *download_cmd(tmp_path, "leech", fid))
    cluster.wait_for("leech", "event=CHUNK_OK .*source=s1", times=3)
    cluster.kill("s1")
    cluster.wait_for("leech", "event=DOWNLOAD_COMPLETE", timeout=60)
    assert downloaded(tmp_path, "leech") == open(path, "rb").read()
    # tracker phát hiện s1 offline do mất heartbeat
    cluster.wait_for("tracker", "event=PEER_EXPIRED peer=s1", timeout=10)


def test_resume_after_restart(cluster, sample, tmp_path):
    """[R5] Peer bị tắt giữa chừng, chạy lại thì tải tiếp từ các chunk đã có."""
    path, fid = sample
    cluster.start_peer("seed", "seed", path, extra=["--upload-delay", "0.1"])
    cluster.wait_for("seed", "event=SEEDING")
    cluster.start_peer("leech", *download_cmd(tmp_path, "leech", fid))
    cluster.wait_for("leech", "event=CHUNK_OK", times=10)
    cluster.kill("leech")
    cluster.start_peer("leech", *download_cmd(tmp_path, "leech", fid))
    cluster.wait_for("leech", "event=DOWNLOAD_COMPLETE", timeout=60)
    log = cluster.log("leech")
    resumed = [int(x) for x in re.findall(r"already_have=(\d+)", log)]
    assert resumed[0] == 0 and resumed[1] >= 10
    assert downloaded(tmp_path, "leech") == open(path, "rb").read()


def test_tracker_restart_recovers(cluster, sample, tmp_path):
    """Tracker khởi động lại: metadata được khôi phục, peer tự đăng ký và announce lại."""
    path, fid = sample
    cluster.start_peer("seed", "seed", path)
    cluster.wait_for("seed", "event=SEEDING")
    cluster.kill("tracker")
    cluster.wait_for("seed", "event=TRACKER_DOWN", timeout=10)
    cluster.start_tracker()
    cluster.wait_for("seed", "event=REGISTERED", times=2, timeout=15)
    cluster.start_peer("leech", *download_cmd(tmp_path, "leech", fid))
    cluster.wait_for("leech", "event=DOWNLOAD_COMPLETE", timeout=60)
    assert downloaded(tmp_path, "leech") == open(path, "rb").read()


def test_bad_join_key_and_unauthenticated_peer(cluster, sample):
    """[S7] Sai join key -> tracker từ chối; peer không có token hợp lệ -> uploader từ chối."""
    path, _ = sample
    body = json.dumps({"peer_id": "evil", "port": 9999, "join_key": "wrong"}).encode()
    req = urllib.request.Request(cluster.tracker_url + "/api/register", data=body, method="POST")
    with pytest.raises(urllib.error.HTTPError) as e:
        urllib.request.urlopen(req, timeout=5)
    assert e.value.code == 403

    cluster.start_peer("seed", "seed", path, extra=["--port", "0"])
    cluster.wait_for("seed", "event=SEEDING")
    port = int(re.search(r"event=PEER_START port=(\d+)", cluster.log("seed")).group(1))
    with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
        P.send_msg(s, {"type": P.HELLO, "peer_id": "evil", "token": "evil.9999999999.deadbeef"})
        resp, _ = P.recv_msg(s, max_payload=0)
    assert resp == {"type": P.ERROR, "code": P.E_AUTH, "size": 0}
    cluster.wait_for("seed", "event=UPLOAD_AUTH_FAILED claimed=evil")


