"""Test tracker nhân bản (replicated tracker) và failover."""
import os

import pytest

from src.common.metadata import build_metadata
from src.peer.tracker_client import MultiTrackerClient, TrackerError, TrackerUnavailable
from tests.cluster import Cluster


# ---------- unit test logic failover với tracker giả ----------
class FakeTracker:
    def __init__(self, up=True, error=None):
        self.up, self.error, self.calls, self.token, self.base_url = up, error, 0, None, "fake"

    def get_peers(self, file_id):
        self.calls += 1
        if not self.up:
            raise TrackerUnavailable(0, "UNAVAILABLE", "down")
        return ["from-" + str(id(self))]

    def heartbeat(self):
        self.calls += 1
        if not self.up:
            raise TrackerUnavailable(0, "UNAVAILABLE", "down")
        if self.error:
            raise TrackerError(404, self.error, "x")
        return {"ok": True}


def make(*fakes):
    client = MultiTrackerClient([f"http://t{i}" for i in range(len(fakes))])
    client.clients = list(fakes)
    return client


def test_read_fails_over_and_prefers_live_tracker():
    a, b = FakeTracker(up=False), FakeTracker()
    client = make(a, b)
    assert client.get_peers("f") == b.get_peers("f")
    b.calls = a.calls = 0
    client.get_peers("f")
    assert a.calls == 0 and b.calls == 1  # tracker chết bị tạm bỏ qua, đọc thẳng từ tracker sống


def test_write_needs_one_live_tracker():
    a, b = FakeTracker(up=False), FakeTracker()
    assert make(a, b).heartbeat() == {"ok": True}
    with pytest.raises(TrackerUnavailable):
        make(FakeTracker(up=False), FakeTracker(up=False)).heartbeat()


def test_write_reports_logical_error_from_any_tracker():
    """Một tracker vừa restart trả UNKNOWN_PEER -> báo lên để peer đăng ký lại."""
    with pytest.raises(TrackerError) as e:
        make(FakeTracker(), FakeTracker(error="UNKNOWN_PEER")).heartbeat()
    assert e.value.code == "UNKNOWN_PEER"


# ---------- test tích hợp: 2 tracker là 2 tiến trình riêng ----------
@pytest.fixture
def cluster(tmp_path):
    c = Cluster(tmp_path, trackers=2)
    c.start_tracker(0)
    c.start_tracker(1)
    yield c
    c.stop_all()


@pytest.fixture
def sample(tmp_path):
    path = tmp_path / "movie.bin"
    path.write_bytes(os.urandom(64 * 1024 * 40))
    return str(path), build_metadata(str(path), 64 * 1024, "")["file_id"]


def test_peer_state_is_replicated_to_all_trackers(cluster, sample):
    """Seeder đăng ký, đăng ký file và announce lên CẢ HAI tracker."""
    path, _ = sample
    cluster.start_peer("seed", "seed", path)
    cluster.wait_for("seed", "event=SEEDING")
    for name in ("tracker", "tracker2"):
        cluster.wait_for(name, "event=PEER_REGISTERED peer=seed")
        cluster.wait_for(name, "event=FILE_REGISTERED peer=seed")
        cluster.wait_for(name, "event=ANNOUNCE peer=seed .*have=40")


def test_download_survives_tracker_crash(cluster, sample, tmp_path):
    """Tracker chính chết giữa lúc tải -> peer chuyển sang tracker dự phòng; peer mới vẫn vào mạng được."""
    path, fid = sample
    cluster.start_peer("seed", "seed", path, extra=["--upload-delay", "0.1"])
    cluster.wait_for("seed", "event=SEEDING")
    cluster.start_peer("L1", "download", fid, "--out", str(tmp_path / "dl1"), "--exit-after-download")
    cluster.wait_for("L1", "event=CHUNK_OK", times=5)
    cluster.kill("tracker")
    cluster.start_peer("L2", "download", fid, "--out", str(tmp_path / "dl2"), "--exit-after-download")
    cluster.wait_for("L1", "event=DOWNLOAD_COMPLETE", timeout=60)
    cluster.wait_for("L2", "event=DOWNLOAD_COMPLETE", timeout=60)
    cluster.wait_for("seed", "event=TRACKER_FAILOVER", timeout=10)
    data = open(path, "rb").read()
    assert open(tmp_path / "dl1" / "movie.bin", "rb").read() == data
    assert open(tmp_path / "dl2" / "movie.bin", "rb").read() == data


def test_restarted_tracker_is_resynced(cluster, sample):
    """Tracker chết rồi chạy lại: peer phát hiện, đăng ký và announce lại lên tracker đó."""
    path, _ = sample
    cluster.start_peer("seed", "seed", path)
    cluster.wait_for("seed", "event=SEEDING")
    cluster.kill("tracker2")
    cluster.wait_for("seed", "event=TRACKER_FAILOVER", timeout=10)
    cluster.start_tracker(1)
    cluster.wait_for("seed", "event=TRACKER_BACK", timeout=15)
    cluster.wait_for("tracker2", "event=PEER_REGISTERED peer=seed", times=2, timeout=15)
    cluster.wait_for("tracker2", "event=ANNOUNCE peer=seed .*have=40", times=2, timeout=15)
