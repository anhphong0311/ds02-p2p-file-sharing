import os
import threading
import time

import pytest

from src.common.auth import issue_token, verify_token
from src.common.metadata import build_metadata
from src.tracker.state import StateError, TrackerState

CHUNK = 16 * 1024


@pytest.fixture
def meta(tmp_path):
    path = tmp_path / "f.bin"
    path.write_bytes(os.urandom(CHUNK * 10))
    return build_metadata(str(path), CHUNK, "http://t")


@pytest.fixture
def state(tmp_path, meta):
    s = TrackerState(peer_timeout=1.0, state_file=str(tmp_path / "state.json"))
    s.add_file(meta)
    return s


def test_announce_and_peer_list(state, meta):
    """[F6] Peer announce chunk -> tracker trả về đúng danh sách peer và chunk, loại trừ chính mình."""
    fid = meta["file_id"]
    state.register_peer("A", "10.0.0.1", 7001)
    state.register_peer("B", "10.0.0.2", 7002)
    state.announce("A", fid, list(range(10)), 1)
    state.announce("B", fid, [1, 3], 1)
    peers = {p["peer_id"]: p for p in state.peers_for(fid, exclude="B")}
    assert set(peers) == {"A"}
    assert peers["A"]["chunks"] == list(range(10))
    assert len(state.peers_for(fid)) == 2


def test_stale_announce_is_ignored(state, meta):
    """[C2] Announce đến trễ (seq nhỏ hơn) hoặc trùng lặp không ghi đè trạng thái mới hơn."""
    fid = meta["file_id"]
    state.register_peer("A", "h", 1)
    assert state.announce("A", fid, [0, 1, 2], seq=10)
    assert not state.announce("A", fid, [0], seq=9)       # đến trễ
    assert not state.announce("A", fid, [0, 1, 2], seq=10)  # trùng
    assert state.peers_for(fid)[0]["chunks"] == [0, 1, 2]


def test_concurrent_announces(state, meta):
    """[C3] Nhiều peer announce đồng thời, mỗi peer nhiều lần -> trạng thái cuối = announce có seq lớn nhất."""
    fid = meta["file_id"]
    for i in range(20):
        state.register_peer(f"p{i}", "h", 1000 + i)

    def worker(i):
        for seq in range(1, 51):
            state.announce(f"p{i}", fid, list(range(seq % 10 + 1)), seq)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    peers = state.peers_for(fid)
    assert len(peers) == 20
    assert all(p["chunks"] == list(range(50 % 10 + 1)) for p in peers)


def test_peer_id_impersonation_rejected(state):
    """[S5] Không thể đăng ký trùng Peer ID của peer đang online nếu không có token của peer đó."""
    state.register_peer("A", "h", 1)
    with pytest.raises(StateError) as e:
        state.register_peer("A", "evil", 2)
    assert e.value.code == "PEER_ID_IN_USE"
    state.register_peer("A", "h", 1, auth_peer_id="A")  # chính chủ đăng ký lại được


def test_forged_or_expired_token_rejected():
    """[S6] Token giả, bị sửa hoặc hết hạn đều bị từ chối."""
    token = issue_token("secret", "A", 60)
    assert verify_token("secret", token) == "A"
    assert verify_token("other-secret", token) is None
    forged = token.replace("A.", "B.", 1)
    assert verify_token("secret", forged) is None
    assert verify_token("secret", issue_token("secret", "A", -1)) is None
    assert verify_token("secret", "garbage") is None


def test_dead_peer_expires(state, meta):
    """[R2] Peer ngừng heartbeat bị xóa khỏi tracker sau timeout."""
    fid = meta["file_id"]
    state.register_peer("A", "h", 1)
    state.register_peer("B", "h", 2)
    state.announce("A", fid, [0], 1)
    state.announce("B", fid, [0], 1)
    time.sleep(0.6)
    state.heartbeat("B")
    time.sleep(0.6)
    assert state.expire_peers() == ["A"]
    assert [p["peer_id"] for p in state.peers_for(fid)] == ["B"]


def test_metadata_survives_restart(tmp_path, state, meta):
    """Metadata được checkpoint xuống đĩa và nạp lại khi tracker khởi động lại."""
    again = TrackerState(peer_timeout=1.0, state_file=str(tmp_path / "state.json"))
    assert again.get_file(meta["file_id"])["name"] == "f.bin"
