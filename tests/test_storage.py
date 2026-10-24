import os
import threading

import pytest

from src.common.metadata import build_metadata
from src.peer.storage import FileStore, IntegrityError

CHUNK = 16 * 1024


@pytest.fixture
def setup(tmp_path):
    src = tmp_path / "f.bin"
    data = os.urandom(CHUNK * 20 + 777)
    src.write_bytes(data)
    meta = build_metadata(str(src), CHUNK, "http://t")
    chunks = [data[i:i + CHUNK] for i in range(0, len(data), CHUNK)]
    return tmp_path, meta, data, chunks


def test_corrupt_chunk_is_rejected(setup):
    """[F4] Chunk sai hash bị từ chối và không được đánh dấu là đã có."""
    tmp, meta, _, chunks = setup
    store = FileStore(str(tmp / "peer"), meta)
    bad = b"\xff" + chunks[2][1:]
    with pytest.raises(IntegrityError):
        store.write_chunk(2, bad)
    assert not store.has(2)
    store.write_chunk(2, chunks[2])
    assert store.has(2)


def test_assemble_out_of_order_and_verify(setup):
    """[F5] Chunk đến không theo thứ tự vẫn được ghép đúng thứ tự, hash cả file được kiểm tra."""
    tmp, meta, data, chunks = setup
    store = FileStore(str(tmp / "peer"), meta)
    for i in reversed(range(len(chunks))):
        store.write_chunk(i, chunks[i])
    out = store.assemble(str(tmp / "out" / "f.bin"))
    assert open(out, "rb").read() == data


def test_resume_after_restart(setup):
    """[R1] Sau khi restart, peer quét lại chunk đã lưu; chunk hỏng/dở dang bị loại bỏ."""
    tmp, meta, _, chunks = setup
    root = str(tmp / "peer")
    store = FileStore(root, meta)
    for i in (0, 1, 5):
        store.write_chunk(i, chunks[i])
    # mô phỏng file chunk bị hỏng trên đĩa và file tạm còn sót
    with open(os.path.join(store.chunks_dir, "5.chunk"), "wb") as f:
        f.write(b"garbage")
    open(os.path.join(store.chunks_dir, "7.chunk.123.tmp"), "wb").close()

    reopened = FileStore.load_all(root)[0]
    assert reopened.have_list() == [0, 1]
    assert len(reopened.missing()) == meta["num_chunks"] - 2


def test_concurrent_chunk_writes(setup):
    """[C1] Nhiều luồng ghi chunk đồng thời -> trạng thái cuối nhất quán, file ghép đúng."""
    tmp, meta, data, chunks = setup
    store = FileStore(str(tmp / "peer"), meta)
    threads = [threading.Thread(target=store.write_chunk, args=(i, c)) for i, c in enumerate(chunks)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert store.is_complete()
    assert open(store.assemble(str(tmp / "o.bin")), "rb").read() == data
