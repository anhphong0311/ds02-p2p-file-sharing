import os

import pytest

from src.common.metadata import (MetadataError, build_metadata, chunk_length, compute_file_id, sha256_hex,
                                 validate_metadata)

CHUNK = 16 * 1024


@pytest.fixture
def sample(tmp_path):
    path = tmp_path / "sample.bin"
    path.write_bytes(os.urandom(CHUNK * 5 + 123))
    return str(path)


def test_metadata_has_chunk_hashes_and_file_hash(sample):
    """[F1] Metadata chứa đủ file_id, tên, kích thước, số chunk, hash từng chunk và hash cả file."""
    meta = build_metadata(sample, CHUNK, "http://tracker")
    data = open(sample, "rb").read()
    assert meta["num_chunks"] == 6
    assert meta["size"] == len(data)
    assert meta["chunk_hashes"][0] == sha256_hex(data[:CHUNK])
    assert meta["file_hash"] == sha256_hex(data)
    assert chunk_length(meta, 5) == 123
    assert meta["tracker"] == "http://tracker"


def test_file_id_is_deterministic(sample):
    """[F2] Hai seeder tạo metadata từ cùng một file thì có cùng file_id."""
    a = build_metadata(sample, CHUNK, "http://a")
    b = build_metadata(sample, CHUNK, "http://b")
    assert a["file_id"] == b["file_id"] == compute_file_id(a)


def test_tampered_metadata_is_rejected(sample):
    """[S1] Metadata bị sửa (hash chunk khác) không khớp file_id -> bị từ chối."""
    meta = build_metadata(sample, CHUNK, "http://t")
    meta["chunk_hashes"][0] = "0" * 64
    with pytest.raises(MetadataError):
        validate_metadata(meta)


@pytest.mark.parametrize("name", ["../evil.bin", "..", "a/b.bin", "C:\\x.bin", ""])
def test_unsafe_file_name_is_rejected(sample, name):
    """[S2] Tên file chứa đường dẫn (path traversal) bị từ chối."""
    meta = build_metadata(sample, CHUNK, "http://t")
    meta["name"] = name
    meta["file_id"] = compute_file_id(meta)
    with pytest.raises(MetadataError):
        validate_metadata(meta)


def test_invalid_sizes_are_rejected(sample):
    meta = build_metadata(sample, CHUNK, "http://t")
    meta["num_chunks"] = 999
    meta["file_id"] = compute_file_id(meta)
    with pytest.raises(MetadataError):
        validate_metadata(meta)
    with pytest.raises(MetadataError):
        build_metadata(sample, 1024, "http://t")
