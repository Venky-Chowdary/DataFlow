"""Iceberg v3 deletion-vector codec — roaring portable + Puffin framing.

Proves the dest-engine reader against the published Puffin / Roaring
specs, not a placeholder. Fail-closed cases stay unmeasured.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.iceberg_deletion_vector import (  # noqa: E402
    IcebergDeletionVectorError,
    decode_deletion_vector_blob,
    decode_roaring32,
    encode_deletion_vector_blob,
    encode_roaring32,
    encode_roaring64,
    read_deletion_vector,
    write_puffin_deletion_vector,
)
from connectors.iceberg_mor import inspect_delete_refs  # noqa: E402


def test_deletion_vector_blob_roundtrip_sparse_positions():
    positions = {0, 1, 5, 4095}
    blob = encode_deletion_vector_blob(positions)
    assert decode_deletion_vector_blob(blob) == positions


def test_deletion_vector_blob_crc_mismatch_is_unmeasured():
    blob = bytearray(encode_deletion_vector_blob({3}))
    blob[-1] ^= 0xFF
    with pytest.raises(IcebergDeletionVectorError, match="CRC-32"):
        decode_deletion_vector_blob(bytes(blob))


def test_deletion_vector_blob_bad_magic_is_unmeasured():
    blob = bytearray(encode_deletion_vector_blob({1}))
    blob[4:8] = b"XXXX"
    with pytest.raises(IcebergDeletionVectorError, match="magic"):
        decode_deletion_vector_blob(bytes(blob))


def test_roaring32_run_container_roundtrip():
    values = list(range(10, 15))
    encoded = encode_roaring32(values, use_runs=True)
    assert decode_roaring32(encoded) == set(values)


def test_roaring32_bitmap_container_roundtrip():
    values = list(range(4097))
    encoded = encode_roaring32(values)
    assert decode_roaring32(encoded) == set(values)


def test_roaring64_high_key_position():
    pos = (3 << 32) | 9
    encoded = encode_roaring64({pos, 2})
    from connectors.iceberg_deletion_vector import decode_roaring64

    assert decode_roaring64(encoded) == {pos, 2}


def test_puffin_write_read_matches_footer_cardinality(tmp_path: Path):
    path = tmp_path / "deletes.puffin"
    offset, size = write_puffin_deletion_vector(
        path, {1, 3}, referenced_data_file="data/part-0.parquet"
    )
    blob = read_deletion_vector(
        path,
        offset=offset,
        size=size,
        referenced_data_file="data/part-0.parquet",
        record_count=2,
    )
    assert blob.positions == frozenset({1, 3})
    assert blob.referenced_data_file == "data/part-0.parquet"
    assert blob.cardinality == 2


def test_puffin_footer_disagrees_with_manifest_is_unmeasured(tmp_path: Path):
    path = tmp_path / "deletes.puffin"
    offset, size = write_puffin_deletion_vector(
        path, {1}, referenced_data_file="data/part-0.parquet"
    )
    with pytest.raises(IcebergDeletionVectorError, match="referenced-data-file"):
        read_deletion_vector(
            path,
            offset=offset,
            size=size,
            referenced_data_file="data/other.parquet",
            record_count=1,
        )


def test_inspect_delete_refs_passes_deletion_vector_fields():
    class _Col:
        def __init__(self, values: list) -> None:
            self._values = values

        def to_pylist(self) -> list:
            return list(self._values)

    class _Deletes:
        num_rows = 1

        def column(self, name: str) -> _Col:
            mapping = {
                "file_path": ["s3://lake/wh/deletes.puffin"],
                "content": [3],
                "file_format": ["PUFFIN"],
                "content_offset": [4],
                "content_size_in_bytes": [40],
                "referenced_data_file": ["s3://lake/wh/part-0.parquet"],
                "record_count": [2],
                "equality_ids": [None],
            }
            if name not in mapping:
                raise KeyError(name)
            return _Col(mapping[name])

    refs = inspect_delete_refs(_Deletes())
    assert refs is not None
    assert refs[0]["content"] == 3
    assert refs[0]["content_offset"] == 4
    assert refs[0]["content_size_in_bytes"] == 40
    assert refs[0]["referenced_data_file"] == "s3://lake/wh/part-0.parquet"
    assert refs[0]["record_count"] == 2
