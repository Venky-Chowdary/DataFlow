"""Object-store spill-to-disk serialize — no second full-body RAM copy."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.object_store_common import (  # noqa: E402
    serialize_object_store_body,
    serialize_object_store_export,
)
from connectors.object_store_multipart import (  # noqa: E402
    upload_object_store_bytes,
)
from connectors.writer_common import mapped_rows_to_json_records  # noqa: E402
from services.value_serializer import DF_MISSING_SENTINEL, json_default  # noqa: E402


def test_normalize_keeps_tsv_extension():
    from connectors.object_store_common import normalize_object_base_key

    assert normalize_object_base_key("exports/orders.tsv") == "exports/orders.tsv"


def test_json_export_matches_dumps_indent_and_omits_missing():
    rows = [("1", "keep", DF_MISSING_SENTINEL), ("2", "next", "x")]
    cols = ["id", "note", "extra"]
    types = {"id": "string", "note": "string", "extra": "string"}
    body, mime = serialize_object_store_body(
        key="exports/a.json", mapped_rows=rows, target_cols=cols, dest_types=types
    )
    assert mime == "application/json"
    expected = json.dumps(
        mapped_rows_to_json_records(rows, cols, types),
        indent=2,
        default=json_default,
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    assert body == expected
    parsed = json.loads(body)
    assert parsed[0] == {"id": "1", "note": "keep"}
    assert "extra" not in parsed[0]


def test_jsonl_and_csv_export_are_incremental_and_omit_missing():
    rows = [("1", DF_MISSING_SENTINEL), ("2", "ok")]
    cols = ["id", "note"]
    types = {"id": "string", "note": "string"}
    jsonl, mime = serialize_object_store_body(
        key="exports/a.jsonl", mapped_rows=rows, target_cols=cols, dest_types=types
    )
    assert mime == "application/x-ndjson"
    lines = jsonl.decode("utf-8").split("\n")
    assert json.loads(lines[0]) == {"id": "1"}
    assert json.loads(lines[1]) == {"id": "2", "note": "ok"}
    csv_body, csv_mime = serialize_object_store_body(
        key="exports/a.csv", mapped_rows=rows, target_cols=cols, dest_types=types
    )
    assert csv_mime == "text/csv"
    assert csv_body.splitlines()[0] == b"id,note"
    assert b"1," in csv_body.splitlines()[1]
    tsv_body, tsv_mime = serialize_object_store_body(
        key="exports/a.tsv", mapped_rows=rows, target_cols=cols, dest_types=types
    )
    assert tsv_mime == "text/tab-separated-values"
    assert tsv_body.splitlines()[0] == b"id\tnote"


def test_export_rolls_to_disk_above_spill_max():
    rows = [(str(i), "x" * 20) for i in range(40)]
    export = serialize_object_store_export(
        key="exports/wide.jsonl",
        mapped_rows=rows,
        target_cols=["id", "note"],
        dest_types={"id": "string", "note": "string"},
        spill_max_size=64,
    )
    try:
        assert export.spilled is True
        assert export.size > 64
        rebuilt = b"".join(chunk for _, chunk in export.iter_parts(30))
        assert rebuilt == export.read_all()
        assert rebuilt.count(b"\n") == 39
    finally:
        export.close()


def test_small_export_does_not_claim_spill():
    export = serialize_object_store_export(
        key="exports/tiny.json",
        mapped_rows=[("1", "ok")],
        target_cols=["id", "note"],
        dest_types={"id": "string", "note": "string"},
        spill_max_size=10_000,
    )
    try:
        assert export.spilled is False
        assert export.size == len(export.read_all())
    finally:
        export.close()


def test_s3_multipart_from_spilled_export():
    store: dict[str, bytes] = {}
    parts_in: dict[int, bytes] = {}

    class FakeS3:
        def create_multipart_upload(self, **_k):
            return {"UploadId": "up-spill"}

        def upload_part(self, PartNumber, Body, **_k):  # noqa: N803
            parts_in[int(PartNumber)] = bytes(Body)
            return {"ETag": f'"e{PartNumber}"'}

        def complete_multipart_upload(self, Key, MultipartUpload, **_k):  # noqa: N803
            ordered = sorted(MultipartUpload["Parts"], key=lambda p: p["PartNumber"])
            store[Key] = b"".join(parts_in[p["PartNumber"]] for p in ordered)

        def abort_multipart_upload(self, **_k):
            raise AssertionError("success must not abort")

        def put_object(self, **_k):
            raise AssertionError("spilled multipart must not PutObject")

    rows = [(str(i), "n" * 8) for i in range(12)]
    export = serialize_object_store_export(
        key="exports/big.jsonl",
        mapped_rows=rows,
        target_cols=["id", "note"],
        dest_types={"id": "string", "note": "string"},
        spill_max_size=32,
    )
    try:
        assert export.spilled
        meta = upload_object_store_bytes(
            "s3",
            client=FakeS3(),
            bucket="landing",
            key="exports/big.jsonl",
            export=export,
            threshold=40,
            part_size=25,
        )
        assert meta["method"] == "multipart"
        assert store["exports/big.jsonl"] == export.read_all()
    finally:
        export.close()


def test_parquet_spill_round_trip():
    pytest.importorskip("pyarrow.parquet")
    import pyarrow.parquet as pq

    rows = [(1, "ok"), (2, "next")]
    export = serialize_object_store_export(
        key="exports/a.parquet",
        mapped_rows=rows,
        target_cols=["id", "note"],
        dest_types={"id": "INTEGER", "note": "TEXT"},
        spill_max_size=8,
    )
    try:
        assert export.content_type == "application/vnd.apache.parquet"
        table = pq.read_table(_buffer(export.read_all()))
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 2]
    finally:
        export.close()


def _buffer(body: bytes):
    import io

    return io.BytesIO(body)
