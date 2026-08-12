"""An object-store source must type its columns like the same file uploaded.

The identical CSV produced ``bigint``/``numeric``/``date`` as a file upload and
``text``/``text``/``text`` as an S3 object, and reported success either way. No
arithmetic on the amounts, no date filtering, no numeric constraints — silently.

Two things caused it. The reader handed the engine bare strings with no types,
and the introspect answered "what columns are here" with a ``string`` placeholder
per header. That placeholder then travelled as a *declared* schema through
``endpoint_source_column_types`` → ``reconcile_source_types``, which is designed
to let a declaration outrank the reader's sampled shape — correct for a
relational catalog, wrong for a store that has no catalog at all.

These tests run without any cloud: ``moto`` provides S3 in-process.
"""

from __future__ import annotations

import uuid

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from connectors.object_store_common import inferred_native_types  # noqa: E402
from src.transfer.endpoint_intelligence import _schema_from_batch  # noqa: E402

_CSV = b"id,amount,ts\n1,1000.00,2024-01-05\n2,2000.50,2024-02-11\n"


class _Batch:
    def __init__(self, headers, meta=None):
        self.headers = headers
        self.meta = meta


def test_reader_infers_types_from_the_object_rows():
    got = inferred_native_types(
        ["id", "amount", "ts"],
        [["1", "1000.00", "2024-01-05"], ["2", "2000.50", "2024-02-11"]],
    )
    assert got["id"] == "INTEGER"
    assert got["amount"].startswith("DECIMAL")
    assert got["ts"] == "DATE"


def test_schema_prefers_reader_types_over_the_placeholder():
    typed = _Batch(["id", "ts"], {"native_types": {"id": "INTEGER", "ts": "DATE"}})
    assert _schema_from_batch(typed) == {"id": "INTEGER", "ts": "DATE"}


def test_schema_keeps_the_placeholder_when_a_reader_cannot_type():
    """Readers with no type knowledge must be left exactly as they were."""
    assert _schema_from_batch(_Batch(["a", "b"], None)) == {"a": "string", "b": "string"}
    assert _schema_from_batch(_Batch(["a"], {})) == {"a": "string"}
    assert _schema_from_batch(_Batch(["a"], {"native_types": {}})) == {"a": "string"}


def test_missing_column_in_reader_types_falls_back_per_column():
    partial = _Batch(["id", "note"], {"native_types": {"id": "INTEGER"}})
    assert _schema_from_batch(partial) == {"id": "INTEGER", "note": "string"}


def test_object_store_read_reports_the_same_types_as_a_file_read():
    """End to end through moto: the S3 object and the file agree."""
    from moto import mock_aws

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="typing-bucket")
        key = f"probe/{uuid.uuid4().hex[:8]}.csv"
        s3.put_object(Bucket="typing-bucket", Key=key, Body=_CSV)

        from connectors.s3_reader import read_object

        batch = read_object(
            cfg={"database": "typing-bucket", "host": "", "port": 0},
            bucket="typing-bucket",
            key=key,
            offset=0,
            limit=50,
        )

    from services.file_parser import FileParser

    file_schema = FileParser.infer_schema(
        [dict(zip(batch.headers, row)) for row in batch.rows]
    )
    assert _schema_from_batch(batch) == file_schema
    assert file_schema["id"] == "INTEGER"
