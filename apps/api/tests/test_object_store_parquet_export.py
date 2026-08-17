"""Object-store Parquet export — typed write + Gate-8 read-back (no cloud creds)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import pytest

from connectors.object_store_common import (
    normalize_object_base_key,
    object_store_read_keys,
    purge_object_store_parts,
    resolve_object_write_key,
    serialize_object_store_body,
)


def test_normalize_keeps_parquet_extension():
    assert normalize_object_base_key("exports/orders.parquet") == "exports/orders.parquet"
    assert normalize_object_base_key("exports/orders").endswith("/export.json")


def test_multi_chunk_parquet_emits_part_keys():
    base = normalize_object_base_key("orders/export.parquet")
    k1, prefix = resolve_object_write_key(base, file_batch_idx=1, total_chunks=2)
    k2, prefix2 = resolve_object_write_key(base, file_batch_idx=2, total_chunks=2)
    assert k1.endswith("part-00001.parquet")
    assert k2.endswith("part-00002.parquet")
    assert prefix == prefix2 == "orders/export/"


def test_read_keys_and_purge_recognize_parquet_parts():
    listed = [
        "orders/export/part-00002.parquet",
        "orders/export/part-00001.parquet",
        "orders/export/readme.txt",
    ]
    keys = object_store_read_keys("orders/export.parquet", listed)
    assert keys == [
        "orders/export/part-00001.parquet",
        "orders/export/part-00002.parquet",
    ]
    store = {k: b"x" for k in listed}
    removed = purge_object_store_parts(
        list_keys=lambda prefix: [k for k in store if k.startswith(prefix)],
        delete_key=store.pop,
        parts_prefix="orders/export/",
        keep_part_count=1,
    )
    assert "orders/export/part-00002.parquet" in removed
    assert "orders/export/part-00001.parquet" in store
    assert "orders/export/readme.txt" in store


def test_serialize_parquet_round_trip_typed_columns():
    pytest.importorskip("pyarrow.parquet")
    import pyarrow.parquet as pq

    rows = [
        (1, Decimal("12.50"), True, date(2026, 8, 14), datetime(2026, 8, 14, 12, 0, 0), "ok"),
        (2, Decimal("0.01"), False, date(2026, 8, 15), datetime(2026, 8, 15, 8, 30, 0), "next"),
    ]
    cols = ["id", "amount", "active", "sold_on", "updated_at", "note"]
    dest_types = {
        "id": "INTEGER",
        "amount": "DECIMAL(10,2)",
        "active": "BOOLEAN",
        "sold_on": "DATE",
        "updated_at": "TIMESTAMP",
        "note": "VARCHAR(32)",
    }
    body, mime = serialize_object_store_body(
        key="exports/orders.parquet",
        mapped_rows=rows,
        target_cols=cols,
        dest_types=dest_types,
    )
    assert mime == "application/vnd.apache.parquet"
    table = pq.read_table(pa_buffer(body))
    assert table.num_rows == 2
    assert table.column("id").to_pylist() == [1, 2]
    assert table.column("amount").to_pylist() == [Decimal("12.50"), Decimal("0.01")]
    assert table.column("active").to_pylist() == [True, False]
    assert table.column("note").to_pylist() == ["ok", "next"]


def pa_buffer(body: bytes):
    import io

    return io.BytesIO(body)


def test_serialize_json_default_unchanged():
    body, mime = serialize_object_store_body(
        key="exports/orders.json",
        mapped_rows=[(1, "a")],
        target_cols=["id", "name"],
        dest_types={"id": "INTEGER", "name": "TEXT"},
    )
    assert mime == "application/json"
    assert b'"id"' in body


def test_parquet_oversize_decimal_does_not_clamp_to_decimal128():
    pytest.importorskip("pyarrow")
    import pyarrow as pa

    from services.arrow_write import logical_to_arrow_type

    t = logical_to_arrow_type("DECIMAL(40,10)", pa, dialect="parquet")
    assert pa.types.is_large_string(t)


def test_s3_writer_parquet_puts_typed_object(monkeypatch):
    pytest.importorskip("pyarrow.parquet")
    import pyarrow.parquet as pq
    from connectors import s3_writer

    puts: list[tuple[str, bytes, str]] = []

    class FakeClient:
        def head_bucket(self, Bucket):  # noqa: N803
            return {}

        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            puts.append((Key, Body, ContentType))

        def delete_object(self, Bucket, Key):  # noqa: N803
            return {}

    monkeypatch.setattr(s3_writer, "boto3_client", lambda *a, **k: FakeClient())
    monkeypatch.setattr(s3_writer, "_ensure_bucket", lambda *a, **k: None)
    monkeypatch.setattr("connectors.s3_reader.list_objects", lambda *a, **k: [])

    result = s3_writer.write_mapped_rows(
        host="",
        port=0,
        database="landing",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="exports/orders.parquet",
        headers=["id", "amount"],
        data_rows=[["1", "10.25"], ["2", "0.50"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "amount", "target": "amount", "target_type": "DECIMAL(10,2)"},
        ],
        column_types={"id": "INTEGER", "amount": "DECIMAL(10,2)"},
        create_table=True,
    )
    assert result.ok, result.error
    assert result.rows_written == 2
    live = [p for p in puts if p[0] == "exports/orders.parquet"]
    assert live
    key, body, content_type = live[0]
    assert content_type == "application/vnd.apache.parquet"
    table = pq.read_table(pa_buffer(body))
    assert table.num_rows == 2
    assert table.column("amount").to_pylist() == [Decimal("10.25"), Decimal("0.50")]


def test_s3_writer_parquet_quarantines_overflow_before_write(monkeypatch):
    pytest.importorskip("pyarrow.parquet")
    from connectors import s3_writer

    puts: list[str] = []

    class FakeClient:
        def head_bucket(self, Bucket):  # noqa: N803
            return {}

        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            puts.append(Key)

        def delete_object(self, Bucket, Key):  # noqa: N803
            return {}

    monkeypatch.setattr(s3_writer, "boto3_client", lambda *a, **k: FakeClient())
    monkeypatch.setattr(s3_writer, "_ensure_bucket", lambda *a, **k: None)
    monkeypatch.setattr("connectors.s3_reader.list_objects", lambda *a, **k: [])

    result = s3_writer.write_mapped_rows(
        host="",
        port=0,
        database="landing",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="exports/orders.parquet",
        headers=["id", "amount"],
        data_rows=[["1", "999999999.99"], ["2", "1.50"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "amount", "target": "amount", "target_type": "DECIMAL(4,2)"},
        ],
        column_types={"id": "INTEGER", "amount": "DECIMAL(4,2)"},
        create_table=True,
        error_policy="quarantine",
    )
    assert result.ok, result.error
    assert result.rows_written == 1
    assert result.rejected_rows >= 1
    assert any(k == "exports/orders.parquet" for k in puts)


def test_gcs_and_adls_serialize_share_s3_parquet_bytes():
    pytest.importorskip("pyarrow.parquet")
    rows = [(1, "ok")]
    cols = ["id", "note"]
    types = {"id": "INTEGER", "note": "TEXT"}
    s3_body, s3_mime = serialize_object_store_body(
        key="a.parquet", mapped_rows=rows, target_cols=cols, dest_types=types
    )
    gcs_body, gcs_mime = serialize_object_store_body(
        key="b.parquet", mapped_rows=rows, target_cols=cols, dest_types=types
    )
    assert s3_mime == gcs_mime == "application/vnd.apache.parquet"
    assert s3_body == gcs_body
