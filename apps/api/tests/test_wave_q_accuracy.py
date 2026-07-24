"""Wave Q accuracy: ORC streaming, nested Parquet, Avro scalars, JSONL fail-closed."""

from __future__ import annotations

import io
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
fastavro = pytest.importorskip("fastavro")

from src.transfer.file_stream import (  # noqa: E402
    STREAMABLE_TYPES,
    _batch_iterator_for_type,
    peek_file_source,
    should_stream_file,
)
from src.transfer.models import EndpointConfig  # noqa: E402


def test_orc_is_streamable_and_peeks():
    pytest.importorskip("pyarrow.orc")
    import pyarrow.orc as orc

    assert "orc" in STREAMABLE_TYPES
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    buf = io.BytesIO()
    try:
        orc.write_table(table, buf)
        content = buf.getvalue()
        columns, schema, total, sample = peek_file_source(content, "data.orc")
    except OSError as exc:
        pytest.skip(f"ORC runtime unavailable in this environment: {exc}")
    assert columns == ["id", "name"]
    assert total == 2
    assert len(sample) == 2
    batches = list(_batch_iterator_for_type("orc", content, 1))
    assert sum(len(b) for b in batches) == 2
    dest = EndpointConfig(kind="database", format="postgresql", table="t")
    assert should_stream_file(content, "data.orc", dest) is True


def test_parquet_nested_list_survives_stream():
    table = pa.table({
        "id": [1, 2],
        "tags": pa.array([[1, 2], [3]], type=pa.list_(pa.int64())),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    content = buf.getvalue()
    columns, schema, total, sample = peek_file_source(content, "nested.parquet")
    assert total == 2
    assert sample[0]["tags"] == [1, 2]
    streamed = list(_batch_iterator_for_type("parquet", content, 10))
    assert streamed[0][0]["tags"] == [1, 2]


def test_avro_scalar_records_wrapped_as_value():
    schema = {"type": "string"}
    buf = io.BytesIO()
    fastavro.writer(buf, schema, ["alpha", "beta"])
    content = buf.getvalue()
    columns, _schema, total, sample = peek_file_source(content, "scalars.avro")
    assert total == 2
    assert sample == [{"value": "alpha"}, {"value": "beta"}]
    assert "value" in columns
    batches = list(_batch_iterator_for_type("avro", content, 10))
    assert batches[0] == [{"value": "alpha"}, {"value": "beta"}]


def test_jsonl_scalar_fail_closed_on_peek_and_stream():
    content = b'{"id": 1}\n42\n'
    with pytest.raises(ValueError, match="JSON object"):
        peek_file_source(content, "bad.jsonl")
    with pytest.raises(ValueError, match="JSON object"):
        list(_batch_iterator_for_type("jsonl", content, 10))


def test_detect_format_avro_orc_not_csv():
    from services.file_parser import detect_format

    assert detect_format("orders.avro", b"Obj\x01") == "avro"
    assert detect_format("orders.orc", b"ORC") == "orc"
    assert detect_format("orders.avro", b"Obj\x01") != "csv"


def test_store_upload_avro_keeps_native_format(tmp_path: Path, monkeypatch):
    from services import file_parser as fp

    monkeypatch.setattr(fp, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(fp, "_save_registry", lambda: None)
    monkeypatch.setattr("services.object_store.stage_bytes", lambda *a, **k: "s3://x")
    schema = {
        "type": "record",
        "name": "Row",
        "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}],
    }
    buf = io.BytesIO()
    fastavro.writer(buf, schema, [{"id": 1, "name": "a"}])
    record = fp.store_upload("demo.avro", buf.getvalue())
    assert record["format"] == "avro"
    names = [
        c if isinstance(c, str) else c.get("name")
        for c in record["columns"]
    ]
    assert "id" in names
    assert "name" in names
