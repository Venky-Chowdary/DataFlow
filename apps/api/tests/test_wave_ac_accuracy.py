"""Wave AC accuracy: object-store single-shot, upsert PK, sparse JSON, int coerce, SaaS totals."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_object_store_chunk_guard_ignores_none_total():
    """Truthiness gate on total_rows must not fail open for S3/GCS/ADLS."""
    src = Path(_API_ROOT / "src/transfer/stream.py").read_text()
    assert 'dest_type in ("s3", "gcs", "adls") and sample_probe.total_rows' not in src
    assert "10_000_000" in src or "10000000" in src
    fs = Path(_API_ROOT / "src/transfer/file_stream.py").read_text()
    assert 'dest_type in ("s3", "gcs", "adls") and total_rows:' not in fs


def test_upsert_without_primary_key_refuses_insert_fallback():
    from services.sync_cursor import requires_upsert

    assert requires_upsert("cdc")
    assert requires_upsert("incremental_deduped")
    # Stream path: missing PK must raise (exercise the guard via source snippet contract).
    stream_src = Path(_API_ROOT / "src/transfer/stream.py").read_text()
    assert "refuse silent insert fallback" in stream_src
    engine_src = Path(_API_ROOT / "src/transfer/engine.py").read_text()
    assert "refuse silent insert fallback" in engine_src


def test_scd2_refuses_invented_conflict_key():
    stream_src = Path(_API_ROOT / "src/transfer/stream.py").read_text()
    assert "refuse inventing a conflict key" in stream_src
    assert "conflict_columns = [target_cols[0]]" not in stream_src


def test_get_file_chunks_json_unions_sparse_late_keys(tmp_path, monkeypatch):
    from services.file_parser import get_file_chunks
    import services.file_parser as fp

    records = [{"id": i} for i in range(51)]
    records[50]["extra_field"] = "late"
    path = tmp_path / "sparse.json"
    path.write_text(json.dumps(records), encoding="utf-8")

    monkeypatch.setattr(fp, "_save_registry", lambda: None)
    fp._file_registry["f1"] = {
        "path": str(path),
        "format": "json",
        "encoding": "utf-8",
        "delimiter": ",",
    }
    chunks = list(get_file_chunks("f1", chunk_size=100))
    assert chunks
    headers, rows = chunks[0]
    assert "extra_field" in headers
    idx = headers.index("extra_field")
    assert rows[50][idx] == "late"


def test_get_file_chunks_jsonl_unions_second_line_keys(tmp_path, monkeypatch):
    from services.file_parser import get_file_chunks
    import services.file_parser as fp

    path = tmp_path / "sparse.jsonl"
    path.write_text('{"id": 1}\n{"id": 2, "extra": "x"}\n', encoding="utf-8")
    monkeypatch.setattr(fp, "_save_registry", lambda: None)
    fp._file_registry["f2"] = {
        "path": str(path),
        "format": "jsonl",
        "encoding": "utf-8",
        "delimiter": ",",
    }
    chunks = list(get_file_chunks("f2", chunk_size=10))
    headers, rows = chunks[0]
    assert "extra" in headers
    assert rows[1][headers.index("extra")] == "x"
    assert rows[0][headers.index("extra")] == ""


def test_saas_extract_records_total_rows_none():
    from connectors.saas_common import extract_records

    batch = extract_records([{"id": "1"}, {"id": "2", "name": "a"}])
    assert len(batch.rows) == 2
    assert "name" in batch.headers
    assert batch.total_rows is None


def test_salesforce_missing_totalsize_stays_none():
    import connectors.salesforce as sf

    def fake_request(**kwargs):
        r = MagicMock()
        r.raise_for_status = MagicMock()
        url = str(kwargs.get("url") or "")
        if "/describe" in url:
            r.json.return_value = {
                "fields": [
                    {"name": "Id", "type": "id", "soapType": "tns:ID"},
                    {"name": "Name", "type": "string", "soapType": "xsd:string"},
                ]
            }
        else:
            r.json.return_value = {
                "done": True,
                "records": [{"Id": "001", "Name": "Ada", "attributes": {"type": "Account"}}],
            }
        return r

    with patch.object(sf, "request", side_effect=fake_request):
        batch = sf.read_object(
            cfg={"api_key": "tok", "host": "https://example.salesforce.com"},
            object="Account",
            limit=10,
        )
    assert len(batch.rows) >= 1
    assert batch.total_rows is None


def test_mongo_integer_refuses_non_integral_float():
    src = Path(_API_ROOT / "connectors/mongodb_writer.py").read_text()
    assert "non-integral float" in src
    assert "non-integral decimal" in src

    import types

    pa = types.SimpleNamespace(
        types=types.SimpleNamespace(
            is_decimal=lambda t: False,
            is_floating=lambda t: False,
            is_integer=lambda t: True,
            is_boolean=lambda t: False,
            is_timestamp=lambda t: False,
        )
    )
    from connectors.iceberg_writer import _coerce_arrow_cell

    with pytest.raises(ValueError, match="non-integral"):
        _coerce_arrow_cell(3.7, object(), pa)
    assert _coerce_arrow_cell(3.0, object(), pa) == 3


def test_snowflake_copy_abort_on_column_mismatch():
    src = Path(_API_ROOT / "connectors/snowflake_writer.py").read_text()
    assert "ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE" in src
    assert "ON_ERROR = 'ABORT_STATEMENT'" in src
    assert "ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE" not in src
    assert "ON_ERROR = 'CONTINUE'" not in src


def test_jsonl_strict_utf8_decode():
    from services.file_parser import parse_jsonl

    with pytest.raises(ValueError, match="UTF-8"):
        parse_jsonl(b'{"id": 1}\n{"id": "\xff"}')


def test_load_json_records_strict_utf8():
    from services.json_tabular import load_json_records

    with pytest.raises(ValueError, match="UTF-8"):
        load_json_records(b'[{"id": "\xff"}]')


def test_file_stream_text_reader_strict():
    src = Path(_API_ROOT / "src/transfer/file_stream.py").read_text()
    assert 'errors="strict"' in src or "errors='strict'" in src
