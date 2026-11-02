"""Wave 36: ES/Redis Gate-8 + UUID/binary probe + refuse UTF-8 invent."""

from __future__ import annotations

import base64
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_verify_target_routes_elasticsearch():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_elasticsearch_index",
        return_value=(3, "es"),
    ) as mocked:
        assert verify_target(
            "elasticsearch",
            {"host": "localhost", "port": 9200},
            schema="",
            table_name="orders",
            fallback_rows=-1,
            fallback_checksum="",
        ) == (3, "es")
        assert mocked.called


def test_read_target_sample_routes_elasticsearch():
    from connectors.base import ReadBatch
    from services.reconciliation import read_target_sample

    batch = ReadBatch(
        headers=["_id", "email"],
        rows=[["doc-1", "a@x.com"]],
        offset=0,
        total_rows=1,
    )
    with patch(
        "connectors.elasticsearch_reader.read_index_batch",
        return_value=(batch, None),
    ):
        rows = read_target_sample(
            "elasticsearch",
            {"host": "localhost", "port": 9200},
            schema="",
            table_name="contacts",
            columns=["_id", "email"],
            limit=10,
        )
    assert rows == [{"_id": "doc-1", "email": "a@x.com"}]


def test_elasticsearch_to_es_value_uuid_and_binary():
    from connectors.elasticsearch_writer import _to_es_value

    assert (
        _to_es_value("{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}", "UUID")
        == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    )
    raw = base64.b64encode(b"\x00\xff").decode("ascii")
    assert _to_es_value(raw, "BYTEA") == raw
    with pytest.raises(ValueError, match="base64"):
        _to_es_value("not-valid-base64!!!", "BINARY")


def test_redis_normalize_typed_doc_uuid_binary():
    from connectors.redis_writer import _normalize_redis_typed_doc

    raw = base64.b64encode(b"abc").decode("ascii")
    out = _normalize_redis_typed_doc(
        {
            "id": "{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}",
            "blob": raw,
            "flag": "true",
        },
        ["id", "blob", "flag"],
        ["UUID", "BYTEA", "BOOLEAN"],
    )
    assert out["id"] == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    assert out["blob"] == raw
    assert out["flag"] is True
    with pytest.raises(ValueError, match="base64"):
        _normalize_redis_typed_doc(
            {"blob": "!!!"},
            ["blob"],
            ["BINARY"],
        )


def test_gate8_writer_meta_on_es_redis_success_shape():
    from connectors.writer_common import gate8_writer_meta

    meta = gate8_writer_meta([{"_id": "1", "n": 1}], ["_id", "n"], ["1"])
    assert meta["source_row_count"] == 1
    assert meta["written_ids"] == ["1"]
    assert meta["reconcile_sample"][0]["_id"] == "1"


def test_postgres_bytea_refuses_utf8_invent():
    from connectors.sql_bind import coerce_binary_wire

    with pytest.raises(ValueError, match="base64"):
        coerce_binary_wire("hello-not-b64")
    assert coerce_binary_wire(base64.b64encode(b"ok").decode("ascii")) == b"ok"


def test_iceberg_arrow_binary_refuses_utf8_invent():
    pytest.importorskip("pyarrow")
    import pyarrow as pa

    from connectors.iceberg_writer import _coerce_arrow_cell

    with pytest.raises(ValueError, match="base64"):
        _coerce_arrow_cell("not-valid-base64!!!", pa.binary(), pa)
    raw = base64.b64encode(b"\x01\x02").decode("ascii")
    assert _coerce_arrow_cell(raw, pa.binary(), pa) == b"\x01\x02"
    assert _coerce_arrow_cell(b"\x01\x02", pa.large_binary(), pa) == b"\x01\x02"


def test_coercion_probe_blocks_invalid_uuid_and_binary_wire():
    from services.coercion_probe import analyze_coercion

    uuid_report = analyze_coercion(
        sample_rows=[{"id": "not-a-uuid"}],
        mappings=[
            {
                "source": "id",
                "target": "id",
                "source_type": "TEXT",
                "target_type": "UUID",
            }
        ],
        source_types={"id": "TEXT"},
        dest_types={"id": "UUID"},
        dest_db_type="postgresql",
        table_exists=True,
    )
    uuid_col = (uuid_report.get("columns") or [None])[0]
    assert uuid_col is not None
    assert uuid_col["failed"] >= 1
    assert uuid_col["severity"] == "block"

    bin_report = analyze_coercion(
        sample_rows=[{"blob": "!!!not-b64!!!"}],
        mappings=[
            {
                "source": "blob",
                "target": "blob",
                "source_type": "TEXT",
                "target_type": "BYTEA",
            }
        ],
        source_types={"blob": "TEXT"},
        dest_types={"blob": "BYTEA"},
        dest_db_type="postgresql",
        table_exists=True,
    )
    bin_col = (bin_report.get("columns") or [None])[0]
    assert bin_col is not None
    assert bin_col["failed"] >= 1
    assert bin_col["severity"] == "block"


def test_coercion_probe_accepts_canonical_uuid_and_base64_binary():
    from services.coercion_probe import analyze_coercion

    uid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    b64 = base64.b64encode(b"payload").decode("ascii")
    report = analyze_coercion(
        sample_rows=[{"id": "{" + uid.upper() + "}", "blob": b64}],
        mappings=[
            {
                "source": "id",
                "target": "id",
                "source_type": "TEXT",
                "target_type": "UUID",
            },
            {
                "source": "blob",
                "target": "blob",
                "source_type": "TEXT",
                "target_type": "BYTEA",
            },
        ],
        source_types={"id": "TEXT", "blob": "TEXT"},
        dest_types={"id": "UUID", "blob": "BYTEA"},
        dest_db_type="postgresql",
        table_exists=True,
    )
    by_target = {c["target"]: c for c in (report.get("columns") or [])}
    assert by_target["id"]["failed"] == 0
    assert by_target["id"]["ok"] >= 1
    assert by_target["blob"]["failed"] == 0
    assert by_target["blob"]["ok"] >= 1
