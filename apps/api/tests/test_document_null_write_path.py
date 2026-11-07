"""Document writers treat reader-null as dest NULL via absent_sql_bind.

Dynamo / Elasticsearch / SQLite / JSON used to only pass Python None.
After extract emits SQL_NULL_SENTINEL, TEXT cells wrote the sentinel
spelling, ES BOOLEAN raised, ES DECIMAL invented ``\"None\"``, and a
Dynamo HASH key became the wire token. Missing stays Missing (or ES
raises so callers omit). Empty string still refuses on typed columns.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.dynamodb_writer import (  # noqa: E402
    _coerce_dynamo_cell,
    _to_dynamo_value,
)
from connectors.elasticsearch_writer import _to_es_value  # noqa: E402
from connectors.mongodb_writer import write_mapped_rows  # noqa: E402
from connectors.sqlite_writer import _to_sqlite_value  # noqa: E402
from connectors.writer_common import omit_missing_fields, to_json_value  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


_NULL_WIRES = (None, SQL_NULL_SENTINEL, "__df_ddb_null__")


def test_sqlite_reader_null_is_sql_null():
    for ddl in ("TEXT", "INTEGER", "BOOLEAN", "DECIMAL", "DATE"):
        for wire in _NULL_WIRES:
            assert _to_sqlite_value(wire, ddl) is None, (ddl, wire)
        assert _to_sqlite_value(Missing, ddl) is Missing


def test_dynamo_reader_null_is_attr_null():
    for ddl in ("S", "BOOLEAN", "DECIMAL", "INTEGER", "DATE"):
        for wire in _NULL_WIRES:
            assert _to_dynamo_value(wire, ddl) is None, (ddl, wire)
        assert _to_dynamo_value(Missing, ddl) is Missing


def test_dynamo_key_refuses_reader_null_identity():
    for wire in _NULL_WIRES:
        with pytest.raises(ValueError, match="refused"):
            _coerce_dynamo_cell(
                wire, col="pk", logical_type="TEXT", key_types={"pk": "S"}
            )
        with pytest.raises(ValueError, match="refused"):
            _coerce_dynamo_cell(
                wire, col="pk", logical_type="INTEGER", key_types={"pk": "N"}
            )


def test_elasticsearch_reader_null_is_json_null():
    for ddl in ("TEXT", "INTEGER", "BOOLEAN", "DECIMAL", "DATE", "UUID"):
        for wire in _NULL_WIRES:
            assert _to_es_value(wire, ddl) is None, (ddl, wire)
    with pytest.raises(ValueError, match="DF_MISSING"):
        _to_es_value(Missing, "TEXT")
    with pytest.raises(ValueError, match="DF_MISSING"):
        _to_es_value(DF_MISSING_SENTINEL, "BOOLEAN")


def test_json_export_reader_null_is_json_null():
    for wire in _NULL_WIRES:
        assert to_json_value(wire, "note", {"note": "string"}) is None
        assert to_json_value(wire, "amt", {"amt": "decimal"}) is None
    assert to_json_value(Missing, "note", {"note": "string"}) is None


def test_omit_missing_skips_reader_null():
    out = omit_missing_fields(
        [
            ("id", "1"),
            ("note", SQL_NULL_SENTINEL),
            ("extra", Missing),
            ("keep", "yes"),
        ]
    )
    assert out == {"id": "1", "keep": "yes"}


def test_empty_string_still_refuses_typed_document_null_invent():
    with pytest.raises(ValueError, match="empty"):
        _to_es_value("", "BOOLEAN")
    with pytest.raises(ValueError, match="empty"):
        _to_es_value("", "DECIMAL")
    with pytest.raises(ValueError, match="empty"):
        _to_dynamo_value("", "DATE")
    with pytest.raises(ValueError, match="empty"):
        _to_sqlite_value("", "BOOLEAN")


def test_zero_and_false_stay_present():
    assert _to_sqlite_value(0, "INTEGER") == 0
    assert _to_dynamo_value(False, "BOOLEAN") is False
    assert _to_es_value(False, "BOOLEAN") is False
    assert to_json_value(0, "n", {"n": "integer"}) == 0


def test_mongo_decimal_reader_null_is_bson_null_not_decimal128_none():
    captured: list = []

    class _Coll:
        def find(self, *a, **k):
            return []

        def bulk_write(self, ops, ordered=False):
            captured.extend(ops)

    class _Db:
        def __getitem__(self, name):
            return _Coll()

        def list_collection_names(self, filter=None):  # noqa: A002
            return []

    class _Client:
        def __getitem__(self, name):
            return _Db()

        def close(self):
            return None

    with patch("connectors.mongodb_common._mongo_client", return_value=_Client()):
        result = write_mapped_rows(
            host="localhost",
            port=27017,
            database="testdb",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "amt"],
            data_rows=[["1", SQL_NULL_SENTINEL]],
            mappings=[
                {"source": "id", "target": "id", "confidence": 1},
                {
                    "source": "amt",
                    "target": "amt",
                    "confidence": 1,
                    "transform": "decimal",
                },
            ],
            column_types={"id": "string", "amt": "DECIMAL"},
            create_table=True,
            write_mode="upsert",
            conflict_columns=["id"],
        )
    assert result.ok, result.error
    assert captured
    body = captured[0]._doc
    # Dense SQL NULL is BSON null on replace/set — never Decimal128("None").
    amt = body["amt"] if "amt" in body else body.get("$set", {}).get("amt")
    assert amt is None
    values = body.values() if "amt" in body else body.get("$set", {}).values()
    assert all(not hasattr(v, "to_decimal") for v in values)
