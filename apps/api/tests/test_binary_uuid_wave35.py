"""Wave 35: Dynamo/Mongo binary fail-closed + UUID bind/Gate-8 parity."""

from __future__ import annotations

import base64
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_uuid_wire_accepts_braces_and_hex():
    from connectors.sql_bind import coerce_uuid_wire

    canonical = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    assert coerce_uuid_wire(canonical.upper()) == canonical
    assert coerce_uuid_wire("{" + canonical.upper() + "}") == canonical
    assert coerce_uuid_wire("urn:uuid:" + canonical) == canonical
    assert coerce_uuid_wire(canonical.replace("-", "")) == canonical
    with pytest.raises(ValueError):
        coerce_uuid_wire("not-a-uuid")


def test_normalize_sql_bind_uuid_route():
    import uuid as _uuid

    from connectors.sql_bind import normalize_sql_bind_value

    raw = "{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}"
    canonical = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    # Generic UUID carriers bind as the canonical lowercase string.
    assert normalize_sql_bind_value(raw, "UUID") == canonical
    # pyodbc UNIQUEIDENTIFIER prefers a native uuid.UUID — string params often
    # raise ODBC 8169 (Microsoft / FreeTDS class). Keep the domain typed.
    mssql = normalize_sql_bind_value(raw, "UNIQUEIDENTIFIER", engine="mssql")
    assert isinstance(mssql, _uuid.UUID)
    assert str(mssql) == canonical
    # Non-MSSQL engines with a UNIQUEIDENTIFIER carrier still get the string.
    assert normalize_sql_bind_value(raw, "UNIQUEIDENTIFIER", engine="postgresql") == canonical


def test_normalize_cell_uuid_braces_and_hex():
    from services.reconciliation import normalize_cell

    braced = "{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}"
    hex32 = "A1B2C3D4E5F67890ABCDEF1234567890"
    expected = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    assert normalize_cell(braced, ddl_type="UUID") == expected
    assert normalize_cell(hex32, ddl_type="UNIQUEIDENTIFIER") == expected


def test_dynamodb_binary_refuses_utf8_invent():
    from connectors.dynamodb_writer import _to_dynamo_value

    valid = base64.b64encode(b"\x00\x01\xff").decode("ascii")
    assert _to_dynamo_value(valid, "BINARY") == b"\x00\x01\xff"
    with pytest.raises(ValueError, match="base64"):
        _to_dynamo_value("not-valid-base64!!!", "BYTEA")


def test_mongodb_binary_refuses_utf8_invent():
    from bson.binary import Binary

    # Exercise the same coerce path Mongo uses.
    from connectors.sql_bind import coerce_binary_wire

    with pytest.raises(ValueError, match="base64"):
        coerce_binary_wire("not-valid-base64!!!")
    raw = coerce_binary_wire(base64.b64encode(b"abc").decode("ascii"))
    assert Binary(raw) == Binary(b"abc")


def test_mongodb_uuid_canonical():
    from connectors.sql_bind import coerce_uuid_wire

    assert (
        coerce_uuid_wire("{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}")
        == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    )
