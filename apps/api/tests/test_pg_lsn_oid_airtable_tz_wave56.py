"""Wave 56: PostgreSQL pg_lsn / oid bind + Airtable createdTime TIMESTAMPTZ."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_pg_lsn_wire():
    from connectors.sql_bind import coerce_pg_lsn_wire, normalize_sql_bind_value

    # Postgres out format: %X/%08X
    assert coerce_pg_lsn_wire("16/B374D848") == "16/B374D848"
    assert coerce_pg_lsn_wire("16/b374d848") == "16/B374D848"
    assert coerce_pg_lsn_wire("0/0") == "0/00000000"
    # uint64 encoding of 0x16 << 32 | 0xB374D848
    raw = (0x16 << 32) | 0xB374D848
    assert coerce_pg_lsn_wire(raw) == "16/B374D848"
    assert coerce_pg_lsn_wire(str(raw)) == "16/B374D848"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_pg_lsn_wire(True)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_pg_lsn_wire("not-an-lsn")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_pg_lsn_wire("16/GGGGGGGG")
    assert normalize_sql_bind_value("1/2", "PG_LSN") == "1/00000002"
    assert normalize_sql_bind_value("A/B", "LSN") == "A/0000000B"


def test_coerce_oid_wire():
    from connectors.sql_bind import coerce_oid_wire, normalize_sql_bind_value

    assert coerce_oid_wire(564182) == 564182
    assert coerce_oid_wire("564182") == 564182
    assert coerce_oid_wire(0) == 0
    assert coerce_oid_wire(0xFFFFFFFF) == 0xFFFFFFFF
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_oid_wire(True)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_oid_wire(-1)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_oid_wire(0x100000000)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_oid_wire("pg_class")  # symbolic names need live catalog — refuse invent
    assert normalize_sql_bind_value("42", "OID") == 42


def test_airtable_created_last_modified_timestamptz():
    from connectors.airtable_writer import airtable_field_to_carrier

    assert airtable_field_to_carrier({"type": "createdTime"}) == "TIMESTAMPTZ"
    assert airtable_field_to_carrier({"type": "lastModifiedTime"}) == "TIMESTAMPTZ"
    assert airtable_field_to_carrier({"type": "dateTime"}) == "TIMESTAMPTZ"
