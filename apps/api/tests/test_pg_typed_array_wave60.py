"""Wave 60: PG typed arrays preserve element carriers + element bind SSOT."""

from __future__ import annotations

import sys
from ipaddress import IPv4Address
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_pg_array_elem_specialty_introspect():
    from services.schema_introspect import _pg_to_logical

    assert _pg_to_logical("inet[]") == "ARRAY<INET>"
    assert _pg_to_logical("uuid[]") == "ARRAY<UUID>"
    assert _pg_to_logical("point[]") == "ARRAY<POINT>"
    assert _pg_to_logical("pg_lsn[]") == "ARRAY<PG_LSN>"
    assert _pg_to_logical("jsonb[]") == "ARRAY<JSONB>"
    assert _pg_to_logical("macaddr[]") == "ARRAY<MACADDR>"
    # Baseline integers still typed.
    assert _pg_to_logical("integer[]") == "ARRAY<INTEGER>"


def test_coerce_array_wire_normalizes_specialty_elems():
    from connectors.sql_bind import coerce_array_wire, normalize_sql_bind_value

    got = coerce_array_wire(
        ["10.0.0.1", "10.0.0.2"],
        engine="postgresql",
        ddl_type="ARRAY<INET>",
    )
    assert got == [IPv4Address("10.0.0.1"), IPv4Address("10.0.0.2")]

    got_lsn = normalize_sql_bind_value(
        '["16/b374d848","0/1"]',
        "ARRAY<PG_LSN>",
        engine="postgresql",
    )
    assert got_lsn == ["16/B374D848", "0/00000001"]

    with pytest.raises(ValueError, match="refuse invent"):
        coerce_array_wire(
            ["not-an-inet"],
            engine="postgresql",
            ddl_type="ARRAY<INET>",
        )
