"""Wave 59: txid_snapshot bind + generic_sql specialty SSOT wiring."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_txid_snapshot_wire():
    from connectors.sql_bind import (
        coerce_txid_snapshot_wire,
        normalize_sql_bind_value,
    )

    assert coerce_txid_snapshot_wire("10:20:10,14,15") == "10:20:10,14,15"
    assert coerce_txid_snapshot_wire("100:100:") == "100:100:"
    # Canonicalize xip order + uniqueness.
    assert coerce_txid_snapshot_wire("10:20:15,10,14,10") == "10:20:10,14,15"
    assert coerce_txid_snapshot_wire(
        {"xmin": 10, "xmax": 20, "xip": [15, 10]}
    ) == "10:20:10,15"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_txid_snapshot_wire(True)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_txid_snapshot_wire("10:5:10")  # xmin > xmax
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_txid_snapshot_wire("10:20:25")  # xip out of range
    assert normalize_sql_bind_value("1:2:", "TXID_SNAPSHOT") == "1:2:"
    assert normalize_sql_bind_value("1:2:1", "PG_SNAPSHOT") == "1:2:1"


def test_txid_snapshot_introspect_and_ddl():
    from services.schema_introspect import _pg_to_logical
    from services.type_system import ddl_type

    assert _pg_to_logical("txid_snapshot") == "TXID_SNAPSHOT"
    assert _pg_to_logical("pg_snapshot") == "PG_SNAPSHOT"
    assert ddl_type("postgresql", "TXID_SNAPSHOT") == "TXID_SNAPSHOT"
    assert ddl_type("postgresql", "PG_SNAPSHOT") == "PG_SNAPSHOT"


def test_generic_sql_to_sa_value_specialty_ssot():
    """generic_sql write path must canonicalize specialty carriers (not invent TEXT)."""
    from ipaddress import IPv4Address

    from connectors.generic_sql import _to_sa_value

    assert _to_sa_value("10.0.0.1", "INET", db_type="postgresql") == IPv4Address(
        "10.0.0.1"
    )
    assert _to_sa_value("16/b374d848", "PG_LSN", db_type="teradata") == "16/B374D848"
    assert _to_sa_value("(0,0)", "POINT", db_type="hana") == "(0,0)"
    assert _to_sa_value("10:20:10,14", "TXID_SNAPSHOT", db_type="postgresql") == (
        "10:20:10,14"
    )
    assert _to_sa_value(564182, "OID", db_type="postgresql") == 564182
    with pytest.raises(ValueError, match="refuse invent"):
        _to_sa_value("not-an-lsn", "PG_LSN", db_type="postgresql")
