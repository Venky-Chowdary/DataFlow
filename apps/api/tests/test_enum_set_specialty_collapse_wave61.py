"""Wave 61: ENUM/SET ordinal+bitmask bind + specialty→VARCHAR collapse honesty."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_enum_ordinal_and_member():
    from connectors.sql_bind import coerce_enum_wire, normalize_sql_bind_value

    ddl = "ENUM('Mercury','Venus','Earth')"
    assert coerce_enum_wire("Venus", ddl_type=ddl) == "Venus"
    assert coerce_enum_wire(2, ddl_type=ddl) == "Venus"  # 1-based ordinal
    assert coerce_enum_wire("3", ddl_type=ddl) == "Earth"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_enum_wire(0, ddl_type=ddl)  # error member
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_enum_wire("", ddl_type=ddl)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_enum_wire("Pluto", ddl_type=ddl)
    assert normalize_sql_bind_value(1, ddl) == "Mercury"


def test_coerce_set_bitmask_and_canonical_order():
    from connectors.sql_bind import coerce_set_wire, normalize_sql_bind_value

    ddl = "SET('a','b','c')"
    assert coerce_set_wire("c,a", ddl_type=ddl) == "a,c"  # definition order
    assert coerce_set_wire(5, ddl_type=ddl) == "a,c"  # bits 0+2
    assert coerce_set_wire("", ddl_type=ddl) == ""
    assert coerce_set_wire(["b", "a"], ddl_type=ddl) == "a,b"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_set_wire("a,z", ddl_type=ddl)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_set_wire(8, ddl_type=ddl)  # bit beyond domain
    assert normalize_sql_bind_value(3, ddl) == "a,b"


def test_enum_quarantine_accepts_ordinal_cdc_wire():
    from connectors.writer_common import quarantine_unfit_enum_set

    details: list[dict] = []
    out = quarantine_unfit_enum_set(
        [(2,), ("z",), (0,)],
        ["status"],
        ["ENUM('a','b','c')"],
        details,
        policy="quarantine",
    )
    assert out == [("b",)]
    assert len(details) == 2


def test_specialty_carrier_would_collapse_to_varchar():
    from services.type_system import (
        is_lossy_coercion,
        is_precision_collapse_coercion,
        specialty_carrier_would_collapse,
    )

    assert specialty_carrier_would_collapse("INET", "VARCHAR(45)") is True
    assert specialty_carrier_would_collapse("PG_LSN", "TEXT") is True
    assert specialty_carrier_would_collapse("POINT", "STRING") is True
    assert specialty_carrier_would_collapse("INET", "INET") is False
    assert specialty_carrier_would_collapse("VARCHAR", "TEXT") is False
    assert is_precision_collapse_coercion("INET", "VARCHAR(64)") is True
    assert is_lossy_coercion("HSTORE", "VARCHAR") is True
    # Same specialty stays green.
    assert is_precision_collapse_coercion("INET", "INET") is False


def test_generic_sql_enum_ordinal_via_to_sa_value():
    from connectors.generic_sql import _to_sa_value

    assert (
        _to_sa_value(2, "ENUM('a','b','c')", db_type="mysql") == "b"
    )
    assert _to_sa_value(5, "SET('a','b','c')", db_type="mysql") == "a,c"
