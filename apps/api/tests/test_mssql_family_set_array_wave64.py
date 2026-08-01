"""Wave 64: MSSQL family DDL alias + MySQL SET→PostgreSQL TEXT[] polarity."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_mssql_family_normalizes_to_sqlserver_ddl():
    from services.type_system import ddl_type

    carrier = "ENUM('ok','fail')"
    for db in ("sqlserver", "mssql", "azure_sql", "synapse", "azure_synapse"):
        assert ddl_type(db, carrier) == "NVARCHAR(4)", db
        assert ddl_type(db, "BOOLEAN") == "BIT", db


def test_set_to_pg_text_array_bind_list():
    from connectors.sql_bind import coerce_set_wire, normalize_sql_bind_value
    from services.type_system import ddl_type, enum_domain_would_collapse

    carrier = "SET('a','b','c')"
    assert ddl_type("postgresql", carrier) == "TEXT[]"
    assert enum_domain_would_collapse(carrier, "TEXT[]") is False

    assert coerce_set_wire("c,a", ddl_type=carrier, as_list=True) == ["a", "c"]
    assert coerce_set_wire(5, ddl_type=carrier, as_list=True) == ["a", "c"]
    assert coerce_set_wire("", ddl_type=carrier, as_list=True) == []

    # Engine-aware normalize: PG → list; MySQL → CSV.
    assert normalize_sql_bind_value("c,a", carrier, engine="postgresql") == ["a", "c"]
    assert normalize_sql_bind_value("c,a", carrier, engine="mysql") == "a,c"


def test_generic_sql_set_pg_list_via_to_sa_value():
    from connectors.generic_sql import _to_sa_value

    assert _to_sa_value(5, "SET('a','b','c')", db_type="postgresql") == ["a", "c"]
    assert _to_sa_value(5, "SET('a','b','c')", db_type="mysql") == "a,c"
