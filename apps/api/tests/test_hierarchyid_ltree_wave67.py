"""Wave 67: SQL Server hierarchyid → PostgreSQL LTREE polarity (AWS DMS class)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_hierarchyid_introspect_and_ddl():
    from services.schema_introspect import _sqlserver_to_logical
    from services.type_system import ddl_type, hierarchyid_to_ltree_path

    assert _sqlserver_to_logical("hierarchyid") == "HIERARCHYID"
    assert ddl_type("sqlserver", "HIERARCHYID") == "HIERARCHYID"
    assert ddl_type("postgresql", "HIERARCHYID") == "LTREE"
    assert ddl_type("mysql", "HIERARCHYID") == "VARCHAR(892)"

    assert hierarchyid_to_ltree_path("/1/2/3/") == "1.2.3"
    assert hierarchyid_to_ltree_path("/") == ""
    assert hierarchyid_to_ltree_path("1.2.3") == "1.2.3"
    with pytest.raises(ValueError, match="refuse invent"):
        hierarchyid_to_ltree_path("/1/bad label/")


def test_hierarchyid_bind_slash_vs_ltree():
    from connectors.sql_bind import (
        coerce_hierarchyid_wire,
        coerce_ltree_wire,
        normalize_sql_bind_value,
    )

    assert coerce_hierarchyid_wire("/1/1/3/") == "/1/1/3/"
    assert coerce_hierarchyid_wire("1.2.3") == "/1/2/3/"
    assert coerce_hierarchyid_wire("/1/2/", as_ltree=True) == "1.2"
    assert coerce_ltree_wire("/1/2/3/") == "1.2.3"

    assert normalize_sql_bind_value(
        "/2/1/", "HIERARCHYID", engine="sqlserver"
    ) == "/2/1/"
    assert normalize_sql_bind_value(
        "/2/1/", "HIERARCHYID", engine="postgresql"
    ) == "2.1"
    assert normalize_sql_bind_value("/2/1/", "LTREE", engine="postgresql") == "2.1"

    with pytest.raises(ValueError, match="binary"):
        coerce_hierarchyid_wire(b"\x5a\xde")


def test_generic_sql_hierarchyid_to_ltree():
    from connectors.generic_sql import _to_sa_value

    assert _to_sa_value("/1/2/", "HIERARCHYID", db_type="postgresql") == "1.2"
    assert _to_sa_value("/1/2/", "HIERARCHYID", db_type="sqlserver") == "/1/2/"
