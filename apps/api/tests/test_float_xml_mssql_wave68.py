"""Wave 68: IEEE FLOAT bind SSOT + SQL Server XML specialty carrier."""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_float_digit_string_and_refuse_bool_token():
    from connectors.sql_bind import coerce_float_wire, normalize_sql_bind_value

    assert coerce_float_wire("1.5") == 1.5
    assert coerce_float_wire(2) == 2.0
    assert normalize_sql_bind_value("3.25", "FLOAT", engine="sqlserver") == 3.25
    assert normalize_sql_bind_value(
        "1e-3", "DOUBLE PRECISION", engine="postgresql"
    ) == 0.001
    assert math.isnan(coerce_float_wire("nan"))
    with pytest.raises(ValueError, match="boolean token"):
        coerce_float_wire("true")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_float_wire("not-a-number")


def test_sqlserver_xml_introspect_and_ddl():
    from services.schema_introspect import _sqlserver_to_logical
    from services.type_system import ddl_type

    assert _sqlserver_to_logical("xml") == "XML"
    assert ddl_type("sqlserver", "XML") == "XML"
    assert ddl_type("postgresql", "XML") == "XML"
    assert ddl_type("oracle", "XML") == "XMLTYPE"
    assert ddl_type("mysql", "XML") == "LONGTEXT"


def test_generic_sql_float_via_to_sa_value():
    from connectors.generic_sql import _to_sa_value

    assert _to_sa_value("1.5", "FLOAT", db_type="sqlserver") == 1.5
    assert _to_sa_value("2", "REAL", db_type="postgresql") == 2.0
