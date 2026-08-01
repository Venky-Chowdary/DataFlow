"""Oracle '' → NULL write bind (VARCHAR2 / HVR write-location coercion)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sql_bind import normalize_sql_bind_value  # noqa: E402


def test_oracle_bind_empty_string_becomes_null():
    assert normalize_sql_bind_value("", "VARCHAR2(100)", engine="oracle") is None
    assert normalize_sql_bind_value("", "NVARCHAR2(50)", engine="oracledb") is None
    assert normalize_sql_bind_value("", "CHAR(1)", engine="oracle") is None


def test_postgres_bind_keeps_empty_string():
    assert normalize_sql_bind_value("", "VARCHAR(100)", engine="postgresql") == ""
    assert normalize_sql_bind_value("", "TEXT", engine="mysql") == ""


def test_oracle_bind_nonempty_passthrough():
    assert normalize_sql_bind_value("hi", "VARCHAR2(10)", engine="oracle") == "hi"
