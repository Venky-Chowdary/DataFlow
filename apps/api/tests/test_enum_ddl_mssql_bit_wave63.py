"""Wave 63: create-new ENUM/SET DDL SSOT + SQL Server BIT vs TINYINT bind."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_ddl_type_enum_mysql_inline_pg_named_type():
    from services.type_system import (
        collect_pg_enum_prerequisites,
        ddl_type,
        enum_domain_would_collapse,
        pg_enum_type_name,
    )

    carrier = "ENUM('active','paused','archived')"
    assert ddl_type("mysql", carrier) == carrier
    assert ddl_type("mariadb", carrier) == carrier

    name = pg_enum_type_name(["active", "paused", "archived"])
    assert name.startswith("df_enum_")
    assert ddl_type("postgresql", carrier) == name
    assert ddl_type("cockroachdb", carrier) == name

    stmts = collect_pg_enum_prerequisites([carrier, "TEXT", carrier])
    assert len(stmts) == 1
    assert f"CREATE TYPE {name} AS ENUM" in stmts[0]
    assert "'active'" in stmts[0] and "'archived'" in stmts[0]
    assert "duplicate_object" in stmts[0]

    assert enum_domain_would_collapse(carrier, "VARCHAR(32)") is True
    assert enum_domain_would_collapse(carrier, "TEXT") is True
    assert enum_domain_would_collapse(carrier, name) is False
    assert enum_domain_would_collapse(carrier, "ENUM('active','paused')") is False


def test_ddl_type_set_mysql_native_elsewhere_varchar():
    from services.type_system import ddl_type, enum_domain_would_collapse

    carrier = "SET('read','write','admin')"
    assert ddl_type("mysql", carrier) == carrier
    # Wave 64: PostgreSQL create-new preserves multi-value polarity as TEXT[].
    assert ddl_type("postgresql", carrier) == "TEXT[]"
    assert ddl_type("sqlserver", carrier) == "NVARCHAR(16)"
    assert ddl_type("oracle", carrier) == "VARCHAR2(16)"
    assert enum_domain_would_collapse(carrier, "VARCHAR(16)") is True
    assert enum_domain_would_collapse(carrier, "TEXT[]") is False


def test_ddl_type_enum_non_pg_bounded_string():
    from services.type_system import ddl_type

    carrier = "ENUM('ok','fail')"
    assert ddl_type("sqlserver", carrier) == "NVARCHAR(4)"
    assert ddl_type("oracle", carrier) == "VARCHAR2(4)"
    assert ddl_type("snowflake", carrier) == "VARCHAR(4)"


def test_mssql_bit_boolean_polarity_tinyint_stays_int():
    """SQL Server BIT is boolean; TINYINT is a numeric byte (not MySQL TINYINT(1))."""
    from connectors.sql_bind import normalize_sql_bind_value

    assert normalize_sql_bind_value("0", "BIT", engine="sqlserver") is False
    assert normalize_sql_bind_value("1", "BIT(1)", engine="mssql") is True
    assert normalize_sql_bind_value(0, "BIT", engine="azure_sql") is False

    tiny = normalize_sql_bind_value("1", "TINYINT", engine="sqlserver")
    assert tiny == 1
    assert type(tiny) is int
    assert normalize_sql_bind_value(0, "TINYINT", engine="mssql") == 0
    assert normalize_sql_bind_value("255", "TINYINT", engine="sqlserver") == 255
    with pytest.raises(ValueError, match="out of range"):
        normalize_sql_bind_value("256", "TINYINT", engine="sqlserver")
    with pytest.raises(ValueError, match="boolean token"):
        normalize_sql_bind_value("true", "TINYINT", engine="sqlserver")

    # MySQL TINYINT(1) convention remains int 0/1 polarity.
    assert normalize_sql_bind_value("0", "TINYINT", engine="mysql") == 0
    assert normalize_sql_bind_value("1", "TINYINT(1)", engine="mysql") == 1


def test_pg_writer_target_types_use_df_enum_name():
    """Create-new PG DDL must reference df_enum_* after prerequisite CREATE TYPE."""
    from services.type_system import ddl_type, pg_enum_type_name

    carrier = "ENUM('a','b')"
    assert ddl_type("postgresql", carrier) == pg_enum_type_name(["a", "b"])
