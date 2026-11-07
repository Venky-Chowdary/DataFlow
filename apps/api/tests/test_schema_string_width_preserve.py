"""Introspect mappers must preserve CHAR/VARCHAR(n) for G3 + write quarantine."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.decision_kernel.type_invent import materialize_dest_ddl  # noqa: E402
from services.schema_introspect import (  # noqa: E402
    _mysql_extra_is_computed_column,
    _mysql_to_logical,
    _oracle_to_logical,
    _pg_to_logical,
    _sqlserver_to_logical,
)


def test_pg_preserves_varchar_and_char_width():
    assert _pg_to_logical("character varying(255)") == "VARCHAR(255)"
    assert _pg_to_logical("varchar(50)") == "VARCHAR(50)"
    assert _pg_to_logical("character(10)") == "CHAR(10)"
    assert _pg_to_logical("char(3)") == "CHAR(3)"
    assert _pg_to_logical("text") == "TEXT"


def test_mysql_preserves_varchar_and_char_width():
    assert _mysql_to_logical("varchar(255)") == "VARCHAR(255)"
    assert _mysql_to_logical("char(10)") == "CHAR(10)"
    assert _mysql_to_logical("text") == "TEXT"


def test_mysql_text_tiers_do_not_collapse_to_text():
    assert _mysql_to_logical("longtext") == "LONGTEXT"
    assert _mysql_to_logical("mediumtext") == "MEDIUMTEXT"
    assert _mysql_to_logical("tinytext") == "TINYTEXT"
    assert _mysql_to_logical("LONGTEXT") == "LONGTEXT"
    assert materialize_dest_ddl("mysql", "LONGTEXT").split()[0] == "LONGTEXT"
    assert materialize_dest_ddl("mysql", "MEDIUMTEXT").split()[0] == "MEDIUMTEXT"
    assert materialize_dest_ddl("mysql", "TINYTEXT").split()[0] == "TINYTEXT"
    assert materialize_dest_ddl("mysql", "TEXT").split()[0] == "TEXT"


def test_mysql_default_generated_is_not_a_computed_column():
    assert _mysql_extra_is_computed_column("DEFAULT_GENERATED") is False
    assert _mysql_extra_is_computed_column("default_generated") is False
    assert _mysql_extra_is_computed_column("") is False
    assert _mysql_extra_is_computed_column("VIRTUAL GENERATED") is True
    assert _mysql_extra_is_computed_column("STORED GENERATED") is True
    assert _mysql_extra_is_computed_column("auto_increment") is False


def test_mysql_preserves_varbinary_and_enum_domain():
    assert _mysql_to_logical("varbinary(16)") == "VARBINARY(16)"
    assert _mysql_to_logical("binary(32)") == "BINARY(32)"
    assert _mysql_to_logical("enum('a','b')") == "ENUM('a','b')"
    assert _mysql_to_logical("set('x','y')") == "SET('x','y')"


def test_sqlserver_preserves_varchar_and_nvarchar_width():
    assert _sqlserver_to_logical("varchar(50)") == "VARCHAR(50)"
    assert _sqlserver_to_logical("nvarchar(100)") == "NVARCHAR(100)"
    assert _sqlserver_to_logical("char(5)") == "CHAR(5)"
    assert _sqlserver_to_logical("nchar(8)") == "NCHAR(8)"
    assert _sqlserver_to_logical("varchar(max)") == "TEXT"
    assert _sqlserver_to_logical("nvarchar(max)") == "NVARCHAR(MAX)"
    assert _sqlserver_to_logical("nvarchar") == "NVARCHAR"
    assert _sqlserver_to_logical("ntext") == "NVARCHAR(MAX)"
    assert _sqlserver_to_logical("nchar(max)") == "NVARCHAR(MAX)"
    assert _sqlserver_to_logical("varbinary(64)") == "VARBINARY(64)"
    assert _sqlserver_to_logical("binary(16)") == "BINARY(16)"


def test_oracle_preserves_varchar2_and_char_width():
    assert _oracle_to_logical("VARCHAR2(100)") == "VARCHAR(100)"
    assert _oracle_to_logical("VARCHAR2(100 BYTE)") == "VARCHAR(100 BYTE)"
    assert _oracle_to_logical("VARCHAR2(100 CHAR)") == "VARCHAR(100 CHAR)"
    assert _oracle_to_logical("NVARCHAR2(50)") == "NVARCHAR(50)"
    assert _oracle_to_logical("NVARCHAR2(50 CHAR)") == "NVARCHAR(50 CHAR)"
    assert _oracle_to_logical("CHAR(10)") == "CHAR(10)"
    assert _oracle_to_logical("NCHAR(4)") == "NCHAR(4)"
