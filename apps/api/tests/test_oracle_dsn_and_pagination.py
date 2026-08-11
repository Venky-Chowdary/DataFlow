"""Oracle/DB2 reachability and row-window syntax on the shared SQL read path.

Two defects found by the live Oracle 23ai / SQL Server 2022 migration matrix:

1. the SQLAlchemy URL was built as ``oracle+oracledb://u:p@host:port/NAME``,
   which is a *SID* DSN — every pluggable database, RAC service and Autonomous
   instance is addressed by service name, so connects died with ORA-12505;
2. the fallback table reader emitted ``LIMIT n OFFSET m``, which Oracle and DB2
   reject (ORA-03047), making those sources unreadable entirely.
"""

from __future__ import annotations

import pytest

from connectors.generic_sql import _build_url
from services.dialect_profiles import (
    page_clause,
    uses_fetch_first_pagination,
    zero_row_probe_sql,
)


def test_oracle_url_uses_service_name_not_sid() -> None:
    url = _build_url(
        {
            "type": "oracle",
            "host": "db.example",
            "port": 1521,
            "database": "FREEPDB1",
            "username": "u",
            "password": "p",
        }
    )
    assert url.query.get("service_name") == "FREEPDB1"
    assert not url.database


def test_oracle_url_honours_explicit_sid() -> None:
    url = _build_url(
        {
            "type": "oracle",
            "host": "db.example",
            "database": "ORCL",
            "sid": "ORCL",
            "username": "u",
            "password": "p",
        }
    )
    assert url.database == "ORCL"
    assert "service_name" not in url.query


def test_explicit_service_name_wins_over_database() -> None:
    url = _build_url(
        {
            "type": "oracle",
            "host": "db.example",
            "database": "ignored",
            "service_name": "svc.example.com",
            "username": "u",
            "password": "p",
        }
    )
    assert url.query.get("service_name") == "svc.example.com"


@pytest.mark.parametrize(
    "dialect",
    ["oracle", "amazon_rds_oracle", "autonomous_database", "db2", "mssql", "azure_sql_database"],
)
def test_fetch_first_dialects_never_emit_limit(dialect: str) -> None:
    assert uses_fetch_first_pagination(dialect)
    clause = page_clause(dialect, 100, 50)
    assert "LIMIT" not in clause.upper()
    assert clause == "OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY"


@pytest.mark.parametrize("dialect", ["postgresql", "mysql", "sqlite", "duckdb"])
def test_limit_dialects_keep_limit_offset(dialect: str) -> None:
    assert not uses_fetch_first_pagination(dialect)
    assert page_clause(dialect, 100, 50) == "LIMIT 50 OFFSET 100"


def test_zero_row_probe_is_valid_per_dialect() -> None:
    assert "TOP 0" in zero_row_probe_sql("mssql", '"t"')
    # ``LIMIT 0`` is a syntax error on Oracle/DB2.
    oracle_probe = zero_row_probe_sql("oracle", '"T"')
    assert "LIMIT" not in oracle_probe.upper()
    assert "WHERE 1=0" in oracle_probe
    assert zero_row_probe_sql("postgresql", '"t"').endswith("LIMIT 0")
