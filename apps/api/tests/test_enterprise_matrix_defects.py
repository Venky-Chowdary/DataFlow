"""Regressions for defects the live Oracle/SQL Server/PostgreSQL matrix exposed.

Each test pins a shared-path fix, not the route that surfaced it: identifier
folding (any upper-folding engine), SQL Server CREATE-permission scope, Oracle
owner privileges, and canonical boolean → numeric coercion (every engine with
no native boolean).
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from services.destination_privilege_probe import evaluate_oracle_privileges
from services.dialect_profiles import denormalize_result_key
from services.transform_engine import apply_transform


# ── Identifier folding ───────────────────────────────────────────────────────

@pytest.mark.parametrize("dialect", ["oracle", "amazon_rds_oracle", "db2", "snowflake"])
def test_upper_folding_result_keys_requote_uppercase(dialect: str) -> None:
    # ORA-00904: the driver lowercases case-insensitive names; quoting the key
    # verbatim references a column that does not exist.
    assert denormalize_result_key(dialect, "id") == "ID"


@pytest.mark.parametrize("dialect", ["postgresql", "mysql", "sqlite", "duckdb"])
def test_lower_folding_result_keys_are_untouched(dialect: str) -> None:
    assert denormalize_result_key(dialect, "id") == "id"


def test_quoted_mixed_case_oracle_name_is_preserved() -> None:
    # Created quoted, so the stored spelling is exactly this — never upper it.
    assert denormalize_result_key("oracle", "OrderId") == "OrderId"


def test_oracle_read_orders_by_physical_column_name() -> None:
    from connectors.generic_sql import _read_table_raw

    conn = MagicMock()
    probe = MagicMock()
    probe.keys.return_value = ["id", "name"]
    result = MagicMock()
    result.keys.return_value = ["id", "name"]
    result.fetchall.return_value = [(1, "a")]
    conn.execute.side_effect = [probe, result]

    _read_table_raw(conn, "ENT_SRC", "DFUSER", offset=0, limit=10, dialect="oracle")

    page_sql = conn.execute.call_args_list[1][0][0].text
    assert 'ORDER BY "ID"' in page_sql
    assert 'ORDER BY "id"' not in page_sql


# ── Oracle owner privileges ──────────────────────────────────────────────────

def test_oracle_owner_can_write_own_table_without_explicit_grant() -> None:
    # ALL_TAB_PRIVS records grants only; an owner never appears in it.
    can_write, _ = evaluate_oracle_privileges(
        session_privs={"CREATE SESSION", "CREATE TABLE"},
        tab_privs=set(),
        table_exists=True,
        need_update=True,
        is_owner=True,
    )
    assert can_write is True


def test_oracle_non_owner_without_grant_is_still_denied() -> None:
    can_write, _ = evaluate_oracle_privileges(
        session_privs={"CREATE SESSION"},
        tab_privs=set(),
        table_exists=True,
        need_update=False,
        is_owner=False,
    )
    assert can_write is False


# ── SQL Server CREATE scope ──────────────────────────────────────────────────

def test_sqlserver_create_probe_uses_database_scope_not_schema_scope() -> None:
    import inspect

    from services import destination_privilege_probe as probe_mod

    src = "\n".join(
        line
        for line in inspect.getsource(probe_mod._probe_sqlserver).splitlines()
        if not line.lstrip().startswith("#")
    )
    # HAS_PERMS_BY_NAME(schema,'SCHEMA','CREATE TABLE') is always NULL — even for
    # sysadmin — so it must never be the create probe.
    assert "'SCHEMA', 'CREATE TABLE'" not in src
    assert "DB_NAME(), 'DATABASE', 'CREATE TABLE'" in src
    assert "IS_SRVROLEMEMBER('sysadmin')" in src


# ── Boolean → numeric ────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    ("wire", "expected"),
    [("true", 1), ("false", 0), ("TRUE", 1), ("t", 1), ("f", 0)],
)
def test_canonical_boolean_wire_loads_into_integer_target(wire: str, expected: int) -> None:
    value, err = apply_transform(wire, "integer")
    assert err is None
    assert value == expected


def test_canonical_boolean_wire_loads_into_decimal_target() -> None:
    value, err = apply_transform("true", "decimal")
    assert err is None
    assert value == Decimal(1)


@pytest.mark.parametrize("wire", ["yes", "on", "y", "enabled"])
def test_informal_boolean_still_refuses_numeric_target(wire: str) -> None:
    _value, err = apply_transform(wire, "integer")
    assert err is not None
