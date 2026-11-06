"""D271 — a declared ENUM into a table DataFlow creates is not a collapse.

Stripe declares ``tax_exempt ENUM('none','exempt','reverse')``. SQL Server,
SQLite, MongoDB, Oracle, Snowflake... cannot declare an enum, so DataFlow's own
create-new carrier there is an open string sized to the longest label. Grading
that as a "fidelity collapse" refused every load into a table DataFlow was
about to create, with no carrier the operator could pick instead. MySQL keeps
the native ENUM and PostgreSQL a ``df_enum_*`` type; a PostgreSQL column that
already exists as TEXT still needs the operator's decision (PG could hold it).

Live: MySQL ENUM source (the same declared domain) -> new SQL Server, SQLite
and PostgreSQL tables, run twice (run 2 into the table run 1 created),
preflight ON, strict, with NULL / '' labels.
"""

from __future__ import annotations

import sqlite3

import pytest

from services.type_coercion import is_lossy_coercion
from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)

_ENUM = "ENUM('none','exempt','reverse')"


@pytest.mark.parametrize(
    ("dest", "carrier"),
    [
        ("sqlserver", "NVARCHAR(7)"), ("sqlite", "VARCHAR(7)"), ("mongodb", "VARCHAR(7)"),
        ("oracle", "VARCHAR2(7)"), ("snowflake", "VARCHAR(7)"),
    ],
)
def test_enum_onto_the_dialects_only_carrier_is_not_lossy(dest, carrier):
    assert is_lossy_coercion(_ENUM, carrier, dest_db=dest, source_db="stripe") is False


def test_set_is_sized_as_the_joined_value_and_oracle_counts_bytes():
    from services.type_system import ddl_type

    joined = "SET('alpha','beta','gamma')"
    assert ddl_type("sqlserver", joined) == "NVARCHAR(16)"
    # The longest single member fits NVARCHAR(5); 'alpha,beta' does not.
    assert is_lossy_coercion(joined, "NVARCHAR(5)", dest_db="sqlserver") is True
    assert is_lossy_coercion(joined, "NVARCHAR(16)", dest_db="sqlserver") is False
    # Oracle VARCHAR2(n) is BYTE semantics: 'café' is 5 bytes.
    assert ddl_type("oracle", "ENUM('café','x')") == "VARCHAR2(5)"
    assert is_lossy_coercion("ENUM('café','x')", "VARCHAR2(4)", dest_db="oracle") is True
    assert is_lossy_coercion("ENUM('café','x')", "VARCHAR2(5)", dest_db="oracle") is False


def test_enum_into_open_text_where_the_engine_has_enums_stays_a_decision():
    assert is_lossy_coercion(_ENUM, "TEXT", dest_db="postgresql") is True
    assert is_lossy_coercion(_ENUM, "VARCHAR(32)", dest_db="mysql") is True
    # A carrier too narrow for the longest label is still a truncation.
    assert is_lossy_coercion(_ENUM, "NVARCHAR(3)", dest_db="sqlserver") is True


_ROWS = [(1, "none"), (2, "exempt"), (3, "reverse"), (4, None)]


def _seed(table: str) -> None:
    h.run_sql(
        "mysql",
        f"CREATE TABLE `{table}` (id INT PRIMARY KEY, tax_exempt {_ENUM} NULL) "
        "CHARACTER SET utf8mb4",
    )
    h.run_sql("mysql", f"INSERT INTO `{table}` VALUES (%s, %s)", params=_ROWS)  # nosec B608


@pytest.mark.parametrize("dest", ["sqlserver", "postgresql", "sqlite"])
def test_mysql_enum_loads_into_a_new_table_twice(tmp_path, isolated, dest):  # noqa: F811
    table = f"d271_{h.token()}"
    out = f"{table}_out"
    path = tmp_path / "dst.sqlite"
    target = h.sqlite_endpoint(path, out) if dest == "sqlite" else h.endpoint(dest, out)
    try:
        _seed(table)
        for run in (1, 2):
            if run == 2:
                h.run_sql("mysql", f"INSERT INTO `{table}` VALUES (5, 'exempt')")  # nosec B608
            result = h.execute(
                isolated, h.endpoint("mysql", table), target,
                sync_mode="full_refresh_overwrite",
            )
            assert result.success, (run, result.error)
        sql = f"SELECT id, tax_exempt FROM {h.quote(dest, out)} ORDER BY id"  # nosec B608
        got = (
            sqlite3.connect(path).execute(sql).fetchall()
            if dest == "sqlite"
            else h.run_sql(dest, sql)
        )
        assert [tuple(r) for r in got] == _ROWS + [(5, "exempt")]
    finally:
        h.drop("mysql", table)
        if dest != "sqlite":
            h.drop(dest, out)
