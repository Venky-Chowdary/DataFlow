"""Append probe review S1: a NULL key part collides only where the engine says it does.

The tuple probe matched NULL parts with IS NULL, so an append of ('x', NULL)
into UNIQUE(a, b) already holding ('x', NULL) was blocked on PostgreSQL,
MySQL and SQLite, which accept it (NULLs are distinct in a unique key). SQL
Server treats NULL as a value and rejects it - including a second NULL in a
single-column UNIQUE key.

Live MySQL -> PostgreSQL / SQL Server, plus SQLite.
"""

from __future__ import annotations

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


def _contract(table: str) -> list[dict]:
    return [{"name": table, "sync_mode": "incremental_append", "primary_key": "",
             "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
             "selected": True}]


def _source(table: str, rows: list[tuple]) -> None:
    h.run_sql("mysql", f"CREATE TABLE {table} (a VARCHAR(10), b VARCHAR(10), "
              "updated_at DATETIME(6)) DEFAULT CHARSET=utf8mb4")
    h.run_sql("mysql", f"INSERT INTO {table} VALUES (%s, %s, %s)", params=rows)  # nosec B608


def _run(fake, table, out, dest):
    return h.execute(
        fake, h.endpoint("mysql", table), h.endpoint(dest, out),
        sync_mode="incremental_append", stream_contracts=_contract(table),
    )


@pytest.fixture()
def live():
    if not h._port_up(3306) or not h._port_up(5432):
        pytest.skip("live MySQL / PostgreSQL not reachable")


def test_postgresql_accepts_a_second_tuple_with_a_null_part(isolated, live):  # noqa: F811
    table, out = f"np_{h.token()}", f"np_{h.token()}_d"
    try:
        _source(table, [("x", None, "2024-02-01"), ("東京", "", "2024-02-02")])
        h.run_sql("postgresql", f"CREATE TABLE {out} (a VARCHAR(10), b VARCHAR(10), "
                  "updated_at TIMESTAMP, UNIQUE (a, b))",
                  f"INSERT INTO {out} VALUES ('x', NULL, '2024-01-01')")  # nosec B608
        run = _run(isolated, table, out, "postgresql")
        assert run.success, run.error
        assert h.run_sql("postgresql", f"SELECT COUNT(*) FROM {out} WHERE a = 'x'")[0][0] == 2  # nosec B608
    finally:
        h.drop("mysql", table)
        h.drop("postgresql", out)


@pytest.mark.parametrize("shape", ["composite", "single"])
def test_sql_server_rejects_a_repeated_null_and_validate_says_so(isolated, live, shape):  # noqa: F811
    if not h._port_up(1433):
        pytest.skip("live SQL Server not reachable")
    table, out = f"ns_{h.token()}", f"ns_{h.token()}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (a VARCHAR(10), b VARCHAR(10), "
                  "updated_at TIMESTAMP)",
                  f"INSERT INTO {table} VALUES ('x', NULL, '2024-02-01')")  # nosec B608
        key = "UNIQUE (a, b)" if shape == "composite" else "UNIQUE (b)"
        bin2 = "COLLATE Latin1_General_100_BIN2"
        h.run_sql("sqlserver", f"CREATE TABLE {out} (a NVARCHAR(10) {bin2}, b NVARCHAR(10) {bin2}, "
                  f"updated_at DATETIME2, {key})",
                  f"INSERT INTO {out} VALUES ('x', NULL, '2024-01-01')")  # nosec B608
        run = h.execute(
            isolated, h.endpoint("postgresql", table), h.endpoint("sqlserver", out),
            sync_mode="incremental_append", stream_contracts=_contract(table),
        )
        assert not run.success
        assert "Blocking issues detected" in (run.error or ""), run.error
        assert "enforces uniqueness" in run.error, run.error
        assert h.run_sql("sqlserver", f"SELECT COUNT(*) FROM {out}")[0][0] == 1  # nosec B608
    finally:
        h.drop("postgresql", table)
        h.drop("sqlserver", out)


def test_sqlite_composite_unique_with_a_null_part_does_not_block(tmp_path):
    import sqlite3

    from services.preflight_service import run_file_preflight

    db = tmp_path / "nk.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (a TEXT, b TEXT, UNIQUE (a, b))")
    conn.execute("INSERT INTO t VALUES ('x', NULL)")
    conn.commit()
    conn.close()
    result = run_file_preflight(
        columns=["a", "b"], column_types={"a": "VARCHAR", "b": "VARCHAR"}, row_count=1,
        mappings=[{"source": "a", "target": "a", "confidence": 0.99},
                  {"source": "b", "target": "b", "confidence": 0.99}],
        destination_connected=True, destination_can_create=True, destination_can_write=True,
        source_connected=True, source_kind="file", source_format="csv",
        sync_mode="append", sample_rows=[{"a": "x", "b": None}],
        destination_db_type="sqlite", destination_table="t", destination_table_exists=True,
        destination_pk_columns=[],
        destination_unique_keys=[{"name": "uq", "columns": ["a", "b"], "primary": False}],
        destination_config={"type": "sqlite", "connection_string": f"sqlite:///{db}"},
        destination_column_types={"a": "TEXT", "b": "TEXT"}, validation_mode="strict",
    )
    g6 = {g["id"]: g for g in result["gates"]}["g6_target_ddl"]
    assert g6["status"] != "block", g6
