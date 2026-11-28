"""D419 — an SCD2 close keeps every fractional digit of the run stamp.

SQL Server is CI-only here (live: ``test_d419_scd2_sqlserver_live.py``). Locally:

* the close UPDATE as SQL Server's pymssql driver renders it. pymssql quotes a
  ``datetime`` parameter as ``'YYYY-MM-DD HH:MM:SS.fff'`` (milliseconds, no
  offset) — measured below on the installed driver, so the model is the
  driver's own quoting, not an assumption;
* the same three-run scenario through the engine on PostgreSQL and MariaDB,
  where the history must also be exact.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402

_STAMP = datetime(2026, 11, 24, 2, 51, 23, 661678, tzinfo=timezone.utc)


class _PymssqlConn:
    """Executes ``sa.text`` the way SQLAlchemy + pymssql hand it to the server."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, clause, params=None):
        pymssql = pytest.importorskip("pymssql._mssql")
        sql = str(clause)
        for name, value in sorted((params or {}).items(), key=lambda kv: -len(kv[0])):
            sql = sql.replace(f":{name}", pymssql.quote_simple_value(value).decode())
        self.statements.append(sql)

        class _Result:
            rowcount = 1

            def scalar(self):
                return 1

        return _Result()


def test_the_driver_model_is_pymssql_itself():
    pymssql = pytest.importorskip("pymssql._mssql")
    assert pymssql.quote_simple_value(_STAMP) == b"'2026-11-24 02:51:23.661'"


def _stored_in(column_type: str, sql: str) -> datetime:
    """What SQL Server stores for ``SET valid_to = <sql>`` (the server-side rules).

    ``CAST('<iso>' AS DATETIMEOFFSET(7))`` keeps the instant and its offset; a
    DATETIME2 column receiving a DATETIMEOFFSET keeps its local wall clock and
    drops the offset (SQL Server data type conversion). A bare quoted literal
    is what pymssql sent: milliseconds, no offset.
    """
    import re

    cast = re.search(r"CAST\(N?'([^']+)' AS (DATETIMEOFFSET|DATETIME2)\(7\)\)", sql)
    if cast:
        value = datetime.fromisoformat(cast.group(1))
    else:
        literal = re.search(r"valid_to\S* = N?'([^']+)'", sql)
        value = datetime.fromisoformat(literal.group(1))
    if column_type == "DATETIME2":
        return value.replace(tzinfo=None)
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


@pytest.mark.parametrize("column_type", ["DATETIMEOFFSET", "DATETIME2"])
@pytest.mark.parametrize("stamp", [_STAMP, _STAMP.astimezone(timezone(timedelta(hours=-5)))])
def test_sqlserver_close_stores_the_run_instant_exactly(stamp, column_type):
    """The stored valid_to is the instant the successor's valid_from opened at:
    every microsecond, and on DATETIME2 (UTC wall clock) not the -05:00 local time."""
    from services.scd2_engine import _expire_rows

    conn = _PymssqlConn()
    closed = _expire_rows(
        conn, "[dbo].[t]", ["id"], {"3"}, stamp, "mssql",
        cols={"valid_from": "valid_from", "valid_to": "valid_to", "is_current": "is_current"},
    )
    assert closed == 1
    stored = _stored_in(column_type, conn.statements[0])
    if column_type == "DATETIME2":
        assert stored == _STAMP.replace(tzinfo=None), (stored, conn.statements[0])
    else:
        assert stored == _STAMP, (stored, conn.statements[0])


def test_sqlserver_close_of_a_naive_stamp_keeps_microseconds():
    from services.scd2_engine import _expire_rows

    conn = _PymssqlConn()
    naive = _STAMP.replace(tzinfo=None)
    _expire_rows(conn, "[dbo].[t]", ["id"], {"3"}, naive, "mssql",
                 cols={"valid_from": "valid_from", "valid_to": "valid_to", "is_current": "is_current"})
    assert _stored_in("DATETIME2", conn.statements[0]) == naive, conn.statements[0]


def test_sqlserver_close_of_keys_missing_from_the_snapshot_keeps_microseconds():
    from services.scd2_engine import close_versions_missing_from_snapshot

    conn = _PymssqlConn()
    close_versions_missing_from_snapshot(
        conn, "[dbo].[t]", "[dbo].[snap]", ["id"], dialect="mssql", timestamp=_STAMP,
        cols={"valid_from": "valid_from", "valid_to": "valid_to", "is_current": "is_current"},
    )
    update = next(s for s in conn.statements if s.lstrip().upper().startswith("UPDATE"))
    assert _STAMP.isoformat(timespec="microseconds") in update, update


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
def test_three_scd2_runs_leave_an_exact_history(store, dest):
    from tests.c3_live_helpers import mysql_endpoint, mysql_sql, pg_endpoint, pg_sql
    from tests.helpers.d419_scd2_scenario import run_scd2_history_scenario

    try:
        pg_sql("SELECT 1", fetch=True)
        if dest == "mysql":
            mysql_sql("SELECT 1", fetch=True)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"{dest} not reachable: {exc}")
    if dest == "postgresql":
        schema = "public"

        def _history(table):
            return pg_sql(
                f'SELECT id, valid_from, valid_to, is_current FROM {schema}."{table}"',  # nosec B608
                fetch=True,
            )

        def _drop(table):
            pg_sql(f'DROP TABLE IF EXISTS {schema}."{table}"')  # nosec B608

        dest_endpoint = pg_endpoint
    else:
        def _history(table):
            return mysql_sql(
                f"SELECT id, valid_from, valid_to, is_current FROM `{table}`", fetch=True  # nosec B608
            )

        def _drop(table):
            mysql_sql(f"DROP TABLE IF EXISTS `{table}`")  # nosec B608

        dest_endpoint = mysql_endpoint
    run_scd2_history_scenario(
        store, pg_sql=pg_sql, pg_endpoint=pg_endpoint, dest_endpoint=dest_endpoint,
        dest_history=_history, drop_dest=_drop,
    )
