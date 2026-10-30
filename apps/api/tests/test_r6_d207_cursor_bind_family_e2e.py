"""D207 family — every engine binds the watermark as a value of the cursor's type.

D207 itself is Oracle (the watermark went over as a string Oracle parsed with
``NLS_DATE_FORMAT``). The same string bind has a second failure on MySQL and
MariaDB: a string compared with a BIGINT or DECIMAL column is compared as a
DOUBLE, so ``seq > '9007199254740992'`` is false for ``9007199254740993`` and
the row on the far side of a page edge is skipped — the run still reports
success. Snowflake-style 64-bit ids and high-precision DECIMAL cursors are
exactly the values that collide in a double.

Each leg runs QA's route (MCP ``start_transfer`` -> ``/api/v1/copilot/confirm``
-> job) three times — run 1, new rows, quiet run 3 — with two-row pages so page
edges fall on the colliding values, and compares the destination with the
source. The SQL Server and PostgreSQL legs (DATETIMEOFFSET across offsets,
DATE ties, timestamptz, numeric) pin the typed bind on the engines whose
string coercion happened to work.
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

B = 2**53  # 9007199254740992: B and B+1 are the same double.


def _mariadb_conn():
    pymysql = pytest.importorskip("pymysql")
    pw = os.environ.get("DATAFLOW_TEST_MARIADB_PASSWORD", "")
    if not pw or not h.port_up(3307):
        pytest.skip("MariaDB not configured (DATAFLOW_TEST_MARIADB_PASSWORD, :3307)")
    return pymysql.connect(
        host=os.environ.get("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1"), port=3307,
        user=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "root"), password=pw,
        database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
        charset="utf8mb4", autocommit=True,
    )


def _exec(kind: str, sql: str) -> None:
    if kind == "mariadb":
        conn = _mariadb_conn()
        try:
            conn.cursor().execute(sql)
        finally:
            conn.close()
        return
    h.execute(kind, sql)


def _config(kind: str) -> dict[str, Any]:
    if kind == "mariadb":
        _mariadb_conn().close()
        return dict(
            type="mariadb", host=os.environ.get("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1"),
            port=3307, database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
            username=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "root"),
            password=os.environ["DATAFLOW_TEST_MARIADB_PASSWORD"],
        )
    return h.dest_config(kind)


def _q(kind: str, name: str) -> str:
    return h.quote("mysql" if kind == "mariadb" else kind, name)


def _lit(kind: str, note: str | None) -> str:
    if note is None:
        return "NULL"
    esc = note.replace("'", "''")
    return f"N'{esc}'" if kind == "sqlserver" else f"'{esc}'"


_TXT = {"postgresql": "TEXT", "mysql": "VARCHAR(40)", "mariadb": "VARCHAR(40)",
        "sqlserver": "NVARCHAR(40)"}

# leg -> (engine, cursor DDL type, run-1 cursor literals, run-2 cursor literals)
# Keys descend while cursors ascend, so the (cursor, key) tie-break cannot
# rescue a page edge the cursor comparison got wrong.
_LEGS: dict[str, tuple[str, str, list[str], list[str]]] = {
    "mysql_bigint": ("mysql", "BIGINT", [str(B - 1), str(B), str(B + 1), str(B + 2),
                                         str(B + 3)], [str(B + 4), str(B + 5)]),
    "mysql_decimal": ("mysql", "DECIMAL(30,10)",
                      ["12345678901234567.0000000001", "12345678901234567.0000000002",
                       "12345678901234567.0000000003", "12345678901234567.0000000004",
                       "12345678901234567.0000000005"],
                      ["12345678901234567.0000000006", "12345678901234567.0000000007"]),
    "mariadb_bigint": ("mariadb", "BIGINT", [str(B - 1), str(B), str(B + 1), str(B + 2),
                                             str(B + 3)], [str(B + 4), str(B + 5)]),
    "sqlserver_datetimeoffset": (
        "sqlserver", "DATETIMEOFFSET(6)",
        ["'2026-10-01T00:30:00+05:30'", "'2026-09-30T20:00:00-04:00'",  # 19:00Z, 00:00Z
         "'2026-10-01T00:30:00+00:00'", "'2026-10-01T06:31:00+05:30'",  # 00:30Z, 01:01Z
         "'2026-10-01T02:00:00+00:00'"],
        ["'2026-09-30T23:00:00-04:00'", "'2026-10-01T09:00:00+05:30'"],  # 03:00Z, 03:30Z
    ),
    "sqlserver_date": ("sqlserver", "DATE",
                       ["'2026-10-01'", "'2026-10-01'", "'2026-10-02'", "'2026-10-02'",
                        "'2026-10-03'"], ["'2026-10-03'", "'2026-10-04'"]),
    "pg_timestamptz": ("postgresql", "TIMESTAMPTZ",
                       ["'2026-10-01 00:30:00+05:30'", "'2026-09-30 20:00:00-04:00'",
                        "'2026-10-01 00:30:00+00'", "'2026-10-01 06:31:00+05:30'",
                        "'2026-10-01 02:00:00+00'"],
                       ["'2026-09-30 23:00:00-04:00'", "'2026-10-01 09:00:00+05:30'"]),
    "pg_numeric": ("postgresql", "NUMERIC(30,10)",
                   ["12345678901234567.0000000001", "12345678901234567.0000000002",
                    "12345678901234567.0000000003", "12345678901234567.0000000004",
                    "12345678901234567.0000000005"],
                   ["12345678901234567.0000000006", "12345678901234567.0000000007"]),
    "pg_date": ("postgresql", "DATE",
                ["'2026-10-01'", "'2026-10-01'", "'2026-10-02'", "'2026-10-02'",
                 "'2026-10-03'"], ["'2026-10-03'", "'2026-10-04'"]),
}

_NOTES = [None, "", "  ", "Łukasz 山田 🎉", "plain", "x", "y"]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    import src.transfer.stream as stream_mod

    # Two-row pages: every other row sits on a page edge.
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 2)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _insert(kind: str, table: str, rows: list[tuple[int, str | None, str]]) -> None:
    for rid, note, cursor in rows:
        _exec(
            kind,
            f"INSERT INTO {_q(kind, table)} (id, note, seq) "  # nosec B608
            f"VALUES ({rid}, {_lit(kind, note)}, {cursor})",
        )


def _dest(table: str) -> list[tuple]:
    rows = h.query("postgresql", f'SELECT id, note FROM "{table}"')  # nosec B608
    return sorted(((int(r[0]), r[1]) for r in rows), key=lambda r: (r[0], str(r[1])))


@pytest.mark.parametrize("leg", list(_LEGS), ids=list(_LEGS))
def test_incremental_page_edges_bind_the_watermark_typed(client, leg):
    from services.connector_store import create_connector

    kind, cursor_type, run1_cursors, run2_cursors = _LEGS[leg]
    token = uuid.uuid4().hex[:8]
    src_table = f"r6cb_s_{token}"
    dest_table = f"r6cb_d_{token}"
    run1 = [(100 - 10 * i, _NOTES[i % len(_NOTES)], c) for i, c in enumerate(run1_cursors)]
    run2 = [(9 - i, _NOTES[(i + 3) % len(_NOTES)], c) for i, c in enumerate(run2_cursors)]
    src_cfg = _config(kind)
    dst_cfg = h.dest_config("postgresql")
    try:
        _exec(
            kind,
            f"CREATE TABLE {_q(kind, src_table)} (id INT PRIMARY KEY, "  # nosec B608
            f"note {_TXT[kind]} NULL, seq {cursor_type} NOT NULL)",
        )
        _insert(kind, src_table, run1)
        src = create_connector({"name": f"r6cb-s-{token}", "role": "source",
                                "workspace_id": "", "ssl": False, **src_cfg})
        dst = create_connector({"name": f"r6cb-d-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **dst_cfg})
        args = {
            "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest_table,
            "sync_mode": "incremental_append", "validation_mode": "strict",
            "cursor_column": "seq", "cursor_semantics": "monotonic_sequence",
            "primary_key": "id",
        }
        expected1 = sorted((r, n) for r, n, _ in run1)
        h.stage_and_run(client, "start_transfer", args)
        assert _dest(dest_table) == sorted(expected1, key=lambda r: (r[0], str(r[1]))), 1

        _insert(kind, src_table, run2)
        j2 = h.stage_and_run(client, "start_transfer", args)
        expected = sorted(
            [(r, n) for r, n, _ in run1 + run2], key=lambda r: (r[0], str(r[1]))
        )
        assert _dest(dest_table) == expected, {"run": 2, "job": j2.get("incremental_watermark")}

        j3 = h.stage_and_run(client, "start_transfer", args)  # quiet
        assert _dest(dest_table) == expected, {"run": 3, "job": j3.get("incremental_watermark")}
        rep3 = j3.get("incremental_watermark") or {}
        if rep3:
            assert int(rep3.get("rows_selected") or 0) <= len(run2), rep3
    finally:
        try:
            _exec(kind, f"DROP TABLE IF EXISTS {_q(kind, src_table)}")  # nosec B608
        except Exception:
            pass
        h.drop_table("postgresql", dest_table)


def test_mysql_compares_a_string_with_bigint_as_a_double():
    """The engine fact the MySQL legs rest on (not DataFlow code)."""
    conn = h.mysql_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT CAST({B + 1} AS SIGNED) > '{B}', CAST({B + 1} AS SIGNED) > {B}")
        assert tuple(cur.fetchone()) == (0, 1)
    finally:
        conn.close()
    assert Decimal(B + 1) > Decimal(B)
