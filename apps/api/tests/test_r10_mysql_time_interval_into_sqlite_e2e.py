"""QA round 10: MySQL TIME values that are negative or past 24 hours, into SQLite.

MySQL ``TIME`` is an interval (-838:59:59 .. 838:59:59), not a wall clock.
SQLite has no temporal storage class: the TEXT DataFlow writes IS the value.
So the only faithful carrier is MySQL's own spelling of the value
(``-01:02:03.000000`` for ``TIME(6)``, ``838:59:59`` for ``TIME``), verified
exactly against the source.

On the integration tip the engine copy (``services/copy_mysql_sqlite.py`` via
``copy_sqlite_common.sqlite_temporal_text``) failed verification on these rows
and the row path refused them. Route: MCP ``start_transfer`` ->
``/api/v1/copilot/confirm`` -> job, real MySQL :3306 into a NEW SQLite table,
then a second run into the table DataFlow created, on both the engine copy and
the row path.
"""

from __future__ import annotations

import sqlite3
import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    sqlite_root = tmp_path / "sqlite"
    sqlite_root.mkdir()
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(sqlite_root))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        c.sqlite_db = str(sqlite_root / "r10time.db")  # type: ignore[attr-defined]
        yield c


# id, TIME(0) literal, TIME(6) literal (None = NULL)
_ROWS = [
    (1, "-01:02:03", "-01:02:03"),
    (2, "838:59:59", "838:59:59.000000"),
    (3, "-838:59:59", "-838:59:59.000000"),
    (4, "25:00:00", "25:00:00.5"),
    (5, "03:04:05", "03:04:05.000001"),
    (6, None, None),
    (7, "00:00:00", "-00:00:00.250000"),
    (8, "100:00:00", "23:59:59.999999"),
]
_RUN2 = [(9, "-100:00:01", "-25:30:00.000007"), (10, "48:00:00", "00:00:00")]


def _mysql_text(table: str) -> dict[int, tuple[str | None, str | None]]:
    """MySQL's own text for each value: what the SQLite cell must hold."""
    rows = h.query(
        "mysql",
        f"SELECT id, CAST(t0 AS CHAR), CAST(t6 AS CHAR) FROM `{table}` ORDER BY id",  # nosec B608
    )
    return {int(r[0]): (r[1], r[2]) for r in rows}


def _sqlite_rows(client, table: str) -> dict[int, tuple[str | None, str | None]]:
    with sqlite3.connect(client.sqlite_db) as conn:
        names = [str(r[0]) for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND lower(name)=lower(?)", (table,))]
        assert names, f"SQLite table {table} missing"
        rows = conn.execute(f'SELECT id, t0, t6 FROM "{names[0]}"').fetchall()  # nosec B608
    return {int(r[0]): (r[1], r[2]) for r in rows}


def _insert(table: str, rows) -> None:
    def lit(v):
        return "NULL" if v is None else f"'{v}'"

    values = ",".join(f"({rid},{lit(a)},{lit(b)})" for rid, a, b in rows)
    h.execute("mysql", f"INSERT INTO `{table}` (id, t0, t6) VALUES {values}")  # nosec B608


def _assert_verified(job: dict) -> None:
    assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"))
    rec = job.get("reconciliation") or {}
    assert rec.get("verification_verdict") in (None, "verified"), rec
    assert int(job.get("records_failed") or 0) == 0, job.get("records_failed")


@pytest.mark.parametrize("path", ["engine_copy", "row_path"])
def test_mysql_time_interval_values_land_as_mysql_text_in_sqlite(client, monkeypatch, path):
    if not h.port_up(3306):
        pytest.skip("MySQL :3306 required")
    if path == "row_path":
        monkeypatch.setenv("DATAFLOW_MYSQL_SQLITE_COPY", "0")
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r10tm_s_{token}", f"r10tm_d_{token}"
    from services.connector_store import create_connector

    try:
        h.execute("mysql", f"CREATE TABLE `{src_t}` (id INT PRIMARY KEY, t0 TIME NULL, t6 TIME(6) NULL)")
        _insert(src_t, _ROWS)
        src = create_connector({"name": f"r10tm-s-{token}", "role": "source", "workspace_id": "",
                                "ssl": False, **h.dest_config("mysql")})
        dst = create_connector({"name": f"r10tm-d-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, "type": "sqlite",
                                "database": client.sqlite_db})
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
        }
        job = h.stage_and_run(client, "start_transfer", args)
        _assert_verified(job)
        expected = _mysql_text(src_t)
        assert expected[1] == ("-01:02:03", "-01:02:03.000000"), expected
        assert expected[2] == ("838:59:59", "838:59:59.000000"), expected
        assert _sqlite_rows(client, dst_t) == expected

        # Run 2 into the table DataFlow created.
        h.execute("mysql", f"DELETE FROM `{src_t}` WHERE id = 4")
        _insert(src_t, _RUN2)
        job2 = h.stage_and_run(client, "start_transfer", args)
        _assert_verified(job2)
        assert _sqlite_rows(client, dst_t) == _mysql_text(src_t)
    finally:
        h.drop_table("mysql", src_t)


def test_gate8_sees_a_changed_interval_time_in_sqlite_text():
    """The verification half: an interval TIME compares exactly — a dropped sign
    or a wrapped hour is a different value, MySQL's own text is the same one."""
    from datetime import timedelta

    from services.reconciliation import normalize_cell

    neg = -timedelta(hours=1, minutes=2, seconds=3)
    assert normalize_cell(neg) == normalize_cell("-01:02:03.000000", ddl_type="TEXT", engine="sqlite")
    assert normalize_cell(neg) == normalize_cell("-01:02:03")
    assert normalize_cell(neg) != normalize_cell("01:02:03", ddl_type="TEXT", engine="sqlite")
    assert normalize_cell(neg) != normalize_cell("22:57:57", ddl_type="TEXT", engine="sqlite")
    big = timedelta(hours=838, minutes=59, seconds=59)
    assert normalize_cell(big) == normalize_cell("838:59:59", ddl_type="TEXT", engine="sqlite")
    assert normalize_cell(big) != normalize_cell("22:59:59", ddl_type="TEXT", engine="sqlite")
    frac = -timedelta(microseconds=250000)
    assert normalize_cell(frac) == normalize_cell("-00:00:00.250000", ddl_type="TEXT", engine="sqlite")
