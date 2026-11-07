"""D28 — CDC on a PostgreSQL source without logical decoding honours cursor_column.

QA (production ac612203): Pilot ``start_transfer`` with ``sync_mode=cdc``,
``cursor_column=updated_at`` and ``cursor_semantics=modification_timestamp`` on
a PostgreSQL server at ``wal_level=replica``. The run never polled the column
the operator named: preflight refused CDC outright, and the engine raised
"wal_level is not logical … a cursor poll would miss updates" before it read the
poll cursor at all, so the declared cursor could not take effect.

The route this file drives is the user's: MCP ``start_transfer`` -> the
``/copilot/confirm`` spend -> the job, against the live local PostgreSQL. Run 1
creates the destination, run 2 polls ``updated_at`` into the table DataFlow
created. NULL, empty, whitespace and unicode cells are compared cell by cell.

Still refused, by design: a poll on the primary key, and a cursor whose meaning
was not declared — an UPDATE does not move either, so the poll would lose it.
"""

from __future__ import annotations

import socket
import time
import uuid

import pytest

_PG = dict(host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow")


def _pg():
    import psycopg2

    conn = psycopg2.connect(**_PG)
    conn.autocommit = True
    return conn


def _wal_level() -> str:
    try:
        socket.create_connection((_PG["host"], _PG["port"]), timeout=2).close()
        conn = _pg()
    except Exception:
        return ""
    try:
        cur = conn.cursor()
        cur.execute("SHOW wal_level")
        return str(cur.fetchone()[0]).lower()
    finally:
        conn.close()


_LEVEL = _wal_level()

pytestmark = pytest.mark.skipif(
    _LEVEL in {"", "logical"},
    reason=f"needs a reachable PostgreSQL without logical decoding (wal_level={_LEVEL or 'unreachable'})",
)

_TERMINAL = {"completed", "completed_with_warnings", "failed", "blocked", "cancelled"}


@pytest.fixture()
def route():
    from services.connector_store import create_connector, delete_connector

    sfx = uuid.uuid4().hex[:6]
    src, dst = f"d28s_{sfx}", f"d28d_{sfx}"
    conn = _pg()
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE {src} (region TEXT PRIMARY KEY, total INT, note TEXT, "
        "updated_at TIMESTAMP NOT NULL)"
    )
    cur.executemany(
        f"INSERT INTO {src} VALUES (%s, %s, %s, %s)",  # nosec B608
        [
            ("east", 1, None, "2026-01-01 00:00:01"),
            ("west", 2, "", "2026-01-01 00:00:02"),
            ("nörd", 3, "naïve 中文 ✓", "2026-01-01 00:00:03"),
            ("lit", 5, "__DF_SQL_NULL__", "2026-01-01 00:00:04"),
        ],
    )
    name = f"D28PG{sfx}"
    saved = create_connector(
        {"name": name, "type": "postgresql", "host": _PG["host"], "port": _PG["port"],
         "database": _PG["dbname"], "username": _PG["user"], "password": _PG["password"],
         "schema": "public"}
    )
    try:
        yield {"conn": conn, "name": name, "src": src, "dst": dst}
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src}")
        cur.execute(f"DROP TABLE IF EXISTS {dst}")
        conn.close()
        delete_connector(saved.id)


def _args(route, **extra):
    args = {
        "source_connector_name": route["name"],
        "source_table": route["src"],
        "dest_connector_name": route["name"],
        "dest_table": route["dst"],
        "sync_mode": "cdc",
        "primary_key": "region",
    }
    args.update(extra)
    return args


def _run(args) -> dict:
    from conftest import spend_pilot_ack
    from services.mongodb_service import get_mongodb_service
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute("start_transfer", dict(args))
    assert staged.success, staged.error
    confirmed = spend_pilot_ack(staged.output["ack_id"], "d28-test")
    assert confirmed["ok"] is True, confirmed
    job_id = confirmed["job_id"]
    svc = get_mongodb_service()
    job: dict = {}
    deadline = time.time() + 180
    while time.time() < deadline:
        job = svc.get_job(job_id) or {}
        if job.get("status") in _TERMINAL:
            break
        time.sleep(0.5)
    return job


def _rows(conn, table: str) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(f"SELECT region, total, note, updated_at FROM {table} ORDER BY region")  # nosec B608
    return [tuple(r) for r in cur.fetchall()]


def test_declared_poll_cursor_runs_and_second_run_polls_it(route):
    args = _args(route, cursor_column="updated_at", cursor_semantics="modification_timestamp")
    first = _run(args)
    assert first.get("status") == "completed", (first.get("status"), first.get("error"))
    conn = route["conn"]
    assert _rows(conn, route["dst"]) == _rows(conn, route["src"])
    by_key = {r[0]: r for r in _rows(conn, route["dst"])}
    assert by_key["east"][2] is None, "SQL NULL must land as NULL, not sentinel text"
    assert by_key["west"][2] == ""
    assert by_key["lit"][2] == "__DF_SQL_NULL__", "a literal sentinel spelling is text"

    cur = conn.cursor()
    cur.execute(
        f"UPDATE {route['src']} SET total = 10, note = NULL, "  # nosec B608
        "updated_at = '2026-01-02 00:00:00' WHERE region = 'west'"
    )
    cur.execute(
        f"INSERT INTO {route['src']} VALUES ('south', 4, '  ', '2026-01-02 00:00:01')"  # nosec B608
    )
    # An edit that does not move the cursor is outside the poll by declaration.
    cur.execute(f"UPDATE {route['src']} SET total = 99 WHERE region = 'nörd'")  # nosec B608

    second = _run(args)
    assert second.get("status") == "completed", (second.get("status"), second.get("error"))
    got = {r[0]: r for r in _rows(conn, route["dst"])}
    src = {r[0]: r for r in _rows(conn, route["src"])}
    assert set(got) == set(src)
    for key in ("east", "west", "south", "lit"):
        assert got[key] == src[key], key
    assert got["west"][2] is None and got["west"][1] == 10
    assert got["south"][2] == "  "
    # Run 2 read past the watermark only: the cursor was polled, not the key.
    assert got["nörd"][1] == 3, "row outside the poll window must not be re-read"
    # ...and Gate-8 names it as out-of-window drift instead of failing (D188).
    drift = (second.get("reconciliation") or {}).get("out_of_window_drift") or {}
    assert drift.get("sample_keys") == ["nörd"], second.get("reconciliation")


def test_poll_on_the_primary_key_is_still_refused(route):
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute("start_transfer", _args(route))
    assert not staged.success
    assert "wal_level" in str(staged.error), staged.error


def test_undeclared_cursor_is_still_refused(route):
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute("start_transfer", _args(route, cursor_column="updated_at"))
    assert not staged.success
    assert "not declared as a modification timestamp" in str(staged.error), staged.error
    # QA: the advice must lead somewhere that runs. It names the declaration
    # test_declared_poll_cursor_runs_and_second_run_polls_it then follows.
    assert "cursor_semantics=modification_timestamp" in str(staged.error)
    assert "run this sync as incremental" not in str(staged.error)
