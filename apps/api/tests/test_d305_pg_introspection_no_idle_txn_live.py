"""D305: PostgreSQL metadata reads leave no session idle in transaction.

QA (seen once): ``SELECT table_name, table_type FROM information_schema.tables``
was the last query of a session ``idle in transaction`` for 10+ minutes,
holding catalog AccessShareLocks and a virtualxid, and it outlived the
connector's deletion.

psycopg2 opens a transaction on the first statement of a non-autocommit
session. A metadata read then has to end it: closing the session does, but
inside an endpoint-session scope (one per MCP tool call, D187) ``close``
hands the session back to the scope's pool for the next consumer, and the
transaction stayed open until that consumer reset it or the scope ended. A
read that raised before ``close`` kept its transaction for as long as
anything referenced the connection.

Each case runs a real introspection against local PostgreSQL and then reads
``pg_stat_activity`` live, while the connector's sessions are still held
(inside the scope), for any session whose last query names this test's
unique schema and that is still in a transaction.
"""

from __future__ import annotations

import uuid

import pytest


def _admin():
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not configured")
    import psycopg2

    p = pg_creds()
    conn = psycopg2.connect(
        host=p["host"], port=int(p["port"]), user=p["username"],
        password=p["password"], dbname=p["database"],
    )
    conn.autocommit = True
    return p, conn


def _open_txns(admin, marker: str) -> list[tuple]:
    """Sessions of this test still in a transaction, after 2 s of grace.

    A client-side close reaches the server a moment later; a session that is
    still in its transaction after the grace period was never ended.
    """
    import time

    deadline = time.monotonic() + 2.0
    found = _open_txns_now(admin, marker)
    while found and time.monotonic() < deadline:
        time.sleep(0.05)
        found = _open_txns_now(admin, marker)
    return found


def _open_txns_now(admin, marker: str) -> list[tuple]:
    with admin.cursor() as cur:
        cur.execute(
            "SELECT pid, state, left(query, 120) FROM pg_stat_activity "
            "WHERE pid <> pg_backend_pid() AND query LIKE %s "
            "AND (state LIKE 'idle in transaction%%' OR xact_start IS NOT NULL)",
            (f"%{marker}%",),
        )
        return list(cur.fetchall())


@pytest.fixture()
def pg_schema():
    p, admin = _admin()
    marker = f"d305_{uuid.uuid4().hex[:8]}"
    with admin.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{marker}"')
        cur.execute(f'CREATE TABLE "{marker}".items (id INT PRIMARY KEY, name TEXT)')
    try:
        yield p, admin, marker
    finally:
        with admin.cursor() as cur:
            cur.execute("SET lock_timeout = '10s'")
            cur.execute(f'DROP SCHEMA IF EXISTS "{marker}" CASCADE')
        admin.close()


def _kwargs(p: dict, marker: str) -> dict:
    return dict(
        host=str(p["host"]), port=int(p["port"]), database=str(p["database"]),
        username=str(p["username"]), password=str(p["password"]),
        connection_string="", ssl=False, schema=marker,
    )


def _probe(p: dict, marker: str) -> None:
    from connectors.postgresql import test_postgresql

    res = test_postgresql(**_kwargs(p, marker))
    assert res.ok and "items" in res.tables, res


def _introspect(p: dict, marker: str) -> None:
    from services.schema_introspect import _introspect_postgresql

    out = _introspect_postgresql(table="items", **_kwargs(p, marker))
    assert out.get("ok") and [c.get("name") for c in out["columns"]] == ["id", "name"], out


@pytest.mark.parametrize("read", [_probe, _introspect], ids=["probe", "introspect"])
def test_metadata_read_inside_a_tool_scope_ends_its_transaction(pg_schema, read):
    from services.endpoint_session import endpoint_session_scope, scope_stats

    p, admin, marker = pg_schema
    with endpoint_session_scope():
        read(p, marker)
        # The pooled session is still held by the scope for the next consumer.
        assert scope_stats().get("postgresql", {}).get("opened") == 1, scope_stats()
        assert _open_txns(admin, marker) == []
        # The next consumer reuses it and still reads correctly.
        read(p, marker)
        assert scope_stats()["postgresql"]["reused"] >= 1, scope_stats()
        assert _open_txns(admin, marker) == []


@pytest.mark.parametrize("read", [_probe, _introspect], ids=["probe", "introspect"])
def test_metadata_read_outside_a_scope_ends_its_transaction(pg_schema, read):
    p, admin, marker = pg_schema
    read(p, marker)
    assert _open_txns(admin, marker) == []


def test_a_metadata_read_that_fails_midway_ends_its_transaction(pg_schema, monkeypatch):
    """A failure after the first catalog query must not leave the session in a
    transaction while the caller still holds the error (and so the session)."""
    import services.schema_introspect as si
    from services.endpoint_session import endpoint_session_scope

    p, admin, marker = pg_schema

    def boom(*_a, **_k):
        raise RuntimeError("synthetic catalog failure")

    monkeypatch.setattr(si, "_pg_fetch_unique_keys", boom)
    with endpoint_session_scope():
        out = si._introspect_postgresql(table="items", **_kwargs(p, marker))
        held = out  # the caller keeps the failure (and its traceback) around
        assert not held.get("ok"), held
        assert _open_txns(admin, marker) == []
