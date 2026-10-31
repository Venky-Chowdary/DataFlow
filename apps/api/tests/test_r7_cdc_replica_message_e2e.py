"""CDC on PostgreSQL with wal_level=replica: the block must say exactly what to do.

QA (production 557f556c): the demo table has no modification-timestamp
column and the server has wal_level=replica, so Validate blocks CDC — that is
correct (round 5's D28 polls only a *declared* cursor). The message, though,
read "log-based CDC is not available (server_not_configured) … Enable the
source change log, or name a column …": an internal code, no PostgreSQL steps,
and no mention that the table has no such column to name.

Runs the real g9c gate against the live local PostgreSQL (wal_level=replica).
"""

from __future__ import annotations

import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")


def _cfg() -> dict:
    from conftest import pg_test_cfg

    return {"type": "postgresql", **pg_test_cfg()}


def _connect(cfg):
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
        user=cfg["username"], password=cfg["password"], connect_timeout=3,
    )


@pytest.fixture
def replica_table():
    cfg = _cfg()
    try:
        conn = _connect(cfg)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SHOW wal_level")
        if cur.fetchone()[0] == "logical":
            conn.close()
            pytest.skip("needs a PostgreSQL with wal_level=replica")
        name = f"r7cdc_{uuid.uuid4().hex[:8]}"
        # QA's shape: a key, data columns, no modification timestamp.
        cur.execute(f"CREATE TABLE {name} (id INT PRIMARY KEY, region TEXT, amount NUMERIC, note TEXT)")
    try:
        yield cfg, name
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {name}")
        conn.close()


def _g9c(cfg, table, contract):
    from services.preflight_service import _cdc_log_capture_gates

    gates = _cdc_log_capture_gates(
        [{"name": table, **contract}],
        source_type="postgresql",
        source_config=cfg,
        source_table=table,
        catalog_primary_key_columns=["id"],
        mappings=None,
    )
    found = [g for g in gates if g["id"] == "g9c_cdc_log_capture"]
    assert found, gates
    return found[0]


def _assert_actionable(msg: str) -> None:
    # No internal cause codes on screen.
    assert "server_not_configured" not in msg
    # Option 1: logical decoding, with the PostgreSQL steps.
    assert "logical decoding" in msg
    assert "wal_level = logical" in msg
    assert "restart" in msg.lower()
    assert "REPLICATION" in msg
    # Option 2: add (or declare) a modification-timestamp column.
    assert "add a column" in msg.lower()
    assert "modification_timestamp" in msg
    # The poll is refused for the reason that applies.
    assert "wal_level=replica" in msg


def test_table_without_a_timestamp_is_blocked_with_exact_steps(replica_table):
    cfg, table = replica_table
    gate = _g9c(cfg, table, {})
    assert gate["status"] == "block"
    msg = gate["message"]
    _assert_actionable(msg)
    assert "primary key" in msg


def test_undeclared_cursor_is_blocked_with_exact_steps(replica_table):
    cfg, table = replica_table
    gate = _g9c(cfg, table, {"cursor_column": "region"})
    assert gate["status"] == "block"
    msg = gate["message"]
    _assert_actionable(msg)
    assert "not declared as a modification timestamp" in msg


def test_declared_timestamp_still_polls(replica_table):
    """D28 unchanged: a declared modification timestamp is polled, not blocked."""
    cfg, table = replica_table
    gate = _g9c(cfg, table, {"cursor_column": "note", "cursor_semantics": "modification_timestamp"})
    assert gate["status"] != "block"
