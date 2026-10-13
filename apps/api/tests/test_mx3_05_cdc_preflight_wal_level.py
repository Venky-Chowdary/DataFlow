"""QA MX3-05 — CDC preflight approved cdc_incremental on PostgreSQL with wal_level=replica.

The failure only surfaced after Confirm ("log-based CDC is not available
(server_not_configured)"). ``probe_log_capture`` deferred every Postgres
source to the slot probe, but the slot probe only runs when a stored LSN
watermark exists — a first CDC run had no Postgres log-capture gate at all.

Live: df-pg2 (:5433, wal_level=replica) and df-pg (:5432, wal_level=logical).
"""

from __future__ import annotations

import uuid

import pytest

from services.cdc_log_capture_probe import probe_log_capture
from services.preflight_service import _cdc_log_capture_gates

psycopg2 = pytest.importorskip("psycopg2")


def _cfg(port: int) -> dict:
    return {
        "type": "postgresql",
        "host": "localhost",
        "port": port,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }


def _wal_level(port: int) -> str | None:
    try:
        conn = psycopg2.connect(
            host="localhost", port=port, dbname="dataflow", user="dataflow",
            password="dataflow", connect_timeout=3,
        )
    except psycopg2.OperationalError:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            return cur.fetchone()[0]
    finally:
        conn.close()


@pytest.fixture
def table_on():
    made: list[tuple[int, str]] = []

    def _make(port: int) -> str:
        name = f"mx305_{uuid.uuid4().hex[:8]}"
        conn = psycopg2.connect(
            host="localhost", port=port, dbname="dataflow", user="dataflow", password="dataflow"
        )
        with conn, conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {name} (id INT PRIMARY KEY, v TEXT)")
        conn.close()
        made.append((port, name))
        return name

    yield _make
    for port, name in made:
        conn = psycopg2.connect(
            host="localhost", port=port, dbname="dataflow", user="dataflow", password="dataflow"
        )
        with conn, conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {name}")
        conn.close()


def _need(port: int, level: str) -> None:
    got = _wal_level(port)
    if got != level:
        pytest.skip(f"needs a PostgreSQL with wal_level={level} on :{port} (got {got})")


def test_probe_refuses_replica_wal_level(table_on):
    _need(5433, "replica")
    table = table_on(5433)
    probe = probe_log_capture("postgresql", _cfg(5433), table=table, primary_key="id")
    assert probe.available is False
    assert probe.cause == "server_not_configured"
    assert "wal_level=replica" in probe.detail


def test_preflight_gate_blocks_first_cdc_run_on_replica(table_on):
    _need(5433, "replica")
    table = table_on(5433)
    gates = _cdc_log_capture_gates(
        [{"name": table}],
        source_type="postgresql",
        source_config=_cfg(5433),
        source_table=table,
        catalog_primary_key_columns=["id"],
        mappings=None,
    )
    g9c = [g for g in gates if g["id"] == "g9c_cdc_log_capture"]
    assert g9c, "no CDC log-capture gate for a wal_level=replica PostgreSQL source"
    assert g9c[0]["status"] == "block"
    assert g9c[0]["details"]["cause"] == "server_not_configured"


def test_logical_server_is_not_blocked(table_on):
    _need(5432, "logical")
    table = table_on(5432)
    probe = probe_log_capture("postgresql", _cfg(5432), table=table, primary_key="id")
    assert probe.available is not False


def test_unreachable_postgres_stays_undecided():
    cfg = {**_cfg(1), "host": "127.0.0.1"}
    probe = probe_log_capture("postgresql", cfg, table="t", primary_key="id")
    assert probe.available is None
