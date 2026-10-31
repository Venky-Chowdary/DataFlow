"""PostgreSQL log CDC attaches while other transactions are open (QA round 11).

QA (production 9c75ada4, CDC row): "First attempt: could not attach:
statement timeout while other load was running". Root cause:
``PostgreSqlChangeStreamCdc.is_available`` created and dropped a throwaway
``<slot>_avail_test`` logical slot under ``SET LOCAL statement_timeout =
5000ms``. Creating a logical slot waits for every transaction open at that
moment to finish, so any write transaction older than five seconds failed the
probe; the message was classified ``unknown`` with no remedy, and the real
slot was then created again (the snapshot wait paid twice).

Through ``UniversalTransferEngine().execute_tracked`` (sync_mode ``cdc``) from
a live PostgreSQL with ``wal_level=logical`` into the live PostgreSQL on
:5432: a concurrent transaction holding an xid is committed after eight
seconds; the run must attach, snapshot NULL / '' / whitespace / unicode rows
cell by cell, and a ``cdc_incremental`` second run into the table it created
must apply an UPDATE to NULL, inserts and a DELETE.

The source cluster is ``R11_PG_LOGICAL_{HOST,PORT,DATABASE,USER,PASSWORD}``
(defaults: the ``PG*`` server); the test skips unless it runs
``wal_level=logical``.
"""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.timeout(300)

_SRC = {
    "host": os.getenv("R11_PG_LOGICAL_HOST", os.getenv("PGHOST", "127.0.0.1")),
    "port": int(os.getenv("R11_PG_LOGICAL_PORT", os.getenv("PGPORT", "5432"))),
    "database": os.getenv("R11_PG_LOGICAL_DATABASE", os.getenv("PGDATABASE", "dataflow")),
    "username": os.getenv("R11_PG_LOGICAL_USER", os.getenv("PGUSER", "dataflow")),
    "password": os.getenv("R11_PG_LOGICAL_PASSWORD", os.getenv("PGPASSWORD", "")),
}
_DST = {
    "host": os.getenv("PGHOST", "127.0.0.1"),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", "dataflow"),
    "username": os.getenv("PGUSER", "dataflow"),
    "password": os.getenv("PGPASSWORD", ""),
}


def _connect(cfg: dict, *, autocommit: bool = True):
    import psycopg2

    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
        user=cfg["username"], password=cfg["password"], connect_timeout=3,
    )
    conn.autocommit = autocommit
    return conn


def _logical_source() -> bool:
    try:
        socket.create_connection((_SRC["host"], _SRC["port"]), timeout=1).close()
        conn = _connect(_SRC)
        try:
            cur = conn.cursor()
            cur.execute("SHOW wal_level")
            return cur.fetchone()[0] == "logical"
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        return False


def _dest_up() -> bool:
    try:
        _connect(_DST).close()
        return True
    except Exception:  # noqa: BLE001
        return False


needs_pg = pytest.mark.skipif(
    not (_logical_source() and _dest_up()),
    reason="needs a PostgreSQL with wal_level=logical (R11_PG_LOGICAL_*) and PG* destination",
)


def _sql(cfg: dict, sql: str, params=None, fetch: bool = False):
    conn = _connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [tuple(r) for r in cur.fetchall()] if fetch else None
    finally:
        conn.close()


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


_SNAPSHOT = [
    (1, "alpha"),
    (2, None),
    (3, ""),
    (4, "   "),
    (5, "naïve 中文 ✓ 😀"),
]


def _request(table: str, sync: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public",
                              table=table, **_SRC),
        destination=EndpointConfig(kind="database", format="postgresql", schema="public",
                                   table=table, **_DST),
        sync_mode=sync,
        stream_contracts=[
            {"name": table, "selected": True, "primary_key": ["id"], "sync_mode": sync,
             "cursor_semantics": "cdc_position"}
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


def _run(fake, table: str, sync: str):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "r11cdc" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(table, sync), job_id)


def _image(cfg: dict, table: str) -> dict[int, tuple]:
    rows = _sql(cfg, f'SELECT id, name FROM "{table}" ORDER BY id', fetch=True)  # nosec B608
    return {int(r[0]): (int(r[0]), r[1]) for r in rows}


def _cleanup(table: str) -> None:
    slots = _sql(_SRC, "SELECT slot_name, active FROM pg_replication_slots", fetch=True)
    for name, active in slots:
        if table in name and not active:
            _sql(_SRC, "SELECT pg_drop_replication_slot(%s)", (name,))
    for (pub,) in _sql(_SRC, "SELECT pubname FROM pg_publication WHERE pubname LIKE %s",
                       (f"%{table}%",), fetch=True):
        _sql(_SRC, f'DROP PUBLICATION IF EXISTS "{pub}"')
    _sql(_SRC, f'DROP TABLE IF EXISTS "{table}"')
    _sql(_SRC, f'DROP TABLE IF EXISTS "{table}_busy"')
    _sql(_DST, f'DROP TABLE IF EXISTS "{table}"')


@needs_pg
def test_cdc_attaches_while_a_long_write_transaction_is_open(isolated):
    table = "r11cdc_" + uuid.uuid4().hex[:8]
    _sql(_SRC, f'CREATE TABLE "{table}" (id INT PRIMARY KEY, name TEXT)')
    _sql(_SRC, f'ALTER TABLE "{table}" REPLICA IDENTITY FULL')
    conn = _connect(_SRC)
    conn.cursor().executemany(f'INSERT INTO "{table}" VALUES (%s, %s)', _SNAPSHOT)  # nosec B608
    conn.close()
    _sql(_SRC, f'CREATE TABLE "{table}_busy" (id INT)')
    # "Other load": a write transaction that holds its xid for eight seconds.
    busy = _connect(_SRC, autocommit=False)
    busy.cursor().execute(f'INSERT INTO "{table}_busy" VALUES (1)')  # nosec B608
    released = threading.Timer(8.0, busy.commit)
    released.start()
    try:
        started = time.monotonic()
        first = _run(isolated, table, "cdc")
        assert first.success, first.error
        assert time.monotonic() - started >= 7.0, "the attach did not wait for the open transaction"
        assert _image(_DST, table) == _image(_SRC, table) == {r[0]: r for r in _SNAPSHOT}
        # No throwaway probe slot is left behind or created.
        names = [r[0] for r in _sql(_SRC, "SELECT slot_name FROM pg_replication_slots", fetch=True)]
        assert not [n for n in names if n.endswith("_avail_test")], names

        # Second run into the table the first run created.
        _sql(_SRC, f'UPDATE "{table}" SET name = NULL WHERE id = 1')
        conn = _connect(_SRC)
        conn.cursor().executemany(
            f'INSERT INTO "{table}" VALUES (%s, %s)',  # nosec B608
            [(6, ""), (7, " \t "), (8, "ünï 表"), (9, None)],
        )
        conn.close()
        _sql(_SRC, f'DELETE FROM "{table}" WHERE id = 3')
        second = _run(isolated, table, "cdc_incremental")
        assert second.success, second.error
        assert _image(_DST, table) == _image(_SRC, table)
        assert _image(_DST, table)[1] == (1, None) and 3 not in _image(_DST, table)
    finally:
        released.cancel()
        try:
            busy.rollback()
        except Exception:  # noqa: BLE001
            pass
        busy.close()
        _cleanup(table)


@needs_pg
def test_attach_without_replication_privilege_is_refused_at_once_with_the_remedy():
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    role = "r11norep_" + uuid.uuid4().hex[:6]
    table = "r11cdcp_" + uuid.uuid4().hex[:8]
    _sql(_SRC, f'CREATE TABLE "{table}" (id INT PRIMARY KEY)')
    try:
        _sql(_SRC, f"CREATE ROLE {role} LOGIN PASSWORD 'r11-norep-pw' NOREPLICATION NOSUPERUSER")
    except Exception as exc:  # noqa: BLE001
        _sql(_SRC, f'DROP TABLE IF EXISTS "{table}"')
        pytest.skip(f"cannot create a probe role on the source: {exc}")
    _sql(_SRC, f'GRANT SELECT ON "{table}" TO {role}')
    busy = _connect(_SRC, autocommit=False)
    busy.cursor().execute("SELECT txid_current()")
    try:
        cdc = PostgreSqlChangeStreamCdc(
            {**_SRC, "username": role, "password": "r11-norep-pw", "connection_string": "",
             "ssl": False},
            table=table, primary_key="id", schema="public", cursor_key=f"r11-{table}",
        )
        started = time.monotonic()
        assert cdc.is_available() is False
        assert time.monotonic() - started < 5.0
        assert cdc.unavailable_reason.cause == "insufficient_privilege", cdc.unavailable_reason
        assert "REPLICATION" in cdc.unavailable_reason.remedy
    finally:
        busy.rollback()
        busy.close()
        _sql(_SRC, f'DROP TABLE IF EXISTS "{table}"')
        _sql(_SRC, f"DROP ROLE IF EXISTS {role}")


@pytest.mark.parametrize(
    "detail",
    [
        "canceling statement due to statement timeout",
        "ERROR:  canceling statement due to lock timeout",
        "could not attach: statement timeout",
    ],
)
def test_a_busy_source_is_classified_with_a_remedy(detail):
    from services.cdc_capability import classify_log_capture_failure

    refusal = classify_log_capture_failure("postgresql", detail)
    assert refusal.cause == "source_busy", refusal
    assert refusal.fail_closed is True
    assert "pg_stat_activity" in refusal.remedy
