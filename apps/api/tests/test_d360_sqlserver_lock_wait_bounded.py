"""D360 — a SQL Server job sat at "Writing batch 1/1" long after its rows landed.

Two waits after the last batch, both live against the local SQL Server:

* The staging reaper (mirror ``_df_mirrorkeys_*`` / SCD2 spools) bounds its
  own lock wait to 2 s, then "resets" the session with ``SET LOCK_TIMEOUT -1``.
  Every pooled SQL Server session carries the 120 s destination bound from
  connect (D19); -1 is "wait forever". That pooled session then hung any later
  statement queued behind another session's lock, in the same run or the next.
* The mirror delete step treated a lock-wait timeout on its COUNTs as
  "unmeasured" and went on to the next COUNT and the UPDATE/DELETE, each of
  which waited the whole bound again behind the same lock, with the job still
  on "Writing batch 1/1" (measured: 360 s after the batch at the 120 s bound).

The lock bound is lowered for the tests (``DATAFLOW_DEST_LOCK_WAIT_SECONDS``);
the default stays 120 s.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import threading
import time
import uuid

import pytest

_LOCK_WAIT_S = 2

_MS = {
    "host": os.getenv("DATAFLOW_TEST_MSSQL_HOST", ""),
    "port": int(os.getenv("DATAFLOW_TEST_MSSQL_PORT", "1433")),
    "database": os.getenv("DATAFLOW_TEST_MSSQL_DB", "dataflow"),
    "username": os.getenv("DATAFLOW_TEST_MSSQL_USER", ""),
    "password": os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
}


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
        return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.skipif(
        not (_MS["host"] and _MS["password"] and _reachable(_MS["host"], _MS["port"])),
        reason="live SQL Server: set DATAFLOW_TEST_MSSQL_* (reachable)",
    ),
    pytest.mark.timeout(120),
]


def _connect(*, autocommit: bool = True):
    import pyodbc

    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={_MS['host']},{_MS['port']};DATABASE={_MS['database']};"
        f"UID={_MS['username']};PWD={_MS['password']};TrustServerCertificate=yes",
        autocommit=autocommit,
        timeout=10,
    )
    # The ODBC driver manager pools closed handles: a handle closed in manual
    # commit mode comes back still in it, and the cleanup DROP is then rolled
    # back at close. Setting the attribute again applies it to the handle.
    conn.autocommit = autocommit
    conn.timeout = 60  # a hung test never holds a session open for long
    return conn


@pytest.fixture
def lock_bound(monkeypatch):
    monkeypatch.setenv("DATAFLOW_DEST_LOCK_WAIT_SECONDS", str(_LOCK_WAIT_S))
    return _LOCK_WAIT_S


def test_reaped_pooled_session_keeps_the_destination_lock_bound(lock_bound):
    """After a sweep, a statement behind another session's lock ends at the bound."""
    import sqlalchemy as sa

    from connectors.generic_sql import _build_engine, with_connection_options
    from services.staging_reaper import reap_orphan_staging

    tag = uuid.uuid4().hex[:8]
    prefix = f"_df_d360_{tag}_"
    orphan, table = f"{prefix}legacy", f"d360r_{tag}"  # unstamped: age unknown
    admin = _connect()
    holder = _connect(autocommit=False)
    engine = _build_engine(with_connection_options({
        "type": "sqlserver", **_MS, "extra": {"trust_server_certificate": True},
    }))
    try:
        admin.execute(f"CREATE TABLE dbo.[{orphan}] (id INT)")
        admin.execute(f"CREATE TABLE dbo.[{table}] (id INT PRIMARY KEY)")
        admin.execute(f"INSERT INTO dbo.[{table}] VALUES (1)")
        holder.execute(f"INSERT INTO dbo.[{table}] VALUES (2)")  # uncommitted

        with engine.connect() as conn:
            assert reap_orphan_staging(conn, prefix, "dbo", "mssql") == [orphan]
            outcome: dict = {}

            def count() -> None:
                started = time.monotonic()
                try:
                    conn.execute(sa.text(f"SELECT COUNT(*) FROM dbo.[{table}]")).scalar()
                    outcome["error"] = None
                except Exception as exc:  # noqa: BLE001 - the outcome is asserted
                    outcome["error"] = exc
                outcome["seconds"] = time.monotonic() - started

            worker = threading.Thread(target=count, daemon=True)
            worker.start()
            worker.join(lock_bound + 8)
            hung = worker.is_alive()
            holder.rollback()  # releases a hung COUNT so the test can clean up
            worker.join(30)
            assert not hung, (
                "the reaped pooled session waited past the destination lock bound "
                f"({lock_bound}s) behind another session's lock"
            )
            from connectors.write_resilience import is_lock_wait_timeout

            assert is_lock_wait_timeout(outcome["error"]), outcome
            assert outcome["seconds"] < lock_bound + 5, outcome
            conn.rollback()
            lock_ms = conn.execute(sa.text("SELECT @@LOCK_TIMEOUT")).scalar()
            assert lock_ms == lock_bound * 1000
    finally:
        try:
            holder.rollback()
        finally:
            holder.close()
        engine.dispose()
        admin.execute(f"DROP TABLE IF EXISTS dbo.[{orphan}]")
        admin.execute(f"DROP TABLE IF EXISTS dbo.[{table}]")
        admin.close()


def test_mirror_into_a_locked_table_fails_after_one_lock_wait(lock_bound, monkeypatch, tmp_path):
    """The mirror delete step stops at the first lock timeout and names the lock."""
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def __init__(self) -> None:
            super().__init__()
            self.timeline: list[tuple[float, str, str]] = []

        def update_job_status(self, job_id, status, **kwargs):
            self.timeline.append((time.monotonic(), status, str(kwargs.get("message") or "")))
            return super().update_job_status(job_id, status, **kwargs)

        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)

    src_path = tmp_path / "d360_src.db"
    src = sqlite3.connect(src_path)
    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    src.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"n{i}") for i in range(1, 26)])
    src.commit()
    src.close()
    table = "d360m_" + uuid.uuid4().hex[:8]

    def run() -> tuple[object, list[tuple[float, str, str]]]:
        job_id = "d360" + uuid.uuid4().hex[:12]
        fake.update_job_status(job_id, "pending", transfer_request={})
        start = len(fake.timeline)
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(
                    kind="database", format="sqlite", database=str(src_path), table="t"
                ),
                destination=EndpointConfig(
                    kind="database", format="sqlserver", schema="dbo", table=table,
                    extra={"trust_server_certificate": True}, **_MS,
                ),
                sync_mode="mirror",
                validation_mode="strict",
                skip_preflight=True,
            ),
            job_id,
        )
        return result, fake.timeline[start:]

    admin = _connect()
    holder = _connect(autocommit=False)
    try:
        first, _ = run()
        assert first.success, first.error
        holder.execute(f"INSERT INTO dbo.[{table}] (id) VALUES (9999)")  # uncommitted

        second, timeline = run()

        assert not second.success
        assert "locked" in str(second.error).lower(), second.error
        batch_at = max(t for t, _, msg in timeline if "Writing batch 1/1" in msg)
        ended_at = timeline[-1][0]
        # One bounded lock wait after the batch, not one per statement.
        assert ended_at - batch_at < 2 * lock_bound, (
            f"{ended_at - batch_at:.1f}s after 'Writing batch 1/1' "
            f"(lock bound {lock_bound}s)"
        )
    finally:
        try:
            holder.rollback()
        finally:
            holder.close()
        admin.execute(f"DROP TABLE IF EXISTS dbo.[{table}]")
        admin.close()
