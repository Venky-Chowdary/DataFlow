"""Root cause of the WAL growth: CDC test slots must not outlive their session.

``df_dataflow_cdcord_e3d64445_3d389a74`` (from
``test_identity_cdc_matrix.py::test_cdc_cross_transaction_order_per_key``)
stayed inactive and pinned 1.76 GB of WAL: the test's teardown dropped only
``NOT active`` slots, and a run stopped with SIGTERM skips ``finally``.

``tests/pg_slot_guard`` records each slot the engine creates in this process,
turns SIGTERM into an unwind, and drops leftovers at session end; it never
touches another session's slot. Live PostgreSQL with ``wal_level=logical``.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import time
import uuid
from pathlib import Path

import pytest

_API = Path(__file__).resolve().parents[1]


def _conn():
    import psycopg2

    try:
        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "127.0.0.1"), port=int(os.environ.get("PGPORT", "5432")),
            dbname=os.environ.get("PGDATABASE", "dataflow"),
            user=os.environ.get("PGUSER", "dataflow"),
            password=os.environ.get("PGPASSWORD", ""), connect_timeout=3)
    except Exception as exc:  # noqa: BLE001 - no server, no test
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SHOW wal_level")
    if cur.fetchone()[0] != "logical":
        conn.close()
        pytest.skip("wal_level is not logical")
    return conn


def _slot_exists(name: str) -> bool:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (name,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def _drop(name: str) -> None:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                    "WHERE slot_name = %s AND NOT active", (name,))
    finally:
        conn.close()


def test_guard_drops_only_the_slots_this_process_created():
    import connectors.postgresql_change_stream as stream
    from tests.pg_slot_guard import CREATED, drop_created_slots

    ours, theirs = f"df_g11guard_{uuid.uuid4().hex[:8]}", f"df_g11other_{uuid.uuid4().hex[:8]}"
    conn = _conn()
    try:
        cur = conn.cursor()
        conn.autocommit = False
        stream._create_logical_slot(cur, ours, "pgoutput")  # the engine's creation site
        conn.commit()
        conn.autocommit = True
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (theirs,))
        assert ours in CREATED and theirs not in CREATED
        assert ours in drop_created_slots()
        assert not _slot_exists(ours)
        assert _slot_exists(theirs), "another session's slot must be left alone"
    finally:
        conn.close()
        _drop(ours)
        _drop(theirs)


def test_sigterm_mid_run_still_drops_the_slot(tmp_path: Path):
    _conn().close()
    name = f"df_g11term_{uuid.uuid4().hex[:8]}"
    test_file = tmp_path / "test_holds_slot.py"
    test_file.write_text(textwrap.dedent(f"""
        import time
        import psycopg2, os
        import connectors.postgresql_change_stream as stream

        def test_holds_a_slot_then_is_killed():
            conn = psycopg2.connect(host=os.environ.get("PGHOST", "127.0.0.1"),
                                    port=int(os.environ.get("PGPORT", "5432")),
                                    dbname=os.environ.get("PGDATABASE", "dataflow"),
                                    user=os.environ.get("PGUSER", "dataflow"),
                                    password=os.environ.get("PGPASSWORD", ""))
            cur = conn.cursor()
            stream._create_logical_slot(cur, {name!r}, "pgoutput")
            conn.commit()
            time.sleep(120)  # killed here, before any teardown of its own
    """))
    env = {**os.environ, "PYTHONPATH": str(_API)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p",
         "tests.pg_slot_guard", str(test_file), "--rootdir", str(tmp_path),
         "--basetemp", str(tmp_path / "bt")],
        cwd=str(_API), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    try:
        deadline = time.time() + 60
        while not _slot_exists(name):
            assert proc.poll() is None, proc.stdout.read().decode()[-2000:]
            assert time.time() < deadline, "the inner run never created its slot"
            time.sleep(0.5)
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=60)
        assert not _slot_exists(name), "SIGTERM left the CDC slot behind"
    finally:
        if proc.poll() is None:
            proc.kill()
        _drop(name)
