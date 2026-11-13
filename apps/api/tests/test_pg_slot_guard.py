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


def _nested_env(**extra: str) -> dict[str, str]:
    """The environment of a nested pytest run: never a CI shard selection.

    Under CI ``DATAFLOW_TEST_SHARD=i/6`` a one-file collection always hashes
    to one shard, so on every other shard the inner test was deselected
    (exit 5, "1 deselected"; the same as PR 423's 16d0180cd).
    """
    env = {k: v for k, v in os.environ.items()
           if k not in {"DATAFLOW_TEST_SHARD", "DATAWRAP_TEST_SHARD"}}
    return {**env, "PYTHONPATH": str(_API), **extra}


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
    env = _nested_env()
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


def test_a_fake_cursor_records_nothing():
    """Re-review M2: test_pg_slot_create_savepoint drives the creation site
    with a fake cursor named "df_slot"; the guard recorded it and later
    dropped the real df_slot on the shared server."""
    from unittest.mock import MagicMock

    import connectors.postgresql_change_stream as stream
    from tests.pg_slot_guard import CREATED

    cur = MagicMock()
    cur.fetchone.return_value = ("0/16B3748",)
    stream._create_logical_slot(cur, "df_slot_fake_g11", "pgoutput")
    assert "df_slot_fake_g11" not in CREATED


def test_a_foreign_slot_with_the_same_name_survives():
    """Our slot is dropped by its test; another process re-creates the name
    (another plugin). Session end must keep the foreign slot."""
    import connectors.postgresql_change_stream as stream
    from tests.pg_slot_guard import CREATED, drop_created_slots

    name = f"df_g11same_{uuid.uuid4().hex[:8]}"
    conn = _conn()
    try:
        conn.autocommit = False
        cur = conn.cursor()
        stream._create_logical_slot(cur, name, "pgoutput")
        conn.commit()
        conn.autocommit = True
        assert name in CREATED
        cur.execute("SELECT pg_drop_replication_slot(%s)", (name,))  # our test's teardown
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", (name,))
        assert name not in drop_created_slots()
        assert _slot_exists(name), "a foreign slot sharing the name was dropped"
    finally:
        conn.close()
        _drop(name)


def test_session_end_never_drops_another_sessions_slots(tmp_path: Path):
    """Concurrent sessions keep their slots (test_d344 lost its slot mid-test).

    ``conftest._drop_leaked_pg_replication_slots`` dropped every inactive
    ``df_dataflow_mt_%`` slot on the shared server at each session end, so a
    second pytest session finishing while test_d344 was between polls dropped
    d344's shared slot ("replication slot ... does not exist" ->
    CdcSlotGapError). A session now drops only slots it provably created
    (``tests.pg_slot_guard``); another session's slot, and the owner's retained
    ``df_dataflow_cdcord_*`` slot, are never touched.
    """
    token = uuid.uuid4().hex[:8]
    foreign = [f"df_dataflow_mt_{token}", f"df_dataflow_cdcord_g11{token}"]
    conn = _conn()
    try:
        cur = conn.cursor()
        for name in foreign:  # another session's idle slots (raw SQL: untracked here)
            cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (name,))
        env = _nested_env()
        done = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
             "tests/test_pg_slot_guard.py::test_a_fake_cursor_records_nothing",
             "--basetemp", str(tmp_path / "bt")],
            cwd=str(_API), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=300,
        )
        assert done.returncode == 0, done.stdout.decode()[-2000:]
        for name in foreign:
            assert _slot_exists(name), f"another session's slot {name} was dropped"
    finally:
        conn.close()
        for name in foreign:
            _drop(name)


def _killed_session(tmp_path: Path, name: str, ledger_dir: Path) -> None:
    """A pytest session that creates ``name`` at the engine site and is SIGKILLed."""
    test_file = tmp_path / "test_killed.py"
    test_file.write_text(textwrap.dedent(f"""
        import os, signal
        import psycopg2
        import connectors.postgresql_change_stream as stream

        def test_creates_a_slot_then_is_killed():
            conn = psycopg2.connect(host=os.environ.get("PGHOST", "127.0.0.1"),
                                    port=int(os.environ.get("PGPORT", "5432")),
                                    dbname=os.environ.get("PGDATABASE", "dataflow"),
                                    user=os.environ.get("PGUSER", "dataflow"),
                                    password=os.environ.get("PGPASSWORD", ""))
            cur = conn.cursor()
            stream._create_logical_slot(cur, {name!r}, "pgoutput")
            conn.commit()
            conn.close()
            os.kill(os.getpid(), signal.SIGKILL)  # no finally, no session end
    """))
    env = _nested_env(DATAFLOW_TEST_SLOT_LEDGER=str(ledger_dir))
    subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p",
         "tests.pg_slot_guard", str(test_file), "--rootdir", str(tmp_path),
         "--basetemp", str(tmp_path / "bt")],
        cwd=str(_API), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300,
    )


def test_slots_of_a_sigkilled_session_are_dropped_by_the_next_session(tmp_path: Path,
                                                                      monkeypatch):
    """Final review: with the prefix sweep gone, SIGKILLed sessions' slots piled up.

    Each session writes a ledger (session id, pid and start time, and each
    slot's identity). A later session drops a ledger's slots only when that
    pid is gone AND the slot still has the recorded identity and is inactive.
    """
    import tests.pg_slot_guard as guard

    _conn().close()
    ledger = tmp_path / "ledger"
    ours, foreign = f"df_g11kill_{uuid.uuid4().hex[:8]}", f"df_dataflow_cdcord_g11k{uuid.uuid4().hex[:6]}"
    conn = _conn()
    try:
        conn.cursor().execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (foreign,))
        _killed_session(tmp_path, ours, ledger)
        assert _slot_exists(ours), "the killed session's slot should still be there"
        files = list(ledger.glob("*.json"))
        assert len(files) == 1 and ours in files[0].read_text()
        monkeypatch.setenv("DATAFLOW_TEST_SLOT_LEDGER", str(ledger))
        dropped = guard.sweep_dead_sessions()
        assert ours in dropped and not _slot_exists(ours)
        assert _slot_exists(foreign), "an unrecorded slot must never be touched"
        assert not list(ledger.glob("*.json")), "the dead session's ledger is done"
    finally:
        conn.close()
        _drop(ours)
        _drop(foreign)


def test_a_live_or_foreign_ledger_entry_is_never_dropped(tmp_path: Path, monkeypatch):
    import json

    import tests.pg_slot_guard as guard

    ledger = tmp_path / "ledger"
    ledger.mkdir()
    monkeypatch.setenv("DATAFLOW_TEST_SLOT_LEDGER", str(ledger))
    alive, renamed = f"df_g11live_{uuid.uuid4().hex[:8]}", f"df_g11same_{uuid.uuid4().hex[:8]}"
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (alive,))
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", (renamed,))
        cur.execute("SELECT current_database()")
        db = cur.fetchone()[0]
        # This very process (alive) recorded ``alive``; a dead process recorded
        # ``renamed`` with another plugin: neither proof holds.
        (ledger / "live.json").write_text(json.dumps({
            "session": "live", "pid": os.getpid(), "start": guard._start_time(os.getpid()),
            "host": guard._host(), "slots": {alive: {"database": db, "plugin": "pgoutput",
                                                       "restart_lsn": "0/0"}}}))
        (ledger / "dead.json").write_text(json.dumps({
            "session": "dead", "pid": 2_000_000_000, "start": "1", "host": guard._host(),
            "slots": {renamed: {"database": db, "plugin": "pgoutput", "restart_lsn": "0/0"}}}))
        assert guard.sweep_dead_sessions() == []
        assert _slot_exists(alive) and _slot_exists(renamed)
        assert (ledger / "live.json").exists()
    finally:
        conn.close()
        _drop(alive)
        _drop(renamed)
