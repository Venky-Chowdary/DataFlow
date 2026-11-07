"""Review M2: two jobs on one table must never regress it while both report green.

Without a lock: A reads (ap, 2) = 2.75 and pauses before writing; the source
moves to 3.00; B reads 3.00, writes and verifies; A then writes 2.75 late. A's
ladder sees source 3.00 and destination 2.75 == what A wrote, and excused it
as a concurrent source update: the table ends on a stale value, both green.

Now a keyed upsert holds an exclusive advisory lock on the destination table
from before its read until it ends, and every other DataFlow writer of the
table takes it shared, so B waits for A and lands 3.00 last.
"""

from __future__ import annotations

import pytest

import threading
import uuid
from decimal import Decimal

import services.destination_write_lock as dwl
import src.transfer.stream  # noqa: F401 — import order: stream before stream_db_chunk
import src.transfer.stream_db_chunk as chunk_mod
from tests.test_r14_ladder_composite_key_scope_live import _dest, _run, world  # noqa: F401
from tests.test_r14_witness_review_live import _run_as



@pytest.fixture(autouse=True)
def _destination_write_lock_on(monkeypatch):
    """These tests exercise attribution, which needs DATAFLOW_DEST_WRITE_LOCK=1."""
    monkeypatch.setenv("DATAFLOW_DEST_WRITE_LOCK", "1")

def test_review_m2_two_jobs_on_one_table_never_regress_it(world, monkeypatch):
    fake, conn, src, dst = world
    assert _run(fake, src, dst).success
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = '2.75' WHERE region = 'ap' AND id = 2")
    a_read, a_go, b_done = threading.Event(), threading.Event(), threading.Event()
    orig_map = chunk_mod.map_rows_for_fingerprint

    def _a_pauses_after_reading(**kw):
        if threading.current_thread().name == "job-A" and not a_read.is_set():
            a_read.set()
            a_go.wait(60)
        return orig_map(**kw)

    monkeypatch.setattr(chunk_mod, "map_rows_for_fingerprint", _a_pauses_after_reading)
    results: dict = {}

    def _job(name):
        try:
            results[name] = _run_as(fake, src, dst, f"r14m2{name}" + uuid.uuid4().hex[:8])
        finally:
            if name == "B":
                b_done.set()

    a = threading.Thread(target=_job, args=("A",), name="job-A")
    a.start()
    assert a_read.wait(60), "job A never reached its write"
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = '3.00' WHERE region = 'ap' AND id = 2")
    b = threading.Thread(target=_job, args=("B",), name="job-B")
    b.start()
    b_finished_inside_a = b_done.wait(8)
    a_go.set()
    a.join(180)
    b.join(180)
    assert results["A"].success, results["A"].error
    assert results["B"].success, results["B"].error
    assert not b_finished_inside_a, "B wrote the table inside A's read-to-ladder window"
    # The table holds the source's current value, never A's stale one.
    assert _dest(conn, dst)[("ap", 2)][0] == Decimal("3.00")
    assert not dwl._HELD, "a destination lock outlived its job"


def test_a_keyed_job_on_a_table_another_job_is_writing_runs_unwitnessed(world, monkeypatch):
    """Shared holder in flight: the keyed job cannot take the lock exclusive, so
    it starts no witness and says why; it is not blocked."""
    fake, conn, src, dst = world
    assert _run(fake, src, dst).success
    import psycopg2

    from tests.test_r14_ladder_composite_key_scope_live import PG

    other = psycopg2.connect(host=PG["host"], port=PG["port"], dbname=PG["database"],
                             user=PG["username"], password=PG["password"])
    other.autocommit = True
    cur = other.cursor()
    cur.execute("SELECT hashtextextended(current_database() || '.' || %s || '.' || %s, 0)", ("public", dst))
    key = cur.fetchone()[0]
    cur.execute("SELECT pg_advisory_lock_shared(%s)", (key,))
    try:
        res = _run(fake, src, dst)
    finally:
        other.close()
    assert res.success, res.error
    assert (res.destination_summary or {}).get("keyed_write_witness", {}).get("reason") == (
        "no_exclusive_destination_lock"
    )


def test_with_the_flag_off_no_lock_is_taken_and_drift_is_disclosed(world, monkeypatch):
    """Default: no destination lock, so no witness attributes; a concurrent
    source update on a not-comparable scope stays advisory and is disclosed."""
    from tests.test_r14_concurrent_source_drift_live import _inject_concurrent_update

    monkeypatch.delenv("DATAFLOW_DEST_WRITE_LOCK", raising=False)
    fake, conn, src, dst = world
    conn.cursor().execute(f"INSERT INTO \"{dst}\" VALUES ('ap', 99, 9.99, 'loaded elsewhere')")
    assert _run(fake, src, dst).success
    _inject_concurrent_update(monkeypatch, conn, src, when="before_ladder")
    res = _run(fake, src, dst)
    assert res.success, res.error
    witness = (res.destination_summary or {}).get("keyed_write_witness") or {}
    assert witness.get("reason") == "destination_write_lock_off"
    assert witness.get("destination_write_lock") == {"enabled": False, "mode": ""}
    report = res.reconciliation or {}
    assert "concurrent_source_drift" not in report
    assert (report.get("value_discrepancy_unattributed") or {}).get("columns") == ["amount"]
    assert not dwl._HELD
