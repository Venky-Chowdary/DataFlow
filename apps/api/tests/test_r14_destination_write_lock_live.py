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

import threading
import uuid
from decimal import Decimal

import services.destination_write_lock as dwl
import src.transfer.stream  # noqa: F401 — import order: stream before stream_db_chunk
import src.transfer.stream_db_chunk as chunk_mod
from tests.test_r14_ladder_composite_key_scope_live import _dest, _run, world  # noqa: F401
from tests.test_r14_witness_review_live import _run_as


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
