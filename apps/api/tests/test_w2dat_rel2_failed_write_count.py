"""REL-2: a failed write reports the rows the destination actually holds.

Before the fix a disk-full SQLite run said "72312 row(s) were already written"
while the table held 74,360: the number was a writer tally across batches,
and the destination was re-counted only for a full-refresh append into an
occupied table.
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace


def _run(db, *, existed_before: bool | None, before: int | None):
    from src.transfer.models import EndpointConfig

    summary: dict = {}
    if existed_before is not None:
        summary["dest_table_existed_before"] = existed_before
    if before is not None:
        summary["target_rows_before"] = before
    dest = EndpointConfig(kind="database", format="sqlite", database=str(db), table="t")
    return SimpleNamespace(
        cfg=SimpleNamespace(destination=dest, dest_table="t"),
        tally=SimpleNamespace(dest_summary=summary),
    )


def test_a_failed_write_into_a_new_table_reports_the_counted_rows(tmp_path):
    from src.transfer.stream_write_ledger import _recount_after_failure

    db = tmp_path / "r2.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(74360)])
    exc = RuntimeError("disk I/O error")
    assert _recount_after_failure(_run(db, existed_before=False, before=None), exc) == 74360
    assert "74,360 row(s) this run wrote" in exc.failed_batch_undo_note

    occupied = RuntimeError("disk I/O error")
    assert _recount_after_failure(_run(db, existed_before=True, before=70000), occupied) == 4360
    # A restored destination holds none of this run's rows: nothing to count.
    restored = RuntimeError("x")
    restored.destination_restored = True
    assert _recount_after_failure(_run(db, existed_before=False, before=None), restored) is None


def test_account_blocked_write_uses_the_counted_rows():
    from src.transfer.job_failure import account_blocked_write

    blocked = RuntimeError("partial write (the failing batch reported 3072 row(s) committed): disk I/O error")
    blocked.rows_written = 3072
    blocked.rows_committed_measured = 74360
    blocked.failed_batch_undo_note = "Counted after the failure: 74,360 row(s) this run wrote are in t; they were not removed."

    class _Mongo:
        def get_job(self, job_id):
            return {"records_processed": 72312}

    kept, message = account_blocked_write(_Mongo(), "job", blocked)
    assert kept == 74360
    assert "74,360 row(s)" in message and "72312" not in message
