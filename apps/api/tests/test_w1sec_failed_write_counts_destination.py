"""REL-2: a failed write into a table the run created reports the rows the table holds.

A SQLite destination that hit a full disk said "72312 row(s) were already
written" (the writer's tallies) while the table held 74,360, and the undo
that should have removed the run-created table could not run on a full disk.
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest


@pytest.fixture
def sqlite_dest(tmp_path_factory, monkeypatch):
    db = tmp_path_factory.mktemp("w1sec_rel2") / "dest.db"
    conn = sqlite3.connect(db)
    conn.execute("create table w1sec_t (id integer)")
    conn.executemany("insert into w1sec_t values (?)", [(i,) for i in range(1234)])
    conn.commit()
    conn.close()
    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(db.parent))
    from src.transfer.models import EndpointConfig

    return EndpointConfig(kind="database", format="sqlite", database=str(db), table="w1sec_t")


def test_a_retained_run_created_table_is_counted(sqlite_dest, monkeypatch):
    from services import batch_undo

    # The undo cannot remove the rows (a full disk refuses the DELETE / DROP).
    monkeypatch.setattr(batch_undo, "_clear", lambda destination, summary: "The undo could not run.")
    summary = {"table": "w1sec_t", "dest_table_existed_before": False, "sync_mode": "full_refresh_overwrite"}
    note = batch_undo.undo_failed_batch_if_dest_was_empty(
        destination=sqlite_dest, dest_summary=summary, sync_mode="full_refresh_overwrite", job_id="w1sec")
    assert summary.get(batch_undo.ROWS_COMMITTED_KEY) == 1234, summary
    assert "1,234" in note, note


def test_the_job_message_carries_the_measured_count(sqlite_dest, monkeypatch):
    from services import batch_undo
    from src.transfer.job_failure import account_blocked_write

    monkeypatch.setattr(batch_undo, "_clear", lambda destination, summary: "The undo could not run.")
    summary = {"table": "w1sec_t", "dest_table_existed_before": False}
    note = batch_undo.undo_failed_batch_if_dest_was_empty(
        destination=sqlite_dest, dest_summary=summary, sync_mode="full_refresh_overwrite")
    blocked = RuntimeError("partial write (the failing batch reported 3072 row(s) committed): disk I/O error")
    blocked.rows_written = 72312
    blocked.failed_batch_undo_note = note
    blocked.rows_committed_measured = summary[batch_undo.ROWS_COMMITTED_KEY]
    mongo = SimpleNamespace(get_job=lambda job_id: {"records_processed": 72312})
    kept, message = account_blocked_write(mongo, "w1sec", blocked)
    assert kept == 1234
    assert "72312 row(s) were already written" not in message and "1,234" in message, message
