"""D110 — a failed job whose batch is removed must not keep the watermark.

ACC-05 advances the route watermark per committed batch because the rows are
durable at that moment. When Gate-8 verification fails and the batch is
removed (empty-destination undo or routed-table restore), that advance is no
longer true: the next run used to skip every source row and report
"completed" while the destination was empty or stale.
"""

from __future__ import annotations

import sqlite3
import uuid

import pytest

from src.transfer.models import EndpointConfig
from services.batch_undo import undo_failed_batch_if_dest_was_empty
from services.sync_cursor import clear_watermark, get_watermark, set_watermark


def _restore_route_watermark_after_undo(summary):
    from services.batch_undo import restore_route_watermark_after_undo

    return restore_route_watermark_after_undo(summary)


def _key() -> str:
    return f"d110-{uuid.uuid4().hex[:12]}"


def _sqlite_dest(tmp_path, rows: int = 2) -> EndpointConfig:
    db = tmp_path / "dest.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"r{i}") for i in range(rows)])
        conn.commit()
    return EndpointConfig(kind="database", format="sqlite", database=str(db), table="t")


def _summary(cursor_key: str, prior, **extra) -> dict:
    summary = {
        "target_rows_before": 0,
        "table": "t",
        "job_id": f"job-{uuid.uuid4().hex[:8]}",
        "watermark_restore_point": {"cursor_key": cursor_key, "watermark": prior},
    }
    summary.update(extra)
    return summary


def test_removed_batch_rolls_watermark_back_to_prerun(tmp_path):
    """Failed job: batch removed → watermark restored to the pre-run value."""
    dest = _sqlite_dest(tmp_path)
    ck = _key()
    set_watermark(ck, "10")  # pre-run
    set_watermark(ck, "40")  # the failed run's advance
    summary = _summary(ck, "10")
    note = undo_failed_batch_if_dest_was_empty(
        destination=dest, dest_summary=summary, sync_mode="incremental_deduped"
    )
    assert summary.get("partial_batch_removed") is True
    assert get_watermark(ck) == "10", note
    assert "watermark" in note.lower()


def test_removed_batch_first_run_clears_watermark(tmp_path):
    """No pre-run watermark (first run): the cursor record is deleted so the
    next run is a true first run, not a skip-everything run."""
    dest = _sqlite_dest(tmp_path)
    ck = _key()
    set_watermark(ck, "40")  # only the failed run's advance exists
    summary = _summary(ck, None)
    undo_failed_batch_if_dest_was_empty(
        destination=dest, dest_summary=summary, sync_mode="incremental_deduped"
    )
    assert summary.get("partial_batch_removed") is True
    assert get_watermark(ck) is None


def test_occupied_dest_retains_batch_and_watermark(tmp_path):
    """Occupied destination: the failed batch is left in place, so the
    watermark advance stands — the rows it names are still durable."""
    dest = _sqlite_dest(tmp_path)
    ck = _key()
    set_watermark(ck, "40")
    summary = _summary(ck, "10", target_rows_before=5)
    undo_failed_batch_if_dest_was_empty(
        destination=dest, dest_summary=summary, sync_mode="incremental_deduped"
    )
    assert summary.get("partial_batch_undo") == "retained"
    assert get_watermark(ck) == "40"


def test_missing_restore_stamp_leaves_watermark_untouched(tmp_path):
    """A summary without the restore stamp (pre-fix jobs) is left alone."""
    dest = _sqlite_dest(tmp_path)
    ck = _key()
    set_watermark(ck, "40")
    summary = {
        "target_rows_before": 0,
        "table": "t",
        "job_id": f"job-{uuid.uuid4().hex[:8]}",
    }
    undo_failed_batch_if_dest_was_empty(
        destination=dest, dest_summary=summary, sync_mode="incremental_deduped"
    )
    assert summary.get("partial_batch_removed") is True
    assert get_watermark(ck) == "40"


def test_restored_route_undo_rolls_watermark_back():
    """The routed-table restore path (partial_batch_undo == 'restored') also
    rolls the watermark back — same rows-undone ⇒ same watermark-undone."""
    ck = _key()
    set_watermark(ck, "40")
    summary = {
        "partial_batch_undo": "restored",
        "watermark_restore_point": {"cursor_key": ck, "watermark": "10"},
    }
    note = _restore_route_watermark_after_undo(summary)
    assert get_watermark(ck) == "10"
    assert "watermark" in note.lower()


def test_retained_route_undo_keeps_watermark():
    ck = _key()
    set_watermark(ck, "40")
    summary = {
        "partial_batch_undo": "retained",
        "watermark_restore_point": {"cursor_key": ck, "watermark": "10"},
    }
    assert _restore_route_watermark_after_undo(summary) == ""
    assert get_watermark(ck) == "40"
    clear_watermark(ck)


@pytest.fixture(autouse=True)
def _cleanup_keys():
    yield
