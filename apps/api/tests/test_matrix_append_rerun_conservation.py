"""Append reruns into an object or document destination.

f9e088bca counts keyed inserts+updates as rows_written. That ledger is the
keyed census. Mongo without ``_id``, S3, and SFTP stay on append-delta, so a
rerun whose artifact count does not grow is still a shortfall. An append that
keeps the prior rows closes when the measured count grows by the batch.
"""

from __future__ import annotations

from services.row_conservation import DEST_READBACK, account_population


def _ledger(*, before: int, after: int, read: int, mode: str = "incremental_append"):
    return account_population(
        rows_read=read,
        dest_count=after,
        dest_count_source=DEST_READBACK,
        dest_count_before=before,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=read,
        sync_mode=mode,
    )


def test_zero_growth_append_rerun_stays_a_shortfall():
    ledger = _ledger(before=100, after=100, read=15)
    assert ledger.balanced is False
    assert ledger.conservation_kind == "append_delta"
    assert "15" in ledger.note
    assert "growth (0" in ledger.note


def test_append_that_keeps_prior_rows_closes():
    ledger = _ledger(before=100, after=115, read=15)
    assert ledger.balanced is True
    assert "100" in ledger.note and "115" in ledger.note


def test_full_refresh_append_rerun_with_no_growth_stays_open():
    ledger = _ledger(before=40, after=40, read=40, mode="full_refresh_append")
    assert ledger.balanced is False
    assert "growth (0" in ledger.note
