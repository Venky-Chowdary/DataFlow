"""Independent dest COUNT(*) closes conservation — writer ack never does.

AWS DMS Full Load can succeed while validation later reports MISSING_TARGET:
the writer counted rows the dest engine does not hold. This module is the
named identity so the certificate cannot circularly balance a short write
against itself.
"""

from __future__ import annotations

from services.row_conservation import (
    DEST_READBACK,
    DEST_UNMEASURED,
    KIND_APPEND_DELTA,
    KIND_EMPTY_PASS,
    KIND_KEYED,
    KIND_OVERWRITE,
    account_job,
    account_population,
    dest_count_from_recon,
    hold_outs,
)


def test_hold_outs_exclude_coerced_null_rows_that_landed():
    assert hold_outs(rejected_rows=5, coerced_null_rows=2) == 3
    assert hold_outs(rejected_rows=2, coerced_null_rows=2) == 0
    assert hold_outs(rejected_rows=0, coerced_null_rows=3) == 0


def test_writer_ack_phase_is_not_a_dest_count():
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "phase": "post_write_writer_ack",
            "coverage": "writer_ack",
            "assurance_level": "writer_ack",
            "message": "verified by writer checksum",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_skipped_readback_stuffs_writer_ack_and_is_refused():
    count, source = dest_count_from_recon(
        {
            "target_rows": 10_000,
            "skipped_readback": True,
            "unproven": True,
            "message": "File/object export wrote successfully",
        }
    )
    assert count is None
    assert source == DEST_UNMEASURED


def test_failed_gate8_still_exposes_independent_dest_count():
    """MISSING_TARGET class: dest COUNT is 9997 even though the write 'succeeded'."""
    count, source = dest_count_from_recon(
        {
            "passed": False,
            "phase": "post_write_failed",
            "target_rows": 9997,
            "source_rows": 10_000,
            "message": "Row count mismatch",
        }
    )
    assert count == 9997
    assert source == DEST_READBACK


def test_overwrite_balances_on_dest_count_not_writer_ack():
    ledger = account_population(
        rows_read=10_000,
        dest_count=9997,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10_000,
        sync_mode="full_refresh_overwrite",
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.rows_written == 9997
    assert ledger.writer_ack == 10_000
    assert ledger.unaccounted == 3
    assert ledger.balanced is False
    assert ledger.writer_ack_delta == -3


def test_coerced_null_rows_are_on_the_destination():
    ledger = account_population(
        rows_read=10,
        dest_count=10,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=2,
        coerced_null_rows=2,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="overwrite",
    )
    assert ledger.rows_quarantined == 0
    assert ledger.unaccounted == 0
    assert ledger.balanced is True


def test_true_quarantine_hold_outs_close_with_dest_count():
    ledger = account_population(
        rows_read=10,
        dest_count=8,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=2,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=8,
        sync_mode="overwrite",
    )
    assert ledger.rows_quarantined == 2
    assert ledger.balanced is True


def test_append_uses_dest_delta_not_whole_table_count():
    ledger = account_population(
        rows_read=10,
        dest_count=40,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="full_refresh_append",
    )
    assert ledger.conservation_kind == KIND_APPEND_DELTA
    assert ledger.rows_written == 10
    assert ledger.balanced is True


def test_append_without_precount_is_unmeasured():
    ledger = account_population(
        rows_read=10,
        dest_count=40,
        dest_count_source=DEST_READBACK,
        dest_count_before=None,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="append",
    )
    assert ledger.balanced is False
    assert ledger.rows_written is None
    assert "Append delta unverified" in ledger.note


def test_upsert_into_nonempty_dest_has_no_count_identity():
    ledger = account_population(
        rows_read=10,
        dest_count=35,
        dest_count_source=DEST_READBACK,
        dest_count_before=30,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="upsert",
    )
    assert ledger.conservation_kind == KIND_KEYED
    assert ledger.balanced is False
    assert ledger.rows_written is None


def test_upsert_into_empty_dest_is_insert_cardinality():
    ledger = account_population(
        rows_read=10,
        dest_count=10,
        dest_count_source=DEST_READBACK,
        dest_count_before=0,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=10,
        sync_mode="upsert",
    )
    assert ledger.conservation_kind == KIND_OVERWRITE
    assert ledger.balanced is True


def test_incremental_empty_pass_is_measured_zero():
    ledger = account_population(
        rows_read=0,
        dest_count=None,
        dest_count_source=DEST_UNMEASURED,
        dest_count_before=None,
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=0,
        sync_mode="incremental_append",
    )
    assert ledger.conservation_kind == KIND_EMPTY_PASS
    assert ledger.balanced is True
    assert ledger.unaccounted == 0


def test_account_job_ignores_records_processed_when_dest_count_exists():
    job = {
        "records_processed": 10_000,
        "sync_mode": "overwrite",
        "reconciliation": {
            "phase": "post_write_verified",
            "source_rows": 10_000,
            "target_rows": 9997,
            "rejected_rows": 0,
            "rows_skipped": 0,
            "target_checksum": "deadbeef",
            "message": "Verified",
        },
        "destination_summary": {"rows": 10_000, "rejected": 50},
    }
    ledger = account_job(job)
    assert ledger.rows_written == 9997
    assert ledger.writer_ack == 10_000
    assert ledger.rows_quarantined == 0
    assert ledger.balanced is False
