"""Coverage / scope classification for Gate-8 reconciliation reports.

Two digests only prove fidelity when they cover the same population. Append or
upsert into a table that already held rows produces a target digest over rows
this job never wrote, so a difference there is structural, not corruption —
reporting it as a checksum failure marks a healthy write as a failed transfer.
This module owns that distinction (and the sibling "export we could not read
back" case) so ``reconciliation`` keeps one honesty rule per concern.
"""

from __future__ import annotations

from typing import Any

# Target digest covers rows outside this job's write set.
WHOLE_TABLE_NOT_COMPARABLE = "whole_table_not_comparable"


def row_count_scope_stamp(out: dict[str, Any]) -> dict[str, Any] | None:
    """Phase stamp for a report whose digests cover different populations.

    Returns ``None`` when the report is comparable and the caller's normal
    divergence rules apply.
    """
    if str(out.get("checksum_scope") or "") != WHOLE_TABLE_NOT_COMPARABLE:
        return None
    stamped = dict(out)
    stamped.update(
        phase="post_write_row_count",
        post_write_pending=False,
        preview=False,
        coverage="row_count",
        assurance_level="row_count",
        checksum_match=False,
        population_proof=False,
        migration_proven=False,
    )
    return stamped


def is_unproven_export(out: dict[str, Any], msg: str) -> bool:
    """True when the destination could not be read back for an independent digest."""
    return bool(
        out.get("unproven") is True
        or out.get("skipped_readback") is True
        or "file/object export" in msg
        or "file export" in msg
        or "object export" in msg
        or ("skipped" in msg and "reconciliation skipped" in msg)
        or ("cell fidelity" in msg and "unproven" in msg)
    )


def is_writer_ack_only(msg: str, target_checksum: str) -> bool:
    """True when the only digest we hold came from the writer, not a read-back."""
    return bool(
        not target_checksum
        or "verified by writer" in msg
        or "read-back verifier not available" in msg
        or ("read-back" in msg and "unavailable" in msg)
    )


def is_sample_authority(msg: str, target_checksum: str, *, writer_only: bool) -> bool:
    """True when a keyed sample is the strongest evidence available."""
    return bool(
        writer_only
        or not target_checksum
        or "sample-verified" in msg
        or "sample verified" in msg
        or "key-aligned" in msg
        or "sample-only assurance" in msg
    )


def extra_rows_note(target_rows: int, expected_rows: int) -> str:
    """Explain surplus destination rows without calling the written rows wrong."""
    written_hint = int(expected_rows) if expected_rows else 0
    return (
        f" Destination has {target_rows - expected_rows} extra row(s) "
        "(append/upsert into a non-empty table); whole-table digests are "
        "not comparable. Use overwrite/truncate for a clean load, or "
        "upsert with a primary key — this is not proof the "
        f"{written_hint} written row(s) are wrong."
    )


def append_row_count_report(
    *,
    source_rows: int,
    target_rows: int,
    expected_rows: int,
    source_checksum: str,
    target_checksum: str,
    sample_note: str,
    rejected_rows: int,
    coerced_null_rows: int,
    rows_skipped: int,
    sample_compare: dict[str, Any] | None,
) -> Any:
    """Row-count-level pass for an append into a non-empty destination.

    Cardinality is verified; per-cell fidelity is explicitly *not* claimed, and
    the operator is told which sync mode would produce full checksum proof.
    """
    from services.reconciliation import ReconciliationReport

    return ReconciliationReport(
        passed=True,
        source_rows=source_rows,
        target_rows=target_rows,
        source_checksum=source_checksum,
        target_checksum=target_checksum,
        message=(
            f"Row count verified ({expected_rows} row(s) appended into a "
            f"non-empty table now holding {target_rows}). Whole-table digests "
            "are not comparable for append into pre-existing rows — per-cell "
            "fidelity is NOT proven. Use overwrite, or upsert with a primary "
            f"key, for full_checksum proof.{sample_note}"
        ),
        rejected_rows=rejected_rows,
        coerced_null_rows=coerced_null_rows,
        rows_skipped=rows_skipped,
        sample_compare=sample_compare,
        checksum_match=False,
        population_proof=False,
        assurance_level="row_count",
        checksum_scope=WHOLE_TABLE_NOT_COMPARABLE,
    )
