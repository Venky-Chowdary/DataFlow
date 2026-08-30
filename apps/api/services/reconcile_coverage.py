"""Coverage / scope classification for Gate-8 reconciliation reports.

Two digests only prove fidelity when they cover the same population. Append or
upsert into a table that already held rows produces a target digest over rows
this job never wrote, so a difference there is structural, not corruption —
reporting it as a checksum failure marks a healthy write as a failed transfer.
This module owns that distinction (and the sibling "export we could not read
back" case) so ``reconciliation`` keeps one honesty rule per concern.
"""

from __future__ import annotations

from typing import Any, Final

# Target digest covers rows outside this job's write set.
WHOLE_TABLE_NOT_COMPARABLE = "whole_table_not_comparable"
# Target digest was re-read WHERE pk IN (written keys): per-cell proof of this
# batch, deliberately silent about rows the job never wrote.
WRITTEN_BATCH_KEYS = "written_batch_keys"
# CDC catch-up: dest COUNT vs current source-table COUNT. Last-batch writer
# checksum is not that population. Engine digest of source vs dest (same
# engine) can still close full_checksum; this scope is COUNT-only honesty.
CDC_SOURCE_IMAGE_COUNT = "cdc_source_image_count"
# A quiet incremental poll: the reader found nothing past the watermark, so no
# batch exists to compare and the proof is that the destination count did not
# move. Population evidence must not be turned on such a report — comparing a
# zero-row batch against a sink that legitimately holds earlier rows fails the
# normal outcome of every scheduled incremental sync.
NO_OP_DEST_UNCHANGED: Final[str] = "no_op_destination_unchanged"


def is_no_op_report(report: dict[str, Any]) -> bool:
    """True when the report declares a no-op poll (nothing read, nothing written)."""
    return str((report or {}).get("assurance_level") or "") == NO_OP_DEST_UNCHANGED


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
        # An unproven append delta already reported assurance "none"; the scope
        # stamp must not upgrade it back to "row_count".
        assurance_level=(
            "row_count" if out.get("passed") else out.get("assurance_level") or "none"
        ),
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


#: How the *source* digest was obtained. ``full_checksum`` claims two
#: independent digests agreed, so a source digest that is really the writer's
#: own account of what it wrote cannot earn it — that compares a write to
#: itself. Streaming passes hand no rows to reconcile, so this is the ordinary
#: case for exactly the large tables where the claim matters most.
SOURCE_DIGEST_WRITER_ACK: Final[str] = "writer_ack"
SOURCE_DIGEST_REMAPPED_ROWS: Final[str] = "remapped_source_rows"
SOURCE_DIGEST_WRITE_PASS: Final[str] = "write_pass_fingerprints"
SOURCE_DIGEST_ENGINE_POPULATION: Final[str] = "engine_population"
#: Second warehouse scan after the write, paged like the extract (scan/keyset,
#: never OFFSET on snapshot-scan sources). Dest is read back independently.
#: This is the Fivetran/HVR Compare class of proof — it may earn full_checksum.
SOURCE_DIGEST_SOURCE_REREAD: Final[str] = "independent_source_reread"

#: Provenances that are independent of the writer's own account of the write.
INDEPENDENT_SOURCE_DIGESTS: Final[frozenset[str]] = frozenset(
    {
        SOURCE_DIGEST_REMAPPED_ROWS,
        SOURCE_DIGEST_ENGINE_POPULATION,
        SOURCE_DIGEST_SOURCE_REREAD,
    }
)


def is_writer_ack_only(
    msg: str, target_checksum: str, *, source_provenance: str = ""
) -> bool:
    """True when the only digest we hold came from the writer, not a read-back.

    ``source_provenance`` is authoritative when the caller supplies it: it knows
    where the digest came from, whereas the message text can only be guessed at.
    """
    if source_provenance == SOURCE_DIGEST_WRITER_ACK:
        # A dest digest is independent even when the source side is the writer's
        # account. Treating that pair as writer-only hid a 1M-row Snowflake
        # SELECT * behind "read-back not available".
        return not bool(target_checksum)
    if source_provenance in INDEPENDENT_SOURCE_DIGESTS:
        return False
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
    target_rows_before: int | None = None,
) -> Any:
    """Cardinality verdict for an append into a non-empty destination.

    The only cardinality proof available for append is the *delta*:
    ``target_rows - target_rows_before == expected_rows``. The final count on
    its own proves nothing — a table that already held more rows than the batch
    satisfies ``target_rows >= expected_rows`` even if the writer appended
    nothing. So the delta decides the verdict:

    * delta known and exact — pass at ``row_count`` assurance (per-cell fidelity
      still not claimed);
    * delta known and wrong — fail; rows are missing or duplicated;
    * delta unknown (no pre-write count) — do **not** pass. Report
      ``assurance_level="none"`` and say the append is unverified rather than
      printing "row count verified" over an unproven write.
    """
    from services.reconciliation import ReconciliationReport

    common = {
        "source_rows": source_rows,
        "target_rows": target_rows,
        "source_checksum": source_checksum,
        "target_checksum": target_checksum,
        "rejected_rows": rejected_rows,
        "coerced_null_rows": coerced_null_rows,
        "rows_skipped": rows_skipped,
        "sample_compare": sample_compare,
        "checksum_match": False,
        "population_proof": False,
        "checksum_scope": WHOLE_TABLE_NOT_COMPARABLE,
        "target_rows_before": target_rows_before,
    }

    if target_rows_before is None:
        return ReconciliationReport(
            passed=False,
            message=(
                "Append delta unverified: destination held an unknown number of "
                f"rows before this write, so the final count ({target_rows}) "
                f"cannot prove the {expected_rows} expected row(s) landed. "
                "Whole-table digests are not comparable for append into "
                "pre-existing rows. Use overwrite, or upsert with a primary "
                f"key, for full_checksum proof.{sample_note}"
            ),
            assurance_level="none",
            **common,
        )

    delta = target_rows - int(target_rows_before)
    if delta != expected_rows:
        return ReconciliationReport(
            passed=False,
            message=(
                f"Append delta mismatch: destination held {target_rows_before} "
                f"row(s) before the write and {target_rows} after — {delta} "
                f"appended, {expected_rows} expected.{sample_note}"
            ),
            assurance_level="none",
            **common,
        )

    return ReconciliationReport(
        passed=True,
        message=(
            f"Append delta verified ({expected_rows} row(s) appended: "
            f"{target_rows_before} → {target_rows}). Whole-table digests are "
            "not comparable for append into pre-existing rows — per-cell "
            "fidelity is NOT proven. Use overwrite, or upsert with a primary "
            f"key, for full_checksum proof.{sample_note}"
        ),
        assurance_level="row_count",
        **common,
    )
