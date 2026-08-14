"""Independent population conservation: dest COUNT(*), not writer ack.

The identity every cardinality-bearing load must close:

    reader_count == dest_population + hold_outs + skipped

``dest_population`` is an independent destination COUNT(*) — Gate-8's
``target_rows`` when that figure came from dest-engine read-back, not from
the writer's ``records_processed``. Closing the identity with the writer's
own acknowledgement is how AWS DMS reports Full Load success and later
``MISSING_TARGET``: the writer counted rows the dest engine does not hold.
Airbyte ``emitted`` / Fivetran MAR counters have the same circular shape.

Hold-outs are rows that did **not** land:

    hold_outs = max(rejected_rows - coerced_null_rows, 0)

Coerced-null rows *did* land (a cell became NULL). Counting them as both
quarantine and written invents a surplus. Gate-8 already uses this drop
arithmetic; this module is the single named identity so the certificate
cannot drift from it.

How dest_population is chosen:

* **overwrite / replace / empty dest** — dest COUNT(*) is the written
  population. Pre-existing rows were dropped or there were none.
* **append** — only the delta proves anything:
  ``COUNT(*)_after - COUNT(*)_before``. A table that already held 30 rows
  satisfies ``dest >= expected`` even if the writer appended nothing.
* **upsert / CDC into a non-empty dest** — COUNT(*) is not conservation
  (updates do not change cardinality). This module refuses to claim a
  balanced ledger from cardinality there; keyed apply is a later property.

Writer ack is a diagnostic third number. It never closes the identity.
A mismatch against dest COUNT is the DMS hole, reported as a note, not as
proof that the rows landed.

An empty pass (reader 0, hold-outs 0, skipped 0, writer ack 0) is a
*measured* zero — the incremental steady state — not an unmeasured dest.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from services.dest_precount import PRECOUNT_KEY
from services.reconcile_coverage import is_unproven_export
from services.sync_cursor import is_append_sync, is_overwrite_sync

DEST_READBACK = "gate8_dest_readback"
DEST_UNMEASURED = "unmeasured"
DEST_EMPTY_PASS = "empty_pass"

KIND_OVERWRITE = "overwrite"
KIND_APPEND_DELTA = "append_delta"
KIND_KEYED = "keyed"
KIND_EMPTY_PASS = "empty_pass"
KIND_UNMEASURED = "unmeasured"


def hold_outs(rejected_rows: int, coerced_null_rows: int) -> int:
    """Rows the dest engine does not hold.

    Coerced-null cells landed; they are not hold-outs. Gate-8's
    ``dropped_rows = max(rejected - coerced_null, 0)`` is the same number.
    """
    return max(int(rejected_rows or 0) - int(coerced_null_rows or 0), 0)


def _as_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_present_int(*values: Any, default: int = 0) -> int:
    """First value that was supplied, including measured zero.

    ``rejected_rows or dest.rejected`` treats a measured 0 as missing and
    picks up a stale dest figure — that inflates hold-outs and invents a
    shortfall.
    """
    for value in values:
        if value is None:
            continue
        parsed = _as_optional_int(value)
        if parsed is not None:
            return parsed
    return default


def dest_count_from_recon(recon: Mapping[str, Any] | None) -> tuple[int | None, str]:
    """Independent dest population, or unmeasured.

    Checksum *claim* and dest *cardinality* are different axes. Streaming
    passes often stamp ``source_checksum_provenance=writer_ack``, so Gate-8
    refuses ``full_checksum`` — correctly, because the source digest is the
    writer's own account. The dest engine was still re-read: ``target_rows``
    and ``target_checksum`` are dest COUNT(*) / dest digest. Using writer-ack
    *phase* to discard that count would refuse every SQLite/PG overwrite
    this host can prove.

    ``target_rows`` is dest COUNT(*) when dest produced a digest or an
    explicit dest row-count phase. File/object exports and "verified by
    writer" with no dest digest stuff the writer's acknowledgement into the
    same field — using that would circularly balance a short write.
    """
    report = dict(recon or {})
    msg = str(report.get("message") or "").lower()
    if report.get("skipped_readback") is True:
        return None, DEST_UNMEASURED
    if is_unproven_export(report, msg):
        return None, DEST_UNMEASURED
    raw = report.get("target_rows")
    if not isinstance(raw, int) or raw < 0:
        return None, DEST_UNMEASURED
    dest_digest = str(report.get("target_checksum") or "").strip()
    if dest_digest:
        return raw, DEST_READBACK
    phase = str(report.get("phase") or "").lower()
    coverage = str(report.get("coverage") or "").lower()
    if coverage == "row_count" or "row_count" in phase or "row count" in msg:
        return raw, DEST_READBACK
    # No dest digest and no dest row-count phase: target_rows is writer ack.
    return None, DEST_UNMEASURED


def conservation_kind(
    sync_mode: str | None,
    *,
    dest_count_before: int | None,
) -> str:
    """Which cardinality identity this run is allowed to close.

    Upsert/CDC into an *empty* destination is insert-only, so overwrite
    cardinality applies. Into a non-empty destination, COUNT(*) cannot
    prove which keys were applied.
    """
    if is_overwrite_sync(sync_mode):
        return KIND_OVERWRITE
    if is_append_sync(sync_mode):
        return KIND_APPEND_DELTA
    if dest_count_before == 0:
        return KIND_OVERWRITE
    return KIND_KEYED


@dataclass(frozen=True)
class ConservationLedger:
    """One closed (or honestly open) population identity."""

    rows_read: int | None
    rows_written: int | None
    rows_quarantined: int
    rows_skipped: int
    rows_coerced_null: int
    writer_ack: int | None
    dest_count: int | None
    dest_count_before: int | None
    unaccounted: int | None
    balanced: bool
    rows_read_source: str
    rows_written_source: str
    conservation_kind: str
    note: str
    writer_ack_delta: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_quarantined": self.rows_quarantined,
            "rows_skipped": self.rows_skipped,
            "rows_coerced_null": self.rows_coerced_null,
            "writer_ack": self.writer_ack,
            "dest_count": self.dest_count,
            "dest_count_before": self.dest_count_before,
            "unaccounted": self.unaccounted,
            "balanced": self.balanced,
            "rows_read_source": self.rows_read_source,
            "rows_written_source": self.rows_written_source,
            "conservation_kind": self.conservation_kind,
            "note": self.note,
            "writer_ack_delta": self.writer_ack_delta,
        }


def _empty_pass_ledger(
    *,
    quarantined: int,
    skipped: int,
    coerced: int,
    writer_ack: int | None,
    dest_count: int | None,
    dest_count_before: int | None,
) -> ConservationLedger:
    return ConservationLedger(
        rows_read=0,
        rows_written=0,
        rows_quarantined=quarantined,
        rows_skipped=skipped,
        rows_coerced_null=coerced,
        writer_ack=writer_ack,
        dest_count=dest_count,
        dest_count_before=dest_count_before,
        unaccounted=0,
        balanced=True,
        rows_read_source="gate8_source_count",
        rows_written_source=DEST_EMPTY_PASS,
        conservation_kind=KIND_EMPTY_PASS,
        note=(
            "Measured empty pass: nothing was read and nothing was written. "
            "This is the incremental steady state, not an unmeasured run."
        ),
        writer_ack_delta=(0 - writer_ack) if writer_ack is not None else None,
    )


def account_population(
    *,
    rows_read: int | None,
    dest_count: int | None,
    dest_count_source: str,
    dest_count_before: int | None,
    rejected_rows: int,
    coerced_null_rows: int,
    rows_skipped: int,
    writer_ack: int | None,
    sync_mode: str | None,
) -> ConservationLedger:
    """Close ``reader == dest_population + hold_outs + skipped`` or say why not."""
    quarantined = hold_outs(rejected_rows, coerced_null_rows)
    skipped = int(rows_skipped or 0)
    coerced = int(coerced_null_rows or 0)
    kind = conservation_kind(sync_mode, dest_count_before=dest_count_before)
    ack = writer_ack if writer_ack is not None else None

    if rows_read is None:
        return ConservationLedger(
            rows_read=None,
            rows_written=None,
            rows_quarantined=quarantined,
            rows_skipped=skipped,
            rows_coerced_null=coerced,
            writer_ack=ack,
            dest_count=dest_count,
            dest_count_before=dest_count_before,
            unaccounted=None,
            balanced=False,
            rows_read_source=DEST_UNMEASURED,
            rows_written_source=dest_count_source,
            conservation_kind=KIND_UNMEASURED,
            note=(
                "Source row count was not measured for this run, so no "
                "conservation check is possible. Destination COUNT(*) cannot "
                "be compared against rows that were never counted on the read."
            ),
            writer_ack_delta=None,
        )

    read = int(rows_read)
    if (
        read == 0
        and quarantined == 0
        and skipped == 0
        and int(ack or 0) == 0
    ):
        return _empty_pass_ledger(
            quarantined=quarantined,
            skipped=skipped,
            coerced=coerced,
            writer_ack=ack,
            dest_count=dest_count,
            dest_count_before=dest_count_before,
        )

    if kind == KIND_KEYED:
        return ConservationLedger(
            rows_read=read,
            rows_written=None,
            rows_quarantined=quarantined,
            rows_skipped=skipped,
            rows_coerced_null=coerced,
            writer_ack=ack,
            dest_count=dest_count,
            dest_count_before=dest_count_before,
            unaccounted=None,
            balanced=False,
            rows_read_source="gate8_source_count",
            rows_written_source=DEST_UNMEASURED,
            conservation_kind=KIND_KEYED,
            note=(
                "Upsert/CDC into a non-empty destination has no COUNT(*) "
                "identity — updates do not change cardinality. Keyed apply "
                "conservation is not claimed for this run."
            ),
            writer_ack_delta=None,
        )

    written: int | None
    written_source = dest_count_source
    if dest_count is None or dest_count_source != DEST_READBACK:
        written = None
        written_source = DEST_UNMEASURED
        return ConservationLedger(
            rows_read=read,
            rows_written=None,
            rows_quarantined=quarantined,
            rows_skipped=skipped,
            rows_coerced_null=coerced,
            writer_ack=ack,
            dest_count=dest_count,
            dest_count_before=dest_count_before,
            unaccounted=None,
            balanced=False,
            rows_read_source="gate8_source_count",
            rows_written_source=written_source,
            conservation_kind=kind,
            note=(
                "Destination COUNT(*) was not independently measured, so no "
                "conservation check is possible. Writer acknowledgements "
                "cannot prove rows landed — that is the DMS MISSING_TARGET "
                "after Full Load success hole."
            ),
            writer_ack_delta=None,
        )

    if kind == KIND_APPEND_DELTA:
        if dest_count_before is None:
            return ConservationLedger(
                rows_read=read,
                rows_written=None,
                rows_quarantined=quarantined,
                rows_skipped=skipped,
                rows_coerced_null=coerced,
                writer_ack=ack,
                dest_count=dest_count,
                dest_count_before=None,
                unaccounted=None,
                balanced=False,
                rows_read_source="gate8_source_count",
                rows_written_source=DEST_UNMEASURED,
                conservation_kind=kind,
                note=(
                    "Append delta unverified: destination held an unknown "
                    f"number of rows before this write, so the final COUNT(*) "
                    f"({dest_count}) cannot prove the batch landed."
                ),
                writer_ack_delta=None,
            )
        written = dest_count - int(dest_count_before)
        written_source = DEST_READBACK
    else:
        written = dest_count
        written_source = DEST_READBACK

    unaccounted = read - (written + quarantined + skipped)
    ack_delta = (written - ack) if ack is not None else None

    if unaccounted > 0:
        note = (
            f"{unaccounted} source row(s) are neither on the destination "
            "(independent COUNT(*)), quarantined, nor skipped. Treat as "
            "potential silent loss — the writer acknowledgement is not "
            "evidence they landed."
        )
    elif unaccounted < 0:
        note = (
            f"{abs(unaccounted)} more row(s) are on the destination, "
            "quarantined, or skipped than were read. Duplicate writes, "
            "pre-existing rows on overwrite, or double-counted rejects."
        )
    else:
        note = (
            "Every source row is on the destination (independent COUNT(*)), "
            "quarantined, or skipped."
        )
    if ack_delta:
        sign = "more" if ack_delta > 0 else "fewer"
        note += (
            f" Writer acknowledged {ack:,}; dest COUNT(*) accounts for "
            f"{written:,} ({abs(ack_delta):,} {sign} than the writer claimed)."
        )
    return ConservationLedger(
        rows_read=read,
        rows_written=written,
        rows_quarantined=quarantined,
        rows_skipped=skipped,
        rows_coerced_null=coerced,
        writer_ack=ack,
        dest_count=dest_count,
        dest_count_before=dest_count_before,
        unaccounted=unaccounted,
        balanced=unaccounted == 0,
        rows_read_source="gate8_source_count",
        rows_written_source=written_source,
        conservation_kind=kind,
        note=note,
        writer_ack_delta=ack_delta,
    )


def account_job(job: Mapping[str, Any]) -> ConservationLedger:
    """Conservation ledger for one job document.

    ``rows_written`` is dest COUNT(*), never ``records_processed``.
    """
    recon = dict(job.get("reconciliation") or {})
    dest = dict(job.get("destination_summary") or {})
    dest_count, dest_source = dest_count_from_recon(recon)
    before = _as_optional_int(dest.get(PRECOUNT_KEY))
    if before is None:
        before = _as_optional_int(recon.get(PRECOUNT_KEY))
    ack_raw = job.get("records_processed")
    if ack_raw is None:
        ack_raw = dest.get("rows")
    return account_population(
        rows_read=_as_optional_int(recon.get("source_rows")),
        dest_count=dest_count,
        dest_count_source=dest_source,
        dest_count_before=before,
        rejected_rows=_first_present_int(
            recon.get("rejected_rows"),
            job.get("rejected_rows"),
            dest.get("rejected"),
        ),
        coerced_null_rows=_first_present_int(
            recon.get("coerced_null_rows"),
            job.get("coerced_null_rows"),
        ),
        rows_skipped=_first_present_int(
            recon.get("rows_skipped"),
            dest.get("rows_skipped"),
        ),
        writer_ack=_as_optional_int(ack_raw),
        sync_mode=str(job.get("sync_mode") or dest.get("sync_mode") or ""),
    )
