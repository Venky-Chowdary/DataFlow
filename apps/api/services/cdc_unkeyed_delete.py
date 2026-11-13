"""Park CDC deletes that have no key, with an audited record (backlog C2).

A delete on an unkeyed table (no primary key, or a declared key the delete's
row image does not carry, or carries as NULL) cannot be applied: there is no
key to address the destination row with. Audit 12 made the readers raise
instead of dropping it, which is honest but stops the stream for good. The
position cannot move past the change, so every later poll hits it again, and
no other change on any table of the route is applied after it.

Neither stopping nor dropping is acceptable, so such a delete is *parked*:

* its row image, table and log position go to the dead-letter queue as one
  ``cdc_unkeyed_delete_parked`` event per batch (durable before the position
  advances; a failed write raises and the position stays where it was);
* an audit event (``cdc.delete.unkeyed_parked``) records the same facts for the
  operator;
* the run evidence carries the count and a note naming the table, so the job
  page says the destination still holds rows the source deleted.

The destination row is left in place. Deleting it by matching the whole row
image would remove every identical row, and a table without a key can hold
several. The operator applies the parked deletes, or gives the table a key and
re-snapshots it.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

#: DLQ action of one batch's parked deletes.
DLQ_ACTION = "cdc_unkeyed_delete_parked"
#: Audit action of one batch's parked deletes.
AUDIT_ACTION = "cdc.delete.unkeyed_parked"
#: Summary keys (per batch and folded per run).
SUMMARY_COUNT_KEY = "cdc_unkeyed_deletes_parked"
SUMMARY_EVIDENCE_KEY = "cdc_unkeyed_delete_evidence"
SUMMARY_NOTE_KEY = "cdc_unkeyed_delete_note"
#: Row images kept on the run evidence (the DLQ keeps every one).
_EVIDENCE_SAMPLE = 20

_REASON = (
    "delete has no resolvable key on an unkeyed table; it cannot be applied by "
    "key, so it was parked instead of stopping the stream"
)


def unkeyed_delete_record(table: str, row: Any, lsn: Any = None) -> dict[str, Any]:
    """One parked delete: the table, the log position and the row image it carried."""
    from services.value_serializer import cell_to_string

    image: dict[str, Any] = {}
    if isinstance(row, dict):
        image = {str(k): cell_to_string(v, preserve_sql_null=True) for k, v in row.items()}
    return {
        "table": str(table or ""),
        "lsn": "" if lsn is None else str(lsn),
        "row": image,
        "image_complete": bool(image),
        "reason": _REASON,
    }


def null_key_part(key: Any, n_columns: int = 1) -> bool:
    """True when a delete key, or any column of a composite key, is SQL NULL.

    A delete key is wire text: a NULL column is the ``__DF_SQL_NULL__`` sentinel
    (or an absent-field spelling), and the user's own text that spells it
    arrives escaped. ``''`` is a value (D53), not NULL. A composite key
    (``n_columns > 1``) is joined with the keyset separator; a single-column key
    is one value even if it contains that character.
    """
    from services.keyset_pagination import KEYSET_SEP
    from services.value_serializer import is_reader_null_cell

    if key is None:
        return True
    if isinstance(key, (list, tuple)):
        return any(p is None or is_reader_null_cell(p) for p in key)
    text = str(key)
    parts = text.split(KEYSET_SEP) if n_columns > 1 else [text]
    return any(is_reader_null_cell(part) for part in parts)


def park_null_key_deletes(change: Any, key_columns: list[str] | None, *, table: str = "") -> int:
    """Move deletes whose key holds a NULL out of ``change.deletes`` into its parked list.

    The readers already park a delete whose key they cannot build. This is the
    apply step's backstop for any reader that still hands one through: such a
    key would otherwise run ``DELETE ... WHERE sku = '__DF_SQL_NULL__'``, match
    nothing (or a row the sentinel was wrongly written as) and be counted as an
    applied delete. Mutates ``change`` in place so every counter that reads it
    afterwards agrees; returns how many deletes were moved (0 is a no-op).
    """
    deletes = list(getattr(change, "deletes", None) or [])
    if not deletes:
        return 0
    cols = [c for c in (key_columns or []) if str(c).strip()]
    width = max(1, len(cols))
    keep = [k for k in deletes if not null_key_part(k, width)]
    if len(keep) == len(deletes):
        return 0
    from connectors.writer_common import extract_cdc_lsn
    from services.cdc_snapshot_window import _pk_row_dict

    lsn = extract_cdc_lsn(getattr(change, "resume_token", None))
    moved: list[dict[str, Any]] = []
    for key in deletes:
        if not null_key_part(key, width):
            continue
        if isinstance(key, (list, tuple)):
            image = {cols[i]: key[i] for i in range(min(len(cols), len(key)))} if cols else {}
        else:
            image = _pk_row_dict(cols, str(key)) if cols else {}
        # The image names NULL as NULL — never the sentinel text re-encoded.
        image = {k: (None if null_key_part(v) else v) for k, v in image.items()}
        moved.append(unkeyed_delete_record(table or str(getattr(change, "table", "") or ""), image, lsn))
    change.deletes = keep
    change.unkeyed_deletes = list(getattr(change, "unkeyed_deletes", None) or []) + moved
    logger.warning(
        "CDC: parked %d delete(s) on %s whose key holds a NULL (never a delete by the sentinel)",
        len(moved), table or getattr(change, "table", "") or "?",
    )
    return len(moved)


def parked_note(count: int, table: str) -> str:
    """Operator wording for ``count`` parked deletes on ``table``."""
    where = f" on '{table}'" if table else ""
    return (
        f"{count:,} CDC delete(s){where} had no key the destination row could be "
        "found by (the table has no primary key, or the delete did not carry the "
        "declared key). They were parked in the dead-letter queue with their row "
        "image and audited, and the stream continued; the destination still holds "
        "those rows. Apply the parked deletes, or give the table a primary key and "
        "re-snapshot it."
    )


def park_unkeyed_deletes(
    records: list[dict[str, Any]] | None,
    *,
    job_id: str,
    pipeline_key: str = "",
    dest_table: str = "",
    connector: str = "",
    workspace_id: str = "",
) -> dict[str, Any] | None:
    """Persist and audit one batch's parked deletes; ``None`` when there are none.

    Raises when either record cannot be written: the caller must not advance
    the stream position past deletes nobody can find again.
    """
    parked = [dict(r) for r in records or [] if isinstance(r, dict)]
    if not parked:
        return None
    if not str(job_id or "").strip():
        raise RuntimeError(
            "CDC unkeyed deletes present but job_id is missing — refuse to advance "
            "the stream position (the parked deletes could not be recorded)"
        )
    tables = sorted({str(r.get("table") or "") for r in parked} - {""})
    table = tables[0] if len(tables) == 1 else ", ".join(tables)
    lsns = [str(r.get("lsn") or "") for r in parked if r.get("lsn")]
    from services.quarantine_dlq import append_dlq_event

    event = append_dlq_event(
        job_id=str(job_id),
        action=DLQ_ACTION,
        rows=len(parked),
        workspace_id=workspace_id,
        details={
            "pipeline_key": pipeline_key,
            "source_table": table,
            "dest_table": dest_table,
            "connector": connector,
            "lsn_first": lsns[0] if lsns else "",
            "lsn_last": lsns[-1] if lsns else "",
            "deletes": parked,
        },
    )
    from services.audit_log import append_audit_event

    audit = append_audit_event(
        action=AUDIT_ACTION,
        resource=f"job:{job_id}",
        level="warning",
        workspace_id=workspace_id or None,
        outcome="parked",
        reason_code="cdc_delete_without_key",
        details={
            "job_id": str(job_id),
            "pipeline_key": pipeline_key,
            "source_table": table,
            "dest_table": dest_table,
            "count": len(parked),
            "dlq_event_id": str(event.get("id") or ""),
            "lsn_first": lsns[0] if lsns else "",
            "lsn_last": lsns[-1] if lsns else "",
        },
    )
    logger.warning(
        "CDC: parked %d unkeyed delete(s) on %s (job %s, DLQ %s)",
        len(parked), table or dest_table, job_id, event.get("id"),
    )
    return {
        "count": len(parked),
        "source_table": table,
        "dest_table": dest_table,
        "dlq_event_id": str(event.get("id") or ""),
        "audit_event_id": str((audit or {}).get("id") or (audit or {}).get("event_id") or ""),
        "sample": parked[:_EVIDENCE_SAMPLE],
    }


def stamp_parked(dest_summary: dict[str, Any] | None, evidence: dict[str, Any] | None) -> dict[str, Any]:
    """Put one batch's parked-delete evidence on its destination summary."""
    out = dest_summary if isinstance(dest_summary, dict) else {}
    if not evidence:
        return out
    out[SUMMARY_COUNT_KEY] = int(evidence.get("count") or 0)
    out[SUMMARY_EVIDENCE_KEY] = [dict(evidence)]
    out[SUMMARY_NOTE_KEY] = parked_note(
        int(evidence.get("count") or 0), str(evidence.get("source_table") or "")
    )
    return out


class ParkedDeleteTally:
    """Run-level fold of every batch's parked deletes (the merge keeps only the last batch)."""

    def __init__(self) -> None:
        self.count = 0
        self.evidence: list[dict[str, Any]] = []

    def add(self, batch_summary: dict[str, Any] | None) -> None:
        if not isinstance(batch_summary, dict):
            return
        n = int(batch_summary.get(SUMMARY_COUNT_KEY) or 0)
        if n <= 0:
            return
        self.count += n
        for ev in batch_summary.get(SUMMARY_EVIDENCE_KEY) or []:
            if len(self.evidence) < _EVIDENCE_SAMPLE:
                self.evidence.append(dict(ev))

    def stamp(self, summary: dict[str, Any] | None, *, cdc_meta: dict[str, Any] | None = None) -> None:
        """Run totals on the final summary (and its ``cdc`` block); a no-op at zero."""
        if not isinstance(summary, dict):
            return
        if isinstance(cdc_meta, dict):
            cdc_meta["unkeyed_deletes_parked"] = self.count
        if self.count <= 0:
            for key in (SUMMARY_COUNT_KEY, SUMMARY_EVIDENCE_KEY, SUMMARY_NOTE_KEY):
                summary.pop(key, None)
            return
        tables = sorted({str(e.get("source_table") or "") for e in self.evidence} - {""})
        summary[SUMMARY_COUNT_KEY] = self.count
        summary[SUMMARY_EVIDENCE_KEY] = list(self.evidence)
        summary[SUMMARY_NOTE_KEY] = parked_note(self.count, ", ".join(tables))
