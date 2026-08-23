"""MongoDB transfer_jobs document budget — enterprise control-plane SSOT.

MongoDB rejects updates when the ``update`` command exceeds ~16 MiB
(``DocumentTooLarge`` / ``'update' command document too large``). Transfer jobs
historically embedded full quarantine ``rejected_details`` and writer
checkpoints into every progress ``$set``, which fails Excel→SQL and other
high-reject routes even when the destination write path is healthy.

Rules
-----
* Quarantine **ledger** lives in DLQ (``quarantine_dlq``) — fail-closed there.
* Job document keeps a **bounded Theater preview** + totals only.
* ``update_job_status`` must always run :func:`trim_job_update_payload` before
  ``update_one`` and retry once with an emergency strip on DocumentTooLarge.
* Never invent success by dropping quarantine counts — only drop preview cells.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)

# MongoDB BSON document hard limit is 16 MiB. Budget the $set payload lower so
# existing job fields + wire overhead still fit after $set merge.
MONGO_BSON_LIMIT_BYTES = 16 * 1024 * 1024
UPDATE_SET_BUDGET_BYTES = int(
    getenv_brand("JOB_UPDATE_SET_BUDGET_BYTES", str(10 * 1024 * 1024)) or (10 * 1024 * 1024)
)
# Theater / Jobs UI preview — DLQ holds the full ledger.
JOB_REJECTED_PREVIEW_MAX = int(getenv_brand("JOB_REJECTED_PREVIEW_MAX", "40") or 40)
JOB_REJECTED_CELL_CHARS = int(getenv_brand("JOB_REJECTED_CELL_CHARS", "240") or 240)
JOB_CHECKPOINT_REJECTED_PREVIEW = int(
    getenv_brand("JOB_CHECKPOINT_REJECTED_PREVIEW", "25") or 25
)
JOB_MESSAGE_CHARS = int(getenv_brand("JOB_MESSAGE_CHARS", "2000") or 2000)
JOB_EVENT_LOG_MAX = int(getenv_brand("JOB_EVENT_LOG_MAX", "100") or 100)

# Nested blobs that historically blew the update command.
_HEAVY_TOP_KEYS = frozenset(
    {
        "rejected_details",
        "checkpoint",
        "destination_summary",
        "error_details",
        "reconcile",
        "proof_bundle",
        "mappings",
        "sample_rows",
        "signed_mappings",
        "stamped_mappings",
        "event_log",
        "warnings",
        "transform_errors",
    }
)


def estimate_bson_size(value: Any) -> int:
    """Best-effort BSON size; falls back to UTF-8 JSON length."""
    try:
        from bson import encode

        return len(encode(value if isinstance(value, dict) else {"_": value}))
    except Exception:
        try:
            return len(
                json.dumps(value, default=str, ensure_ascii=False).encode("utf-8")
            )
        except Exception:
            return len(str(value).encode("utf-8", errors="replace"))


def slim_rejected_detail(detail: Any, *, cell_chars: int = JOB_REJECTED_CELL_CHARS) -> dict[str, Any]:
    """Cap one quarantine evidence row for job-document storage."""
    if not isinstance(detail, dict):
        return {"message": str(detail)[:cell_chars]}
    out: dict[str, Any] = {}
    for key in (
        "row",
        "row_number",
        "index",
        # The offending value is the finding: without it Inspect shows a reason
        # for a cell it cannot name, and the export has an empty column.
        "value",
        "original_value",
        "reason",
        "message",
        "error",
        "column",
        "source",
        "target",
        "transform",
        "policy",
        "failure_class",
    ):
        if key in detail and detail[key] is not None:
            val = detail[key]
            out[key] = str(val)[:cell_chars] if isinstance(val, str) else val
    for key in ("values", "source_values", "raw", "cells", "row_values"):
        if key not in detail:
            continue
        raw = detail[key]
        if isinstance(raw, dict):
            slim: dict[str, Any] = {}
            for i, (ck, cv) in enumerate(raw.items()):
                if i >= 24:
                    slim["…"] = f"+{len(raw) - 24} fields"
                    break
                slim[str(ck)[:80]] = (
                    str(cv)[:cell_chars]
                    if cv is not None and not isinstance(cv, (int, float, bool))
                    else cv
                )
            out[key] = slim
        elif isinstance(raw, list):
            out[key] = [
                str(x)[:cell_chars] if x is not None else x for x in raw[:24]
            ]
        else:
            out[key] = str(raw)[:cell_chars]
    if "message" not in out and detail.get("error"):
        out["message"] = str(detail.get("error"))[:cell_chars]
    return out


def slim_rejected_details(
    details: list[Any] | None,
    *,
    limit: int = JOB_REJECTED_PREVIEW_MAX,
    cell_chars: int = JOB_REJECTED_CELL_CHARS,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Return (preview, total_count, truncated)."""
    rows = list(details or [])
    total = len(rows)
    preview = [slim_rejected_detail(d, cell_chars=cell_chars) for d in rows[: max(0, limit)]]
    return preview, total, total > len(preview)


def slim_checkpoint_for_job_store(checkpoint: Any) -> dict[str, Any] | Any:
    """Keep resume tokens + counters; bound quarantine preview on the job doc.

    Full rejected_details must already be (or be going to) DLQ via
    ``_persist_checkpoint_quarantine_delta`` — the job blob is not the ledger.
    """
    if not isinstance(checkpoint, dict):
        return checkpoint
    out = dict(checkpoint)
    details = out.get("rejected_details")
    if isinstance(details, list):
        preview, total, truncated = slim_rejected_details(
            details, limit=JOB_CHECKPOINT_REJECTED_PREVIEW
        )
        out["rejected_details"] = preview
        out["rejected_details_total"] = max(
            int(out.get("rejected_details_total") or 0),
            int(out.get("rejected_rows") or 0),
            total,
        )
        out["rejected_details_truncated"] = bool(
            truncated or out.get("rejected_details_truncated")
        )
        # Preserve exact reject count for Theater honesty.
        if not out.get("rejected_rows"):
            out["rejected_rows"] = int(out["rejected_details_total"])
    # Drop accidental sample dumps.
    for heavy in ("sample_rows", "mapped_rows", "batch_rows", "preview_rows"):
        out.pop(heavy, None)
    return out


def slim_destination_summary(summary: Any) -> dict[str, Any] | Any:
    if not isinstance(summary, dict):
        return summary
    out = dict(summary)
    details = out.get("rejected_details")
    if isinstance(details, list):
        preview, total, truncated = slim_rejected_details(details)
        out["rejected_details"] = preview
        out["rejected_details_total"] = max(
            int(out.get("rejected_details_total") or 0),
            int(out.get("rejected_rows") or 0),
            total,
        )
        out["rejected_details_truncated"] = bool(
            truncated or out.get("rejected_details_truncated")
        )
    if isinstance(out.get("checkpoint"), dict):
        out["checkpoint"] = slim_checkpoint_for_job_store(out["checkpoint"])
    return out


def trim_job_update_payload(
    updates: dict[str, Any],
    *,
    budget_bytes: int = UPDATE_SET_BUDGET_BYTES,
) -> dict[str, Any]:
    """Return a copy of ``updates`` that fits under the BSON update budget."""
    out = dict(updates)

    if "message" in out and out["message"] is not None:
        out["message"] = str(out["message"])[:JOB_MESSAGE_CHARS]
    if "error" in out and out["error"] is not None:
        out["error"] = str(out["error"])[:JOB_MESSAGE_CHARS]

    if isinstance(out.get("event_log"), list):
        out["event_log"] = list(out["event_log"])[-JOB_EVENT_LOG_MAX:]

    if "rejected_details" in out and isinstance(out["rejected_details"], list):
        preview, total, truncated = slim_rejected_details(out["rejected_details"])
        out["rejected_details"] = preview
        out["rejected_details_total"] = max(
            int(out.get("rejected_details_total") or 0), total
        )
        out["rejected_details_truncated"] = bool(
            truncated or out.get("rejected_details_truncated")
        )
        if "rejected_rows" not in out:
            out["rejected_rows"] = int(out["rejected_details_total"])

    if "checkpoint" in out:
        out["checkpoint"] = slim_checkpoint_for_job_store(out["checkpoint"])

    if "destination_summary" in out:
        out["destination_summary"] = slim_destination_summary(out["destination_summary"])

    if isinstance(out.get("error_details"), dict):
        ed = dict(out["error_details"])
        for k, v in list(ed.items()):
            if isinstance(v, str) and len(v) > JOB_MESSAGE_CHARS:
                ed[k] = v[:JOB_MESSAGE_CHARS]
            elif isinstance(v, (list, dict)) and estimate_bson_size(v) > 32_768:
                ed[k] = {"truncated": True, "preview": str(v)[:512]}
        out["error_details"] = ed

    # Drop accidental full mapping / sample dumps from status writes.
    for key in ("mappings", "sample_rows", "signed_mappings", "stamped_mappings"):
        if key in out and estimate_bson_size(out[key]) > 64_768:
            out[key] = {"truncated": True, "count": len(out[key]) if hasattr(out[key], "__len__") else 1}

    size = estimate_bson_size(out)
    if size <= budget_bytes:
        return out

    # Progressive strip of heavy keys until under budget.
    for key in (
        "rejected_details",
        "checkpoint",
        "destination_summary",
        "reconcile",
        "proof_bundle",
        "warnings",
        "transform_errors",
        "error_details",
        "event_log",
        "phases",
        "mappings",
        "sample_rows",
    ):
        if key not in out:
            continue
        if key == "rejected_details":
            out[key] = []
            out["rejected_details_truncated"] = True
        elif key == "destination_summary" and isinstance(out[key], dict):
            ds = dict(out[key])
            ds["rejected_details"] = []
            ds["rejected_details_truncated"] = True
            ds.pop("checkpoint", None)
            out[key] = ds
        elif key == "checkpoint" and isinstance(out[key], dict):
            out[key] = _resume_tokens_only(out[key])
        elif key == "event_log":
            out[key] = list(out.get("event_log") or [])[-20:]
        elif key == "phases":
            # Keep phase labels only — drop long messages.
            phases = out[key]
            if isinstance(phases, list):
                out[key] = [
                    {
                        "id": p.get("id"),
                        "status": p.get("status"),
                        "label": p.get("label"),
                    }
                    if isinstance(p, dict)
                    else p
                    for p in phases[:12]
                ]
            else:
                out.pop(key, None)
        else:
            out[key] = {"truncated": True, "reason": "job_document_budget"}
        size = estimate_bson_size(out)
        if size <= budget_bytes:
            break

    if estimate_bson_size(out) > budget_bytes:
        # Guaranteed under budget — status/progress/resume only.
        return emergency_strip_job_update(out)

    out["_job_document_budget"] = {
        "trimmed": True,
        "approx_bytes": size,
        "budget_bytes": budget_bytes,
    }
    return out


def _resume_tokens_only(checkpoint: dict[str, Any]) -> dict[str, Any]:
    """Checkpoint slice safe for job $set — resume tokens + counters, no evidence."""
    keys = (
        "job_id",
        "phase",
        "source_type",
        "dest_type",
        "chunk_index",
        "chunk_total",
        "rows_processed",
        "cursor_column",
        "cursor_value",
        "offset",
        "file_offset",
        "dynamodb_cursor",
        "es_search_after",
        "kafka_cursor",
        "checksum",
        "write_mode",
        "conflict_columns",
        "attempt",
        "max_attempts",
        "status",
        "updated_at",
        "rejected_rows",
        "rejected_details_total",
        "rejected_details_truncated",
        "quarantine_dlq_persisted_count",
    )
    out = {k: checkpoint[k] for k in keys if k in checkpoint}
    # redis_scan_state can be large — keep only if small.
    rss = checkpoint.get("redis_scan_state")
    if rss is not None and estimate_bson_size(rss) <= 4096:
        out["redis_scan_state"] = rss
    out["rejected_details"] = []
    out["rejected_details_truncated"] = True
    if "rejected_rows" not in out and checkpoint.get("rejected_details_total"):
        out["rejected_rows"] = int(checkpoint.get("rejected_details_total") or 0)
    return out


def _rejected_count_from_updates(updates: dict[str, Any]) -> int:
    """Prefer nested destination_summary / checkpoint totals over missing root."""
    candidates = [
        updates.get("rejected_rows"),
        updates.get("rejected_details_total"),
    ]
    ds = updates.get("destination_summary")
    if isinstance(ds, dict):
        candidates.extend(
            [ds.get("rejected_rows"), ds.get("rejected_details_total")]
        )
    cp = updates.get("checkpoint")
    if isinstance(cp, dict):
        candidates.extend(
            [cp.get("rejected_rows"), cp.get("rejected_details_total")]
        )
    best = 0
    for c in candidates:
        try:
            best = max(best, int(c or 0))
        except (TypeError, ValueError):
            continue
    return best


def emergency_strip_job_update(updates: dict[str, Any]) -> dict[str, Any]:
    """Last-resort $set payload after DocumentTooLarge — status + resume tokens."""
    keep_keys = {
        "status",
        "updated_at",
        "started_at",
        "completed_at",
        "phase",
        "failed_at_phase",
        "progress_pct",
        "message",
        "error",
        "records_processed",
        "rejected_rows",
        "rejected_details_total",
        "rejected_details_truncated",
        "chunk_current",
        "chunk_total",
        "lease_fence",
        "cancel_requested",
    }
    out = {k: updates[k] for k in keep_keys if k in updates}
    if "message" in out:
        out["message"] = str(out["message"])[:JOB_MESSAGE_CHARS]
    if "error" in out:
        out["error"] = str(out["error"])[:JOB_MESSAGE_CHARS]

    reject_n = _rejected_count_from_updates(updates)
    out["rejected_rows"] = reject_n
    out["rejected_details_total"] = max(
        int(out.get("rejected_details_total") or 0), reject_n
    )
    out["rejected_details"] = []
    out["rejected_details_truncated"] = True

    # Always rewrite checkpoint to resume-tokens-only when present — never leave
    # a stale fat checkpoint beside advanced records_processed (resume cliff).
    if isinstance(updates.get("checkpoint"), dict):
        out["checkpoint"] = _resume_tokens_only(updates["checkpoint"])

    checksum = None
    ds_in = updates.get("destination_summary")
    if isinstance(ds_in, dict) and ds_in.get("checksum"):
        checksum = ds_in.get("checksum")
    elif isinstance(updates.get("checkpoint"), dict):
        checksum = updates["checkpoint"].get("checksum")
    dest_summary: dict[str, Any] = {
        "rejected_rows": reject_n,
        "rejected_details": [],
        "rejected_details_total": reject_n,
        "rejected_details_truncated": True,
        "job_document_budget_emergency": True,
        "note": (
            "Job control-plane update was truncated to fit MongoDB 16 MiB limit. "
            "Resume tokens preserved when present. Quarantine ledger remains in "
            "DLQ when persist succeeded."
        ),
    }
    if checksum:
        dest_summary["checksum"] = checksum
    out["destination_summary"] = dest_summary

    out["_job_document_budget"] = {"emergency_strip": True}
    return out


def is_document_too_large_error(exc: BaseException) -> bool:
    msg = str(exc or "").lower()
    name = type(exc).__name__.lower()
    if "documenttoolarge" in name or "document too large" in msg:
        return True
    if "command document too large" in msg or "bsonobjsize" in msg:
        return True
    if "16mb" in msg or "16777216" in msg:
        return True
    return False


def apply_job_update_with_budget(
    collection: Any,
    filt: dict[str, Any],
    updates: dict[str, Any],
) -> Any:
    """``update_one`` with trim + one emergency retry on DocumentTooLarge."""
    trimmed = trim_job_update_payload(updates)
    try:
        return collection.update_one(filt, {"$set": trimmed})
    except Exception as exc:
        if not is_document_too_large_error(exc):
            raise
        logger.error(
            "transfer_jobs update DocumentTooLarge — emergency strip retry "
            "(approx_bytes=%s): %s",
            estimate_bson_size(trimmed),
            exc,
            exc_info=exc,
        )
        emergency = emergency_strip_job_update(trimmed)
        return collection.update_one(filt, {"$set": emergency})
