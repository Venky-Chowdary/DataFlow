"""Slim job documents for list endpoints — full detail stays on GET /jobs/{id}.

List payloads that include rejected_details / logs / mapping_proof / preflight
routinely exceed ~100KB on Railway and trip browser timeouts + false offline banners.
"""

from __future__ import annotations

from typing import Any

# Whitelist: only fields the Jobs list / Overview cards need.
_LIST_KEEP_KEYS = frozenset({
    "_id",
    "id",
    "job_id",
    "name",
    "status",
    "source_type",
    "source_name",
    "destination_type",
    "destination_database",
    "destination_collection",
    "records_processed",
    "total_rows",
    "rejected_rows",
    "coerced_null_rows",
    "progress_pct",
    "progress_indeterminate",
    "phase",
    "message",
    "error",
    "error_code",
    "error_title",
    "failed_at_phase",
    "created_at",
    "updated_at",
    "started_at",
    "completed_at",
    "workspace_id",
    "sync_mode",
    "records_per_second",
    "chunk_current",
    "chunk_total",
    "chunk_size",
    "operation",
    "triggered_by",
    "created_by",
    "retry_of",
    "cdc_lag_seconds",
})


def slim_job_for_list(job: dict[str, Any] | None) -> dict[str, Any]:
    """Return a compact copy for list views — never ship quarantine samples here."""
    if not job:
        return {}
    out: dict[str, Any] = {}
    for key, value in job.items():
        if key in _LIST_KEEP_KEYS:
            out[key] = value
    # Preserve reject count if only nested under destination_summary.
    if "rejected_rows" not in out and isinstance(job.get("destination_summary"), dict):
        ds = job["destination_summary"]
        if "rejected_rows" in ds:
            out["rejected_rows"] = ds.get("rejected_rows")
        if "coerced_null_rows" in ds and "coerced_null_rows" not in out:
            out["coerced_null_rows"] = ds.get("coerced_null_rows")
    # Tiny checkpoint summary for Resume affordance (no row samples).
    cp = job.get("checkpoint")
    if isinstance(cp, dict):
        out["checkpoint"] = {
            k: cp.get(k)
            for k in ("chunk_index", "offset", "rows_processed", "phase", "status", "cursor_column")
            if k in cp
        }
    return out
