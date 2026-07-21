"""Slim job documents for list endpoints — full detail stays on GET /jobs/{id}.

List payloads that include rejected_details / logs / mapping_proof routinely
exceed nginx proxy buffers and trip browser timeouts on Railway.
"""

from __future__ import annotations

from typing import Any

# Top-level keys safe to omit from Job Theater / Overview list cards.
_LIST_DROP_KEYS = frozenset({
    "rejected_details",
    "logs",
    "log_lines",
    "events",
    "chunks",
    "chunk_results",
    "mapping_proof",
    "sample_rows",
    "preview_rows",
    "quarantine_rows",
    "quarantine_samples",
    "row_samples",
    "dry_run_rows",
    "transform_errors",
    "gate_details",
    "preflight_raw",
    "explain",
    "agentic_repair",
})

_NESTED_DROP = {
    "destination_summary": frozenset({
        "rejected_details",
        "sample_rows",
        "preview_rows",
        "quarantine_samples",
        "write_samples",
    }),
    "source_summary": frozenset({
        "sample_rows",
        "preview_rows",
        "rows",
    }),
    "reconciliation": frozenset({
        "mismatches",
        "sample_mismatches",
        "details",
    }),
}


def slim_job_for_list(job: dict[str, Any] | None) -> dict[str, Any]:
    """Return a copy with heavy diagnostic arrays stripped for list views."""
    if not job:
        return {}
    out: dict[str, Any] = {}
    for key, value in job.items():
        if key in _LIST_DROP_KEYS:
            continue
        if key in _NESTED_DROP and isinstance(value, dict):
            drop = _NESTED_DROP[key]
            out[key] = {k: v for k, v in value.items() if k not in drop}
            continue
        out[key] = value
    # Preserve counts so UI still shows quarantine / reject totals without samples.
    if "rejected_rows" not in out and isinstance(job.get("destination_summary"), dict):
        ds = job["destination_summary"]
        if "rejected_rows" in ds:
            out.setdefault("rejected_rows", ds.get("rejected_rows"))
    return out
