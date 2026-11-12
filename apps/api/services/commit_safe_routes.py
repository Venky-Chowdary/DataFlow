"""Commit-safe statements for routes that apply no read window (PR 407 review M4).

The commit-safe window (:mod:`services.commit_safe_window`) is bound to the
database streaming path. The batch path, a file source, a file-export
destination, an SDK source and a query-CDC poll read ``cursor > watermark``
with no commit-order bound, so a transaction that commits after a later value
was read leaves its row behind the watermark on those routes too. Each run of
such a route says so in its own evidence — the same record the window writes
(``destination_summary.commit_safe_window``), graded "not guaranteed", with the
key census recommended — whether or not preflight ran.

The stamp lives here, not in the engine, so the engine module keeps its size
budget: Gate-8's entry point (``reconcile_step.run_reconciliation``) and the
CDC poll call it.
"""

from __future__ import annotations

from typing import Any

from services.commit_safe_window import (
    EVIDENCE_KEY,
    GUARANTEE_NONE,
    STRATEGY_NONE,
    unguarded_reason,
)


def unwindowed_evidence(route: str) -> dict[str, Any]:
    """The evidence of an incremental run on ``route``, which applies no window."""
    return {
        "strategy": STRATEGY_NONE,
        "guarantee": GUARANTEE_NONE,
        "reason": unguarded_reason(f"the {route} applies no commit-safe read window"),
        "stored_watermark": None,
        "replay_seed": None,
        "census_recommended": True,
        "route": route,
    }


def stamp_unwindowed_route(
    dest_summary: Any, *, route: str, incremental: bool | None = None
) -> None:
    """Record "not guaranteed" for an incremental run that applied no window.

    A run that planned a window keeps its own record. ``incremental`` defaults
    to the summary's ``sync_mode``; a CDC poll passes True (it reads
    ``cursor > watermark`` under the ``cdc`` mode).
    """
    if not isinstance(dest_summary, dict) or dest_summary.get(EVIDENCE_KEY):
        return
    if incremental is None:
        from services.sync_cursor import normalize_sync_mode, requires_incremental

        mode = str(dest_summary.get("sync_mode") or dest_summary.get("effective_sync_mode") or "")
        incremental = bool(mode) and requires_incremental(normalize_sync_mode(mode, default=mode))
    if incremental:
        dest_summary[EVIDENCE_KEY] = unwindowed_evidence(route)


def reconciled_route_label(source_endpoint: Any, endpoint: Any, dest_summary: Any) -> str:
    """Which path a run Gate-8 verifies took, in the words its evidence uses."""
    from services.brand_env import getenv_brand_bool
    from services.type_system import destination_is_file_export

    if not getenv_brand_bool("DATAFLOW_COMMIT_SAFE_WINDOW", True):
        return "route (the window is switched off: DATAFLOW_COMMIT_SAFE_WINDOW=0)"
    dest_kind = str(getattr(endpoint, "kind", "") or "").lower()
    dest_format = str(getattr(endpoint, "format", "") or "").lower()
    if dest_kind == "file_export" or destination_is_file_export(dest_format):
        return "file-export destination"
    src_kind = str(getattr(source_endpoint, "kind", "") or "").lower()
    if src_kind in {"file", "cloud", "object_store", "upload"}:
        return "file source"
    try:
        from connectors.sdk import get_sdk_connector

        if get_sdk_connector(str(getattr(source_endpoint, "format", "") or "")) is not None:
            return "SDK source"
    except Exception:  # noqa: BLE001 — an unknown connector is named generically below
        pass
    summary = dest_summary if isinstance(dest_summary, dict) else {}
    return "read path" if summary.get("streaming") else "batch path"


def stamp_reconciled_route(dest_summary: Any, *, source_endpoint: Any, endpoint: Any) -> None:
    """Gate-8 entry: state "not guaranteed" for an incremental run with no window."""
    if isinstance(dest_summary, dict) and not dest_summary.get(EVIDENCE_KEY):
        stamp_unwindowed_route(
            dest_summary, route=reconciled_route_label(source_endpoint, endpoint, dest_summary)
        )
