"""Which destination rows an incremental_append transform proof grades (D359).

``incremental_append`` keeps every version of a key (R6). A read-back scoped by
key alone returns the earlier versions too, so a correct new version looked
untransformed. The run's cursor window selects exactly the versions this run
appended (R9: verify exactly this run's writes).

The window is used only when it can be: the run recorded it, and the cursor
column lands under a mapped destination column. Otherwise the proof keeps the
key-scoped read-back it always had. A run that was verifiable stays verifiable;
it never becomes "unverifiable" because a window is missing.
"""

from __future__ import annotations

from typing import Any

INCREMENTAL_APPEND_MODES = frozenset({"incremental_append", "incremental_append_only"})


def append_versions_by_window(
    dest_summary: dict[str, Any] | None,
    sync_mode: str,
    mappings: list[dict[str, Any]] | None,
    digest_cols: list[str],
) -> bool:
    """True when the proof should scope the destination by this run's cursor window."""
    from services.incremental_drift import RunWindow
    from services.sync_cursor import normalize_sync_mode

    if normalize_sync_mode(sync_mode or "", default="") not in INCREMENTAL_APPEND_MODES:
        return False
    summary = dest_summary if isinstance(dest_summary, dict) else {}
    window = RunWindow.from_summary(summary)
    if window is None:
        return False
    to_target = {
        str(m.get("source")): str(m.get("target"))
        for m in (mappings or [])
        if isinstance(m, dict) and m.get("source") and m.get("target")
    }
    dest_cursor = to_target.get(window.cursor, window.cursor)
    return dest_cursor in set(digest_cols or [])
