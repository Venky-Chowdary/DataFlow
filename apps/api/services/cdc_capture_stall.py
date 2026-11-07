"""SQL Server CDC capture-stall honesty — reader-at-tip ≠ capture healthy.

``sys.fn_cdc_get_max_lsn()`` is the tip of *captured* changes, not source
commits. When the capture agent stalls, max_lsn freezes and a reader sitting
on that tip looks caught up while source writes are silently lost until
retention gaps. Classify stall from ``sys.dm_cdc_log_scan_sessions`` latency /
errors — never invent healthy from frozen tip alone (idle source is normal).
"""

from __future__ import annotations

from typing import Any

# Microsoft DMV latency is capture lag in seconds (session_id = 0 = current).
STALL_WARN_SEC = 60.0
STALL_CRITICAL_SEC = 300.0
# Frozen max_lsn only contributes when scan latency also proves capture lag.
FREEZE_CONFIRM_SEC = 120.0


def classify_mssql_capture_stall(
    *,
    max_lsn: str = "",
    frozen_for_sec: float | None = None,
    scan_latency_sec: float | None = None,
    error_count: int | None = None,
    failed_sessions_count: int | None = None,
    dmv_available: bool = True,
    stall_warn_sec: float = STALL_WARN_SEC,
    stall_critical_sec: float = STALL_CRITICAL_SEC,
    freeze_confirm_sec: float = FREEZE_CONFIRM_SEC,
) -> dict[str, Any]:
    """Return stall classification for Theater / Freshness.

    Keys: ``capture_stall``, ``capture_stall_severity``, ``capture_stall_reason``,
    ``capture_latency_seconds``, ``dmv_available``.
    """
    out: dict[str, Any] = {
        "capture_stall": False,
        "capture_stall_severity": None,
        "capture_stall_reason": None,
        "capture_latency_seconds": scan_latency_sec,
        "dmv_available": bool(dmv_available),
        "max_lsn": max_lsn or None,
        "frozen_for_sec": frozen_for_sec,
    }

    errors = int(error_count or 0)
    failed = int(failed_sessions_count or 0)

    if not dmv_available and scan_latency_sec is None:
        out["capture_stall_reason"] = (
            "dm_cdc_log_scan_sessions unavailable — capture health unknown "
            "(do not invent healthy from reader-at-tip)"
        )
        out["capture_stall_severity"] = "unknown"
        return out

    if errors > 0 or failed > 0:
        out["capture_stall"] = True
        out["capture_stall_severity"] = "critical"
        out["capture_stall_reason"] = (
            f"CDC log scan errors (error_count={errors}, "
            f"failed_sessions_count={failed}) — capture agent unhealthy; "
            "reader at max_lsn is not catch-up"
        )
        return out

    latency = scan_latency_sec
    try:
        latency_f = float(latency) if latency is not None else None
    except (TypeError, ValueError):
        latency_f = None
    out["capture_latency_seconds"] = latency_f

    if latency_f is not None and latency_f >= float(stall_critical_sec):
        out["capture_stall"] = True
        out["capture_stall_severity"] = "critical"
        out["capture_stall_reason"] = (
            f"CDC capture latency {latency_f:.1f}s exceeds critical "
            f"({stall_critical_sec:.0f}s) — max_lsn may be frozen while source commits"
        )
        return out

    if latency_f is not None and latency_f >= float(stall_warn_sec):
        # Frozen tip + proven latency strengthens the warn → critical.
        try:
            frozen = float(frozen_for_sec) if frozen_for_sec is not None else 0.0
        except (TypeError, ValueError):
            frozen = 0.0
        if frozen >= float(freeze_confirm_sec) and max_lsn:
            out["capture_stall"] = True
            out["capture_stall_severity"] = "critical"
            out["capture_stall_reason"] = (
                f"max_lsn frozen {frozen:.0f}s with capture latency {latency_f:.1f}s — "
                "reader at tip is not catch-up; check CDC capture job"
            )
            return out
        out["capture_stall"] = True
        out["capture_stall_severity"] = "warn"
        out["capture_stall_reason"] = (
            f"CDC capture latency {latency_f:.1f}s exceeds warn ({stall_warn_sec:.0f}s)"
        )
        return out

    return out
