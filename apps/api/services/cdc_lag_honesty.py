"""CDC lag honesty SSOT — Debezium-class semantics, no heartbeat greenwash.

Standing rules:
* Heartbeat proves the consumer is alive — never that replication is caught up.
* ``cdc_lag_seconds`` is only set from a proven source commit timestamp, or
  ``0`` when WAL/binlog byte lag proves catch-up.
* When behind on bytes but seconds are unknown, basis is ``wal_bytes`` and
  freshness severity uses byte thresholds — never invent a fake 0s lag.
* Default delivery remains at-least-once; this module does not claim exactly-once.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

# Caught-up band: small residual WAL/binlog is normal (flush jitter).
CATCH_UP_BYTES = 1 * 1024 * 1024  # 1 MiB
BYTE_WARN = 16 * 1024 * 1024  # 16 MiB — Theater already surfaces MB lag
BYTE_CRITICAL = 64 * 1024 * 1024  # 64 MiB — matches JobTheater alert band

LAG_BASIS_COMMIT_TS = "commit_ts"
LAG_BASIS_WAL_BYTES = "wal_bytes"
LAG_BASIS_UNKNOWN = "unknown"


def _aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def age_seconds(anchor: datetime | None, *, now: datetime | None = None) -> float | None:
    """Non-negative age of ``anchor`` vs ``now``, or None when unknown."""
    a = _aware(anchor)
    if a is None:
        return None
    n = _aware(now) or datetime.now(timezone.utc)
    return max(0.0, (n - a).total_seconds())


def severity_from_byte_lag(lag_bytes: int | None) -> str | None:
    """Return warn/critical from byte lag, or None when not behind."""
    if lag_bytes is None:
        return None
    b = int(lag_bytes)
    if b >= BYTE_CRITICAL:
        return "critical"
    if b >= BYTE_WARN:
        return "warn"
    return None


def observe_cdc_lag(
    *,
    last_event_commit_at: datetime | None = None,
    last_heartbeat_at: datetime | None = None,
    replication_lag_bytes: int | None = None,
    now: datetime | None = None,
    max_lag_warn_seconds: float = 60.0,
    max_lag_critical_seconds: float | None = None,
) -> dict[str, Any]:
    """Compute honest lag fields for job docs / Freshness SLO / Theater.

    Returns keys:
      ``cdc_lag_seconds``, ``cdc_lag_basis``, ``cdc_heartbeat_age_sec``,
      ``replication_lag_bytes``, ``freshness_severity``, ``cdc_lag_unknown_reason``.
    """
    n = _aware(now) or datetime.now(timezone.utc)
    hb_age = age_seconds(last_heartbeat_at, now=n)
    commit_lag = age_seconds(last_event_commit_at, now=n)
    try:
        byte_lag = int(replication_lag_bytes) if replication_lag_bytes is not None else None
    except (TypeError, ValueError):
        byte_lag = None

    critical_floor = float(
        max_lag_critical_seconds
        if max_lag_critical_seconds is not None
        else max(max_lag_warn_seconds * 5.0, max_lag_warn_seconds + 60.0)
    )

    lag_seconds: float | None = None
    basis = LAG_BASIS_UNKNOWN
    unknown_reason: str | None = None

    caught_up = byte_lag is not None and byte_lag <= CATCH_UP_BYTES
    behind = byte_lag is not None and byte_lag > CATCH_UP_BYTES

    if caught_up:
        # Proven flush/binlog catch-up — seconds lag is 0 even if last event
        # was hours ago (idle source). Heartbeat must not be required.
        lag_seconds = 0.0
        basis = LAG_BASIS_WAL_BYTES
    elif commit_lag is not None:
        lag_seconds = commit_lag
        basis = LAG_BASIS_COMMIT_TS
    elif behind:
        # Behind on bytes without a commit clock — refuse fake seconds.
        lag_seconds = None
        basis = LAG_BASIS_WAL_BYTES
        unknown_reason = (
            f"replication_lag_bytes={byte_lag} behind catch-up band; "
            "source commit timestamp unavailable"
        )
    else:
        lag_seconds = None
        basis = LAG_BASIS_UNKNOWN
        unknown_reason = (
            "no proven commit timestamp and no WAL/binlog byte lag probe"
        )

    byte_sev = severity_from_byte_lag(byte_lag)
    sec_sev: str | None = None
    if lag_seconds is not None:
        if lag_seconds > critical_floor:
            sec_sev = "critical"
        elif lag_seconds > max_lag_warn_seconds:
            sec_sev = "warn"
        else:
            sec_sev = "ok"

    if byte_sev == "critical" or sec_sev == "critical":
        severity = "critical"
    elif byte_sev == "warn" or sec_sev == "warn":
        severity = "warn"
    elif sec_sev == "ok" or (caught_up and sec_sev is None):
        severity = "ok"
    else:
        severity = "unknown"

    return {
        "cdc_lag_seconds": lag_seconds,
        "cdc_lag_basis": basis,
        "cdc_heartbeat_age_sec": hb_age,
        "replication_lag_bytes": byte_lag,
        "freshness_severity": severity,
        "cdc_lag_unknown_reason": unknown_reason,
        "caught_up": caught_up,
    }


def merge_freshness_severity(*parts: str | None) -> str:
    """Worst-of severity merge for multi-pipeline SLO."""
    order = {"critical": 3, "warn": 2, "ok": 1, "unknown": 0}
    best = "unknown"
    for p in parts:
        if not p:
            continue
        if order.get(str(p), 0) > order.get(best, 0):
            best = str(p)
    return best
