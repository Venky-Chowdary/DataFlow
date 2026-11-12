"""CDC lag honesty SSOT — Debezium-class semantics, no heartbeat greenwash.

Standing rules:
* Heartbeat proves the consumer is alive — never that replication is caught up.
* "Caught up" needs both halves: no unapplied WAL/binlog bytes AND a live
  consumer. Bytes inside the jitter band are healthy but are not zero lag, and
  a figure left behind by a consumer that went silent proves nothing now.
* ``cdc_lag_seconds`` is only set from a proven source commit timestamp, or
  ``0`` when the pipeline is caught up.
* When behind on bytes but seconds are unknown, basis is ``wal_bytes`` and
  freshness severity uses byte thresholds — never invent a fake 0s lag.
* Default delivery remains at-least-once; this module does not claim exactly-once.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

# Jitter band: small residual WAL/binlog is normal between flushes. Inside it
# freshness is healthy, but it is not "caught up" — that needs zero bytes.
CATCH_UP_BYTES = 1 * 1024 * 1024  # 1 MiB
#: A consumer whose last heartbeat is older than this is not known to be alive
#: (the Freshness SLO's own stale-heartbeat threshold).
HEARTBEAT_STALE_SECONDS = 300.0
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
      ``replication_lag_bytes``, ``freshness_severity``, ``cdc_lag_unknown_reason``,
      ``caught_up`` (zero unapplied bytes and a live consumer) and
      ``consumer_alive`` (``None`` when no heartbeat is on record).
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

    consumer_alive = None if hb_age is None else hb_age <= HEARTBEAT_STALE_SECONDS
    stale = consumer_alive is False
    in_band = byte_lag is not None and byte_lag <= CATCH_UP_BYTES
    caught_up = byte_lag == 0 and consumer_alive is True
    behind = byte_lag is not None and byte_lag > CATCH_UP_BYTES

    if stale:
        # Every figure here was left by a consumer that has since gone silent:
        # the source kept moving, so neither "0 bytes" nor an old commit clock
        # describes the lag now.
        lag_seconds = None
        basis = LAG_BASIS_WAL_BYTES if byte_lag is not None else LAG_BASIS_UNKNOWN
        unknown_reason = (
            f"consumer heartbeat is {hb_age:.0f}s old (stale after "
            f"{HEARTBEAT_STALE_SECONDS:.0f}s); lag figures are from its last tick"
        )
    elif caught_up:
        # Nothing unapplied and the consumer is alive — seconds lag is 0 even
        # if the last event was hours ago (idle source).
        lag_seconds = 0.0
        basis = LAG_BASIS_WAL_BYTES
    elif in_band:
        # Healthy, but not zero: unapplied log, or zero bytes with no proof the
        # consumer is alive. Never a made-up 0 s.
        lag_seconds = None
        basis = LAG_BASIS_WAL_BYTES
        unknown_reason = (
            f"replication_lag_bytes={byte_lag} not yet confirmed (inside the "
            f"{CATCH_UP_BYTES}-byte jitter band): not proven caught up"
            if byte_lag
            else "no unapplied bytes, but no heartbeat proves the consumer is alive"
        )
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
    elif stale:
        severity = "unknown"
    elif sec_sev == "ok" or in_band:
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
        "consumer_alive": consumer_alive,
    }


def merge_freshness_severity(*parts: str | None) -> str:
    """Worst-of severity across pipelines.

    A pipeline nobody can vouch for is worse than a healthy one: ``unknown``
    outranks ``ok``, so one unknown pipeline never reads as an all-ok SLO.
    """
    order = {"critical": 3, "warn": 2, "unknown": 1, "ok": 0}
    worst: str | None = None
    for p in parts:
        name = str(p) if p else ""
        if name not in order:
            continue
        if worst is None or order[name] > order[worst]:
            worst = name
    return worst or "unknown"


def fold_evidence_severity(*parts: str | None) -> str:
    """Fold several readings of ONE pipeline into its severity.

    Here ``unknown`` means "this reading has nothing to say" (no byte probe,
    no commit clock), so it does not erase a proven reading of the same
    pipeline: a stamped lag that is ok stays ok.
    """
    order = {"critical": 3, "warn": 2, "ok": 1, "unknown": 0}
    best = "unknown"
    for p in parts:
        if p and order.get(str(p), 0) > order[best]:
            best = str(p)
    return best
