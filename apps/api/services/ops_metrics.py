"""In-process Prometheus-compatible ops metrics for Datawrap transfers.

Exposes counters/gauges for job outcomes, quarantine, CDC lag, and reconcile
results without requiring an external APM dependency. Scrapable at ``GET /metrics``.
JSON snapshot + per-pipeline lag at ``GET /ops/freshness``.
"""

from __future__ import annotations

import threading
import time
from typing import Any

_lock = threading.Lock()

_counters: dict[str, float] = {  # nosec B105
    "dataflow_jobs_total": 0.0,
    "dataflow_jobs_completed_total": 0.0,
    "dataflow_jobs_failed_total": 0.0,
    "dataflow_jobs_quarantine_total": 0.0,
    "dataflow_rows_processed_total": 0.0,
    "dataflow_rows_quarantined_total": 0.0,
    "dataflow_reconcile_pass_total": 0.0,
    "dataflow_reconcile_fail_total": 0.0,
    "dataflow_cdc_polls_total": 0.0,
    "dataflow_cdc_fallback_query_total": 0.0,
}

_gauges: dict[str, float] = {
    "dataflow_cdc_lag_seconds": 0.0,
    "dataflow_cdc_lag_bytes": 0.0,
    "dataflow_jobs_running": 0.0,
}

# Labeled series: metric_name -> {label_key -> value}
_labeled_gauges: dict[str, dict[str, float]] = {
    "dataflow_pipeline_lag_seconds": {},
    "dataflow_pipeline_lag_bytes": {},
}
_labeled_counters: dict[str, dict[str, float]] = {
    "dataflow_pipeline_cdc_polls_total": {},
}
_pipeline_heartbeat: dict[str, float] = {}


def _inc(name: str, amount: float = 1.0) -> None:
    with _lock:
        _counters[name] = float(_counters.get(name, 0.0)) + amount


def _set_gauge(name: str, value: float) -> None:
    with _lock:
        _gauges[name] = float(value)


def _label_key(*, schedule_id: str = "", stream: str = "", job_id: str = "") -> str:
    sid = (schedule_id or "").strip() or "_"
    st = (stream or "").strip() or "_"
    jid = (job_id or "").strip() or "_"
    return f"schedule_id={sid},stream={st},job_id={jid}"


def record_job_outcome(
    *,
    status: str,
    records: int = 0,
    quarantined: int = 0,
    reconcile_ok: bool | None = None,
) -> None:
    """Record a finished job for scrapeable ops metrics."""
    _inc("dataflow_jobs_total")
    st = (status or "").lower()
    if st in {"completed", "completed_with_quarantine", "success"}:
        _inc("dataflow_jobs_completed_total")
    elif st in {"failed", "cancelled", "error"}:
        _inc("dataflow_jobs_failed_total")
    if quarantined > 0 or st == "completed_with_quarantine":
        _inc("dataflow_jobs_quarantine_total")
    if records:
        _inc("dataflow_rows_processed_total", float(records))
    if quarantined:
        _inc("dataflow_rows_quarantined_total", float(quarantined))
    if reconcile_ok is True:
        _inc("dataflow_reconcile_pass_total")
    elif reconcile_ok is False:
        _inc("dataflow_reconcile_fail_total")


def record_terminal_job_transition(
    *,
    previous_status: str | None,
    status: str,
    records: int = 0,
    quarantined: int = 0,
    reconcile_ok: bool | None = None,
) -> None:
    """Record metrics only when a job first enters a terminal status."""
    try:
        from services.job_status import is_terminal
    except ImportError:  # pragma: no cover
        from src.services.job_status import is_terminal

    if not is_terminal(status) or is_terminal(previous_status):
        return
    record_job_outcome(
        status=status,
        records=records,
        quarantined=quarantined,
        reconcile_ok=reconcile_ok,
    )


def record_cdc_poll(
    *,
    lag_seconds: float | None = None,
    lag_bytes: int | None = None,
    lag_basis: str | None = None,
    used_query_fallback: bool = False,
    schedule_id: str = "",
    stream: str = "",
    job_id: str = "",
) -> None:
    _inc("dataflow_cdc_polls_total")
    if used_query_fallback:
        _inc("dataflow_cdc_fallback_query_total")
    key = _label_key(schedule_id=schedule_id, stream=stream, job_id=job_id)
    with _lock:
        _labeled_counters.setdefault("dataflow_pipeline_cdc_polls_total", {})
        _labeled_counters["dataflow_pipeline_cdc_polls_total"][key] = (
            float(_labeled_counters["dataflow_pipeline_cdc_polls_total"].get(key, 0.0)) + 1.0
        )
        _pipeline_heartbeat[key] = time.time()
        # Only stamp second-lag gauge when proven (not heartbeat invent).
        if lag_seconds is not None and lag_seconds >= 0:
            _gauges["dataflow_cdc_lag_seconds"] = float(lag_seconds)
            _labeled_gauges.setdefault("dataflow_pipeline_lag_seconds", {})
            _labeled_gauges["dataflow_pipeline_lag_seconds"][key] = float(lag_seconds)
        elif lag_seconds is None:
            # Clear stale "0 from heartbeat" invent — unknown until proven.
            _labeled_gauges.setdefault("dataflow_pipeline_lag_seconds", {})
            _labeled_gauges["dataflow_pipeline_lag_seconds"].pop(key, None)
        if lag_bytes is not None and int(lag_bytes) >= 0:
            _gauges["dataflow_cdc_lag_bytes"] = float(int(lag_bytes))
            _labeled_gauges.setdefault("dataflow_pipeline_lag_bytes", {})
            _labeled_gauges["dataflow_pipeline_lag_bytes"][key] = float(int(lag_bytes))
        if lag_basis:
            _labeled_gauges.setdefault("dataflow_pipeline_lag_basis_code", {})
            # Encode basis as small int for scrape; UI uses job fields.
            _basis_code = {"commit_ts": 1, "wal_bytes": 2, "unknown": 0}.get(
                str(lag_basis), 0
            )
            _labeled_gauges["dataflow_pipeline_lag_basis_code"][key] = float(_basis_code)


def set_running_jobs(count: int) -> None:
    _set_gauge("dataflow_jobs_running", float(max(0, count)))


def snapshot() -> dict[str, Any]:
    with _lock:
        return {
            "counters": dict(_counters),
            "gauges": dict(_gauges),
            "pipeline_lag_seconds": dict(_labeled_gauges.get("dataflow_pipeline_lag_seconds", {})),
            "pipeline_lag_bytes": dict(_labeled_gauges.get("dataflow_pipeline_lag_bytes", {})),
            "pipeline_polls_total": dict(_labeled_counters.get("dataflow_pipeline_cdc_polls_total", {})),
            "pipeline_heartbeat_at": dict(_pipeline_heartbeat),
            "scraped_at": time.time(),
        }


def freshness_summary(
    *,
    max_lag_warn_seconds: float = 60.0,
    max_lag_critical_seconds: float | None = None,
    heartbeat_stale_seconds: float = 300.0,
) -> dict[str, Any]:
    """UI-friendly freshness view: worst lag, per-pipeline rows, SLO alerts.

    Heartbeat age proves liveness only — never catch-up. WAL/binlog byte lag
    and proven commit-timestamp lag drive SLO (see ``cdc_lag_honesty``).
    """
    from services.cdc_lag_honesty import (
        BYTE_CRITICAL,
        BYTE_WARN,
        merge_freshness_severity,
        observe_cdc_lag,
        severity_from_byte_lag,
    )

    snap = snapshot()
    critical_floor = float(
        max_lag_critical_seconds
        if max_lag_critical_seconds is not None
        else max(max_lag_warn_seconds * 5.0, max_lag_warn_seconds + 60.0)
    )
    now = time.time()
    pipelines: list[dict[str, Any]] = []
    worst: float | None = None
    keys = set(snap.get("pipeline_heartbeat_at") or {})
    keys |= set(snap.get("pipeline_lag_seconds") or {})
    keys |= set(snap.get("pipeline_lag_bytes") or {})

    for key in keys:
        parts = dict(p.split("=", 1) for p in key.split(",") if "=" in p)
        lag_raw = (snap.get("pipeline_lag_seconds") or {}).get(key)
        bytes_raw = (snap.get("pipeline_lag_bytes") or {}).get(key)
        lag_f = float(lag_raw) if lag_raw is not None else None
        byte_f = int(float(bytes_raw)) if bytes_raw is not None else None
        hb = (snap.get("pipeline_heartbeat_at") or {}).get(key)
        hb_age = (now - float(hb)) if hb is not None else None
        heartbeat_stale = hb_age is not None and hb_age > float(heartbeat_stale_seconds)

        # Re-derive severity — never treat missing seconds + fresh heartbeat as ok.
        obs = observe_cdc_lag(
            last_event_commit_at=None,
            last_heartbeat_at=None,
            replication_lag_bytes=byte_f,
            max_lag_warn_seconds=max_lag_warn_seconds,
            max_lag_critical_seconds=critical_floor,
        )
        # If we have a stamped second lag, fold it in.
        if lag_f is not None:
            if lag_f > critical_floor:
                sec_sev = "critical"
            elif lag_f > max_lag_warn_seconds:
                sec_sev = "warn"
            else:
                sec_sev = "ok"
            severity = merge_freshness_severity(obs.get("freshness_severity"), sec_sev)
            if worst is None or lag_f > worst:
                worst = lag_f
        else:
            severity = str(obs.get("freshness_severity") or "unknown")
            byte_sev = severity_from_byte_lag(byte_f)
            if byte_sev:
                severity = merge_freshness_severity(severity, byte_sev)

        if heartbeat_stale:
            severity = merge_freshness_severity(severity, "critical")

        stale = severity in {"warn", "critical"}
        pipelines.append({
            "schedule_id": parts.get("schedule_id", "_"),
            "stream": parts.get("stream", "_"),
            "job_id": parts.get("job_id", "_"),
            "lag_seconds": lag_f,
            "lag_bytes": byte_f,
            "lag_basis": obs.get("cdc_lag_basis") if lag_f is None else (
                "commit_ts" if lag_f and lag_f > 0 else obs.get("cdc_lag_basis")
            ),
            "polls_total": float((snap.get("pipeline_polls_total") or {}).get(key, 0)),
            "heartbeat_at": hb,
            "heartbeat_age_seconds": hb_age,
            "stale": stale,
            "severity": severity,
            "byte_warn_threshold": BYTE_WARN,
            "byte_critical_threshold": BYTE_CRITICAL,
        })

    def _sort_key(p: dict[str, Any]) -> tuple:
        sev = p["severity"]
        lag = p["lag_seconds"]
        lag_sort = -(lag if lag is not None else -1.0)
        return (0 if sev == "critical" else 1 if sev == "warn" else 2 if sev == "ok" else 3, lag_sort)

    pipelines.sort(key=_sort_key)
    global_lag = (snap.get("gauges") or {}).get("dataflow_cdc_lag_seconds")
    if worst is None and global_lag is not None and float(global_lag) > 0:
        worst = float(global_lag)

    alerts: list[dict[str, Any]] = []
    for p in pipelines:
        if p["severity"] == "ok":
            continue
        sid = p["schedule_id"]
        jid = p["job_id"]
        stream = p["stream"]
        if p["severity"] == "unknown":
            title = "CDC lag unknown"
            detail = (
                f"No proven commit lag or WAL/binlog byte probe on {stream} — "
                "Freshness SLO not met (heartbeat alone is not catch-up)."
            )
        elif (
            p["severity"] == "critical"
            and p.get("heartbeat_age_seconds")
            and p["heartbeat_age_seconds"] > heartbeat_stale_seconds
        ):
            title = "CDC heartbeat stale"
            detail = (
                f"No CDC poll heartbeat for {float(p['heartbeat_age_seconds']):.0f}s "
                f"on {stream} (threshold {heartbeat_stale_seconds:.0f}s)."
            )
        elif p["severity"] == "critical" and (p.get("lag_bytes") or 0) >= BYTE_CRITICAL:
            title = "CDC WAL/binlog lag critical"
            detail = (
                f"Replication lag {int(p['lag_bytes']):,} bytes exceeds critical "
                f"({BYTE_CRITICAL:,} B) on {stream}."
            )
        elif p["severity"] == "critical":
            title = "CDC lag critical"
            detail = (
                f"Lag {float(p['lag_seconds'] or 0):.1f}s exceeds critical SLO "
                f"({critical_floor:.0f}s) on {stream}."
            )
        elif (p.get("lag_bytes") or 0) >= BYTE_WARN:
            title = "CDC WAL/binlog lag above SLO"
            detail = (
                f"Replication lag {int(p['lag_bytes']):,} bytes exceeds warn "
                f"({BYTE_WARN:,} B) on {stream}."
            )
        else:
            title = "CDC lag above SLO"
            detail = (
                f"Lag {float(p['lag_seconds'] or 0):.1f}s exceeds warn SLO "
                f"({max_lag_warn_seconds:.0f}s) on {stream}."
            )
        alerts.append({
            "severity": p["severity"],
            "code": "cdc_freshness",
            "title": title,
            "detail": detail,
            "schedule_id": None if sid in {"", "_"} else sid,
            "job_id": None if jid in {"", "_"} else jid,
            "stream": None if stream in {"", "_"} else stream,
            "lag_seconds": p["lag_seconds"],
            "lag_bytes": p.get("lag_bytes"),
        })

    stale_count = sum(1 for p in pipelines if p["stale"])
    critical_count = sum(1 for p in pipelines if p["severity"] == "critical")
    unknown_count = sum(1 for p in pipelines if p["severity"] == "unknown")
    if critical_count:
        slo_status = "critical"
    elif stale_count:
        slo_status = "warn"
    elif pipelines and unknown_count == len(pipelines):
        slo_status = "unknown"
    elif pipelines and unknown_count:
        # Mixed: any proven ok without warn → warn (unknown is not SLO met).
        slo_status = "warn"
    elif pipelines:
        slo_status = "ok"
    else:
        # No CDC poll samples — not a freshness failure (batch Excel→SQL etc.).
        slo_status = "n_a"

    return {
        "worst_lag_seconds": worst,
        "warn_threshold_seconds": max_lag_warn_seconds,
        "critical_threshold_seconds": critical_floor,
        "byte_warn_threshold": BYTE_WARN,
        "byte_critical_threshold": BYTE_CRITICAL,
        "heartbeat_stale_seconds": heartbeat_stale_seconds,
        "stale_count": stale_count,
        "critical_count": critical_count,
        "unknown_count": unknown_count,
        "slo_status": slo_status,
        "alerts": alerts[:50],
        "pipelines": pipelines[:100],
        "counters": snap.get("counters") or {},
        "gauges": snap.get("gauges") or {},
        "scraped_at": snap.get("scraped_at"),
    }


def prometheus_text() -> str:
    """Render Prometheus exposition format (text/plain; version=0.0.4)."""
    lines: list[str] = [
        "# HELP dataflow_jobs_total Total transfer jobs finalized",
        "# TYPE dataflow_jobs_total counter",
        "# HELP dataflow_cdc_lag_seconds Latest observed CDC lag in seconds",
        "# TYPE dataflow_cdc_lag_seconds gauge",
        "# HELP dataflow_pipeline_lag_seconds Per-pipeline CDC lag in seconds",
        "# TYPE dataflow_pipeline_lag_seconds gauge",
    ]
    with _lock:
        for name, value in sorted(_counters.items()):
            lines.append(f"{name} {value}")
        for name, value in sorted(_gauges.items()):
            lines.append(f"{name} {value}")
        for name, series in sorted(_labeled_gauges.items()):
            for labels, value in sorted(series.items()):
                lines.append(f"{name}{{{labels}}} {value}")
        for name, series in sorted(_labeled_counters.items()):
            for labels, value in sorted(series.items()):
                lines.append(f"{name}{{{labels}}} {value}")
    lines.append("")
    return "\n".join(lines)
