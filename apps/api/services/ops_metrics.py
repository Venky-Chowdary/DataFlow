"""In-process Prometheus-compatible ops metrics for DataFlow transfers.

Exposes counters/gauges for job outcomes, quarantine, CDC lag, and reconcile
results without requiring an external APM dependency. Scrapable at ``GET /metrics``.
JSON snapshot + per-pipeline lag at ``GET /ops/freshness``.
"""

from __future__ import annotations

import threading
import time
from typing import Any

_lock = threading.Lock()

_counters: dict[str, float] = {
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
    "dataflow_jobs_running": 0.0,
}

# Labeled series: metric_name -> {label_key -> value}
_labeled_gauges: dict[str, dict[str, float]] = {
    "dataflow_pipeline_lag_seconds": {},
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
        if lag_seconds is not None and lag_seconds >= 0:
            _gauges["dataflow_cdc_lag_seconds"] = float(lag_seconds)
            _labeled_gauges.setdefault("dataflow_pipeline_lag_seconds", {})
            _labeled_gauges["dataflow_pipeline_lag_seconds"][key] = float(lag_seconds)


def set_running_jobs(count: int) -> None:
    _set_gauge("dataflow_jobs_running", float(max(0, count)))


def snapshot() -> dict[str, Any]:
    with _lock:
        return {
            "counters": dict(_counters),
            "gauges": dict(_gauges),
            "pipeline_lag_seconds": dict(_labeled_gauges.get("dataflow_pipeline_lag_seconds", {})),
            "pipeline_polls_total": dict(_labeled_counters.get("dataflow_pipeline_cdc_polls_total", {})),
            "pipeline_heartbeat_at": dict(_pipeline_heartbeat),
            "scraped_at": time.time(),
        }


def freshness_summary(*, max_lag_warn_seconds: float = 60.0) -> dict[str, Any]:
    """UI-friendly freshness view: worst lag, per-pipeline rows, warn flags."""
    snap = snapshot()
    pipelines: list[dict[str, Any]] = []
    worst: float | None = None
    for key, lag in (snap.get("pipeline_lag_seconds") or {}).items():
        parts = dict(p.split("=", 1) for p in key.split(",") if "=" in p)
        lag_f = float(lag)
        if worst is None or lag_f > worst:
            worst = lag_f
        hb = (snap.get("pipeline_heartbeat_at") or {}).get(key)
        pipelines.append({
            "schedule_id": parts.get("schedule_id", "_"),
            "stream": parts.get("stream", "_"),
            "job_id": parts.get("job_id", "_"),
            "lag_seconds": lag_f,
            "polls_total": float((snap.get("pipeline_polls_total") or {}).get(key, 0)),
            "heartbeat_at": hb,
            "stale": lag_f > max_lag_warn_seconds,
        })
    pipelines.sort(key=lambda p: p["lag_seconds"], reverse=True)
    global_lag = float((snap.get("gauges") or {}).get("dataflow_cdc_lag_seconds") or 0)
    if worst is None and global_lag > 0:
        worst = global_lag
    return {
        "worst_lag_seconds": worst,
        "warn_threshold_seconds": max_lag_warn_seconds,
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
