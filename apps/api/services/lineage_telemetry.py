"""Lineage and telemetry events for the universal transfer orchestrator.

Emits correlated logs, metrics, traces, and lineage events.  Designed to be
compatible with OpenLineage and OpenTelemetry concepts: jobs, runs, datasets,
source/destination paths, and validation evidence.
"""

from __future__ import annotations

import json
import os
from services.brand_env import getenv_brand
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any

from services.value_serializer import json_default

#: Retained lineage events. Bounded because the API and worker processes are
#: long-lived: an unbounded list grew for the entire process lifetime, one entry
#: per emitted event across every job, and was only ever cleared by an explicit
#: test helper. A ring buffer keeps the recent-history use cases (tests, the
#: lineage export endpoint, operator debugging) while making the footprint flat.
MAX_LINEAGE_EVENTS = int(getenv_brand("MAX_LINEAGE_EVENTS", "5000") or 5000)

#: Deque subclass so existing ``LINEAGE_EVENTS.append`` / iteration / ``len`` /
#: ``.clear()`` call sites keep working; indexing and slicing of a deque covers
#: the read patterns in this module and its tests.
LINEAGE_EVENTS: deque[dict[str, Any]] = deque(maxlen=MAX_LINEAGE_EVENTS)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emit(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    event = {
        "event_type": event_type,
        "event_id": str(uuid.uuid4()),
        "timestamp": _now(),
        "payload": payload,
    }
    # Oldest events fall off the left once the buffer is full.
    LINEAGE_EVENTS.append(event)
    # Mirror onto the active OpenTelemetry span when tracing is on. The in-
    # memory list stays the source of truth for tests and the lineage export;
    # the span event is how the same signal shows up in an APM timeline.
    try:
        from services.tracing import add_span_event

        attrs = {
            k: v for k, v in (payload or {}).items() if not isinstance(v, (dict, list))
        }
        add_span_event(event_type, attrs)
    except Exception:
        pass
    return event


def emit_run_started(
    *,
    run_id: str,
    job_id: str,
    source: dict[str, Any],
    destination: dict[str, Any],
    validation_mode: str = "strict",
    write_semantics: str = "append",
) -> dict[str, Any]:
    return _emit(
        "run_started",
        {
            "run_id": run_id,
            "job_id": job_id,
            "source": source,
            "destination": destination,
            "validation_mode": validation_mode,
            "write_semantics": write_semantics,
        },
    )


def emit_preflight_completed(
    *,
    run_id: str,
    passed: bool,
    readiness_score: float,
    blockers: list[dict[str, Any]] | None = None,
    validation_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _emit(
        "preflight_completed",
        {
            "run_id": run_id,
            "passed": passed,
            "readiness_score": readiness_score,
            "blockers": blockers or [],
            "validation_plan": validation_plan,
        },
    )


def emit_stage_duration(
    *,
    run_id: str,
    stage: str,
    duration_ms: float,
    row_count: int = 0,
    byte_count: int = 0,
) -> dict[str, Any]:
    return _emit(
        "stage_duration",
        {
            "run_id": run_id,
            "stage": stage,
            "duration_ms": duration_ms,
            "row_count": row_count,
            "byte_count": byte_count,
        },
    )


def emit_reconciliation(
    *,
    run_id: str,
    source_count: int,
    target_count: int,
    mismatched_keys: list[Any] | None = None,
    checksum_ok: bool | None = None,
    job_id: str = "",
) -> dict[str, Any]:
    return _emit(
        "reconciliation",
        {
            "run_id": run_id,
            "job_id": job_id,
            "source_count": source_count,
            "target_count": target_count,
            "mismatched_keys": mismatched_keys or [],
            "checksum_ok": checksum_ok,
        },
    )


def emit_quarantine(
    *,
    run_id: str,
    quarantine_count: int,
    reasons: dict[str, int] | None = None,
    job_id: str = "",
) -> dict[str, Any]:
    return _emit(
        "quarantine",
        {
            "run_id": run_id,
            "job_id": job_id,
            "quarantine_count": quarantine_count,
            "reasons": reasons or {},
        },
    )


def emit_lineage(
    *,
    run_id: str,
    source_dataset: str,
    target_dataset: str,
    mappings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return _emit(
        "lineage",
        {
            "run_id": run_id,
            "source": source_dataset,
            "target": target_dataset,
            "mappings": mappings or [],
        },
    )


def emit_run_completed(
    *,
    run_id: str,
    job_id: str,
    records_transferred: int = 0,
    source_summary: dict[str, Any] | None = None,
    destination_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    dest = destination_summary if isinstance(destination_summary, dict) else {}
    payload: dict[str, Any] = {
        "run_id": run_id,
        "job_id": job_id,
        "records_transferred": records_transferred,
        "source_summary": source_summary or {},
        "destination_summary": dest,
    }
    # CDC lag already lives on the job; carry it on the lineage event when the
    # writer stamped a proven basis — never invent seconds from heartbeat age.
    if dest.get("cdc_lag_seconds") is not None:
        payload["cdc_lag_seconds"] = dest.get("cdc_lag_seconds")
    if dest.get("cdc_lag_basis"):
        payload["cdc_lag_basis"] = dest.get("cdc_lag_basis")
    event = _emit("run_completed", payload)
    persist_event_on_job(job_id, event)
    _seal_run_evidence(job_id=job_id, run_id=run_id, records=records_transferred, dest=dest)
    return event


def _seal_run_evidence(
    *, job_id: str, run_id: str, records: int, dest: dict[str, Any]
) -> None:
    """Chain a durable record of the finished run (best effort).

    This ring buffer is bounded and process-local, and the job document is
    mutable, so a run nobody exported a proof pack for otherwise leaves no
    tamper-evident trace that it happened at all.
    """
    try:
        from services.evidence_chain import seal_run_evidence

        recon = dest.get("reconciliation") if isinstance(dest.get("reconciliation"), dict) else None
        seal_run_evidence(
            run_id=run_id,
            job_id=job_id,
            records_transferred=records,
            reconciliation=recon,
        )
    except Exception:
        return


def emit_run_failed(
    *,
    run_id: str,
    job_id: str,
    error: str = "",
    error_details: dict[str, Any] | None = None,
    retriable: bool = False,
) -> dict[str, Any]:
    return _emit(
        "run_failed",
        {
            "run_id": run_id,
            "job_id": job_id,
            "error": error,
            "error_details": error_details or {},
            "retriable": retriable,
        },
    )


MAX_JOB_LINEAGE_EVENTS = 40


def persist_event_on_job(job_id: str, event: dict[str, Any] | None) -> None:
    """Append a bounded lineage event onto the job document for Theater.

    The in-memory ring stays for tests/export. Theater reads ``lineage_events``
    on the job — CDC lag stays on existing job fields, not here.
    """
    jid = str(job_id or "").strip()
    if not jid or not isinstance(event, dict):
        return
    try:
        from services.mongodb_service import get_mongodb_service

        svc = get_mongodb_service()
        job = svc.get_job(jid)
        if not job:
            return
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        slim = {
            k: v
            for k, v in payload.items()
            if k not in {
                "mappings",
                "validation_plan",
                "source_summary",
                "destination_summary",
                "source",
                "destination",
            }
        }
        entry = {
            "event_type": event.get("event_type"),
            "event_id": event.get("event_id"),
            "timestamp": event.get("timestamp"),
            "payload": slim,
        }
        events = [e for e in (job.get("lineage_events") or []) if isinstance(e, dict)]
        events.append(entry)
        svc.update_job_fields(jid, {"lineage_events": events[-MAX_JOB_LINEAGE_EVENTS:]})
    except Exception:
        return


def get_events(run_id: str | None = None) -> list[dict[str, Any]]:
    if run_id is None:
        return list(LINEAGE_EVENTS)
    return [e for e in LINEAGE_EVENTS if e["payload"].get("run_id") == run_id]


def clear_events() -> None:
    LINEAGE_EVENTS.clear()


def to_ndjson() -> str:
    return "\n".join(json.dumps(e, default=json_default) for e in LINEAGE_EVENTS)
