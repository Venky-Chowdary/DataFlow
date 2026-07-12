"""Lineage and telemetry events for the universal transfer orchestrator.

Emits correlated logs, metrics, traces, and lineage events.  Designed to be
compatible with OpenLineage and OpenTelemetry concepts: jobs, runs, datasets,
source/destination paths, and validation evidence.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

LINEAGE_EVENTS: list[dict[str, Any]] = []


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emit(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    event = {
        "event_type": event_type,
        "event_id": str(uuid.uuid4()),
        "timestamp": _now(),
        "payload": payload,
    }
    LINEAGE_EVENTS.append(event)
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
) -> dict[str, Any]:
    return _emit(
        "reconciliation",
        {
            "run_id": run_id,
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
) -> dict[str, Any]:
    return _emit(
        "quarantine",
        {
            "run_id": run_id,
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


def get_events(run_id: str | None = None) -> list[dict[str, Any]]:
    if run_id is None:
        return list(LINEAGE_EVENTS)
    return [e for e in LINEAGE_EVENTS if e["payload"].get("run_id") == run_id]


def clear_events() -> None:
    LINEAGE_EVENTS.clear()


def to_ndjson() -> str:
    return "\n".join(json.dumps(e, default=str) for e in LINEAGE_EVENTS)
