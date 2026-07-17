"""Compatibility shim: canonical implementation now lives in services.lineage_telemetry."""
from __future__ import annotations

from services.lineage_telemetry import (
    _emit,
    _now,
    clear_events,
    emit_lineage,
    emit_preflight_completed,
    emit_quarantine,
    emit_reconciliation,
    emit_run_completed,
    emit_run_failed,
    emit_run_started,
    emit_stage_duration,
    get_events,
    to_ndjson,
)

__all__ = ['_now', '_emit', 'emit_run_started', 'emit_preflight_completed', 'emit_stage_duration', 'emit_reconciliation', 'emit_quarantine', 'emit_lineage', 'emit_run_completed', 'emit_run_failed', 'get_events', 'clear_events', 'to_ndjson']
