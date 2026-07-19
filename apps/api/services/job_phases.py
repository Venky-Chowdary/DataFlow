"""Transfer job phase timeline — persisted execution stages."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

PHASE_ORDER = ("preflight", "extract", "transform", "load", "reconcile")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def initial_phases() -> list[dict[str, Any]]:
    return [{"name": name, "status": "pending", "message": ""} for name in PHASE_ORDER]


def _finalize_phase_timing(phase: dict[str, Any], *, ended_at: str | None = None) -> None:
    """Stamp ended_at / elapsed_ms when a phase leaves the active state."""
    end = ended_at or _now()
    started = phase.get("started_at")
    if started and not phase.get("ended_at"):
        phase["ended_at"] = end
        try:
            start_dt = datetime.fromisoformat(str(started).replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
            phase["elapsed_ms"] = max(0, int((end_dt - start_dt).total_seconds() * 1000))
        except (TypeError, ValueError):
            pass


def advance_phase(
    phases: list[dict[str, Any]] | None,
    active: str,
    *,
    status: str = "active",
    message: str = "",
) -> list[dict[str, Any]]:
    """Update phase list: mark prior done, set active phase."""
    now = _now()
    current = [dict(p) for p in (phases or initial_phases())]
    seen_active = False
    for phase in current:
        name = phase.get("name", "")
        if name == active:
            if phase.get("status") != "active" or not phase.get("started_at"):
                phase["started_at"] = now
            phase["status"] = status
            phase["message"] = message
            seen_active = True
        elif not seen_active and phase.get("status") == "active":
            phase["status"] = "done"
            _finalize_phase_timing(phase, ended_at=now)
        elif not seen_active and phase.get("status") == "pending" and name != active:
            pass
    if not seen_active:
        current.append({"name": active, "status": status, "message": message, "started_at": now})
    # Mark earlier phases done when advancing
    order = {n: i for i, n in enumerate(PHASE_ORDER)}
    active_idx = order.get(active, 99)
    for phase in current:
        idx = order.get(phase.get("name", ""), 99)
        if idx < active_idx and phase.get("status") in ("pending", "active"):
            if phase.get("status") == "active":
                _finalize_phase_timing(phase, ended_at=now)
            phase["status"] = "done"
    return current


def complete_phases(
    phases: list[dict[str, Any]] | None,
    *,
    success: bool,
    message: str = "",
) -> list[dict[str, Any]]:
    now = _now()
    current = [dict(p) for p in (phases or initial_phases())]
    for phase in current:
        if phase.get("status") == "active":
            phase["status"] = "done" if success else "failed"
            _finalize_phase_timing(phase, ended_at=now)
            if message:
                phase["message"] = message
        elif phase.get("status") == "pending":
            phase["status"] = "done" if success else "skipped"
    if success:
        for phase in current:
            if phase.get("name") == "reconcile":
                phase["status"] = "done"
                phase["message"] = message or phase.get("message", "")
                if not phase.get("started_at"):
                    phase["started_at"] = now
                _finalize_phase_timing(phase, ended_at=now)
    return current


def phase_from_engine_label(label: str) -> str:
    mapping = {
        "preflight": "preflight",
        "reading": "extract",
        "writing": "load",
        "reconcile": "reconcile",
        "completed": "reconcile",
        "failed": "reconcile",
    }
    return mapping.get(label, "extract")
