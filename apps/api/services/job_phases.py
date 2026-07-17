"""Transfer job phase timeline — persisted execution stages."""

from __future__ import annotations

from datetime import datetime, timezone

PHASE_ORDER = ("preflight", "extract", "transform", "load", "reconcile")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def initial_phases() -> list[dict[str, str]]:
    return [{"name": name, "status": "pending", "message": ""} for name in PHASE_ORDER]


def advance_phase(
    phases: list[dict[str, str]] | None,
    active: str,
    *,
    status: str = "active",
    message: str = "",
) -> list[dict[str, str]]:
    """Update phase list: mark prior done, set active phase."""
    current = [dict(p) for p in (phases or initial_phases())]
    seen_active = False
    for phase in current:
        name = phase.get("name", "")
        if name == active:
            phase["status"] = status
            phase["message"] = message
            seen_active = True
        elif not seen_active and phase.get("status") == "active":
            phase["status"] = "done"
        elif not seen_active and phase.get("status") == "pending" and name != active:
            pass
    if not seen_active:
        current.append({"name": active, "status": status, "message": message})
    # Mark earlier phases done when advancing
    order = {n: i for i, n in enumerate(PHASE_ORDER)}
    active_idx = order.get(active, 99)
    for phase in current:
        idx = order.get(phase.get("name", ""), 99)
        if idx < active_idx and phase.get("status") in ("pending", "active"):
            phase["status"] = "done"
    return current


def complete_phases(
    phases: list[dict[str, str]] | None,
    *,
    success: bool,
    message: str = "",
) -> list[dict[str, str]]:
    current = [dict(p) for p in (phases or initial_phases())]
    for phase in current:
        if phase.get("status") == "active":
            phase["status"] = "done" if success else "failed"
            if message:
                phase["message"] = message
        elif phase.get("status") == "pending":
            phase["status"] = "done" if success else "skipped"
    if success:
        for phase in current:
            if phase.get("name") == "reconcile":
                phase["status"] = "done"
                phase["message"] = message or phase.get("message", "")
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
