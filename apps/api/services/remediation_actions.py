"""The one registry of remediation actions a person can approve.

Two surfaces propose fixes: Pilot's ``remediate_validation`` tool (Validate
step, before a load) and Parity Studio (after a load, from the parity report).
Both pick from this fixed menu; neither the model nor a surface can invent an
action. Each action names the surface it belongs to, its label, whether it
writes (and so needs a person's confirmation) and whether it is destructive.
"""

from __future__ import annotations

from dataclasses import dataclass

STUDIO = "studio"  # Validate step in Transfer Studio
PARITY = "parity"  # Parity Studio, after the load


@dataclass(frozen=True)
class RemediationAction:
    kind: str
    surface: str
    label: str
    requires_confirm: bool
    destructive: bool = False


REMEDIATION_ACTIONS: dict[str, RemediationAction] = {
    a.kind: a
    for a in (
        RemediationAction("normalize_control_chars", STUDIO, "Normalize control characters…", False),
        RemediationAction("open_bad_data_fix", STUDIO, "Fix bad data…", True),
        RemediationAction(
            "quarantine_and_rerun", STUDIO, "Quarantine bad rows and re-run…", True, destructive=True
        ),
        RemediationAction("review_mappings", STUDIO, "Open Map step to review mappings", False),
        RemediationAction("rerun_preflight", STUDIO, "Re-run Validate", False),
        # Parity Studio: every one is applied only after a person approves it.
        RemediationAction("widen_column", PARITY, "Widen the destination column", True),
        RemediationAction("resync_keys", PARITY, "Re-sync the affected keys by upsert", True),
        RemediationAction("accept", PARITY, "Accept the difference with a note", True),
        RemediationAction("rerun", PARITY, "Re-run the job", True),
    )
}


def kinds_for(surface: str) -> tuple[str, ...]:
    """Action kinds one surface may propose, in menu order."""
    return tuple(k for k, a in REMEDIATION_ACTIONS.items() if a.surface == surface)


def action(kind: str, surface: str) -> RemediationAction | None:
    """The action, or ``None`` when ``kind`` is not on ``surface``'s menu."""
    found = REMEDIATION_ACTIONS.get(str(kind or ""))
    return found if found is not None and found.surface == surface else None
