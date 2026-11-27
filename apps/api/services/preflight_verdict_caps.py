"""Validate verdict caps applied after every gate ran.

Lives in its own module (Phase F8 size freeze) and is re-exported by
``services.preflight_service`` so every existing import keeps working.
"""

from __future__ import annotations

import logging
from typing import Any

# The preflight service's logger, so log records keep their name.
logger = logging.getLogger("services.preflight_service")


def apply_readiness_honesty_caps(out: dict[str, Any]) -> dict[str, Any]:
    """Cap Validate readiness when G9 uniqueness is sample-only.

    Gate pass-ratio can be 100% while population uniqueness is unproven.
    Fivetran/Airbyte do not call that certified-ready. Execute may still
    proceed; the score must not look like a migration certificate.
    """
    payload = out if isinstance(out, dict) else {}
    reasons: list[str] = []
    for gate in payload.get("gates") or []:
        if not isinstance(gate, dict):
            continue
        gid = str(gate.get("id") or "").lower()
        details = gate.get("details") if isinstance(gate.get("details"), dict) else {}
        coverage = str(details.get("coverage") or "").lower()
        msg = str(gate.get("message") or "").lower()
        if "g9" in gid or "data_integrity" in gid:
            if (
                coverage == "sample"
                or "population uniqueness not proven" in msg
                or "validate sample only" in msg
            ):
                reasons.append("g9_sample_uniqueness")
        if "g5" in gid or "dry_run" in gid:
            if (
                coverage == "sample"
                or details.get("sample_cap") is not None
                or "sample" in msg
                or gid == "g5_dry_run"
            ):
                reasons.append("g5_sample_dry_run")
    if reasons:
        try:
            score = float(payload.get("readiness_score") or 0)
        except (TypeError, ValueError):
            score = 0.0
        payload["readiness_score"] = min(score, 92.0)
        if "g9_sample_uniqueness" in reasons:
            payload["population_uniqueness_proven"] = False
        payload["readiness_cap_reason"] = "+".join(dict.fromkeys(reasons))
    return payload


def _block_decision_on_hard_gate(out: dict[str, Any]) -> dict[str, Any]:
    """A block-status gate can never sit beside ``decision=approve``.

    The proof bundle is stamped before the hosted gates (g3f population fit,
    additive stamp) run; without this a g3f block left "No blocking issues
    detected" on the verdict that Execute and Pilot read (QA MX3-22).
    """
    pb = out.get("proof_bundle")
    td = (pb or {}).get("transfer_decision") if isinstance(pb, dict) else None
    if out.get("passed") is not False or not isinstance(td, dict):
        return out
    if str(td.get("decision") or "").lower() != "approve":
        return out
    blocking = [
        g for g in out.get("gates") or []
        if isinstance(g, dict) and g.get("status") == "block"
    ]
    if not blocking:
        return out
    messages = [str(g.get("message") or g.get("id") or "") for g in blocking]
    logger.warning(
        "preflight verdict demoted approve->block: gate(s) %s blocked after the "
        "proof bundle was stamped",
        [g.get("id") for g in blocking],
    )
    out["proof_bundle"] = {
        **pb,
        "passed": False,
        "transfer_decision": {
            **td,
            "decision": "block",
            "blockers": list(dict.fromkeys([*(td.get("blockers") or []), *messages])),
            "reason": "Blocking issues detected: " + "; ".join(messages[:3]),
        },
    }
    return out
