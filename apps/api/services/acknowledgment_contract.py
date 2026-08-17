"""One owner for operator acknowledgments (PII/compliance, schema drift, FK risk).

An acknowledgment is a governance record, not a checkbox: it only counts when it
carries *who* accepted the risk and *why*, and it only applies to the mapping
revision it was granted for. Both preflight transports (the ad-hoc Studio call
and the persisted-plan call) resolve acknowledgments here so one path cannot
accept an attestation the other would refuse.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

MIN_ACTOR_LEN = 2
MIN_REASON_LEN = 8


class AcknowledgmentRefused(ValueError):
    """Raised when an acknowledgment is claimed without a usable actor/reason."""


@dataclass(frozen=True)
class Acknowledgments:
    """A resolved set of operator attestations for one preflight run."""

    compliance: bool = False
    schema_drift: bool = False
    fk_risk: bool = False
    actor: str = ""
    reason: str = ""

    @property
    def any_claimed(self) -> bool:
        return bool(self.compliance or self.schema_drift or self.fk_risk)

    def as_policies(self, *, mapping_version: int) -> dict[str, Any]:
        """Persistable form, stamped with the revision it was granted for."""
        return {
            "compliance_acknowledged": self.compliance,
            "schema_drift_acknowledged": self.schema_drift,
            "fk_risk_acknowledged": self.fk_risk,
            "acknowledgment_actor": self.actor,
            "acknowledgment_reason": self.reason,
            "acknowledgment_version": int(mapping_version),
            "acknowledgment_at": datetime.now(timezone.utc).isoformat(),
        }


def resolve_acknowledgments(
    *,
    compliance: object = False,
    schema_drift: object = False,
    fk_risk: object = False,
    actor: object = "",
    reason: object = "",
) -> Acknowledgments:
    """Validate a claimed acknowledgment set, or refuse it.

    Nothing claimed is always valid — it simply means the gates stand.
    """
    ack = Acknowledgments(
        compliance=bool(compliance),
        schema_drift=bool(schema_drift),
        fk_risk=bool(fk_risk),
        actor=str(actor or "").strip(),
        reason=str(reason or "").strip(),
    )
    if not ack.any_claimed:
        return ack
    if len(ack.actor) < MIN_ACTOR_LEN:
        raise AcknowledgmentRefused(
            "acknowledgment_actor is required when acknowledging compliance, "
            "schema drift, or FK risk"
        )
    if len(ack.reason) < MIN_REASON_LEN:
        raise AcknowledgmentRefused(
            f"acknowledgment_reason is required (at least {MIN_REASON_LEN} characters)"
        )
    return ack


def acknowledgments_from_policies(
    policies: dict[str, Any] | None,
    *,
    mapping_version: int,
) -> Acknowledgments:
    """Read a previously recorded acknowledgment, honouring its revision scope.

    A record granted against an older mapping revision is deliberately ignored:
    the operator accepted the risk of a specific mapping, and a remap has to be
    attested again rather than inheriting a green from the shape it replaced.
    """
    pol = policies or {}
    recorded = pol.get("acknowledgment_version")
    if recorded is not None and int(recorded) != int(mapping_version):
        return Acknowledgments()
    return Acknowledgments(
        compliance=bool(pol.get("compliance_acknowledged")),
        schema_drift=bool(pol.get("schema_drift_acknowledged")),
        fk_risk=bool(pol.get("fk_risk_acknowledged")),
        actor=str(pol.get("acknowledgment_actor") or "").strip(),
        reason=str(pol.get("acknowledgment_reason") or "").strip(),
    )


def audit_acknowledgments(
    ack: Acknowledgments,
    *,
    resource: str,
    details: dict[str, Any] | None = None,
) -> None:
    """Append one audit event per attestation actually claimed."""
    if not ack.any_claimed:
        return
    from services.audit_log import append_audit_event

    actions = {
        "acknowledge_compliance": ack.compliance,
        "acknowledge_schema_drift": ack.schema_drift,
        "acknowledge_fk_risk": ack.fk_risk,
    }
    # An FK acknowledgment covers declared destination FK metadata only — it is
    # never a proof that the destination population is orphan-free.
    scope = {
        "acknowledge_fk_risk": {
            "coverage": "destination_fk_metadata",
            "population_orphan_proven": False,
        },
    }
    for action, claimed in actions.items():
        if not claimed:
            continue
        append_audit_event(
            action=f"preflight.{action}",
            resource=resource,
            actor=ack.actor,
            details={
                **(details or {}),
                **scope.get(action, {}),
                "reason": ack.reason,
            },
        )
