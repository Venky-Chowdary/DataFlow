"""PII review of a rehearsal's extracts: the transfer PII gate, acknowledged per system.

A rehearsal extract is a real transfer job, so the preflight PII / compliance
gate (``services.preflight_proof_bundle``) holds a table whose columns look
like PII, exactly as it holds any transfer. That gate is never bypassed here:

* an extract the gate held makes its table (and the rehearsal) read
  ``needs_pii_review``, naming the columns and their categories (never a value)
  and the action;
* a person acknowledges PII for the system through the transfer
  acknowledgement contract (:mod:`services.acknowledgment_contract`: a named
  actor and a reason, audited as ``preflight.acknowledge_compliance``), stored on
  the system with when and for which definition (``definition_sha256``);
* the next rehearsal snapshots that acknowledgement, and its extract jobs carry
  it as ``compliance_acknowledged`` + ``acknowledgment_actor`` /
  ``acknowledgment_reason``, so the gate passes them the way it passes an
  acknowledged transfer; the snapshot is part of the rehearsal and its signed
  Exit Evidence Pack.

An acknowledgement applies only while the system keeps the definition it was
given for (another table, key or connection needs a new one), like an
acknowledgement scoped to a mapping revision.
"""

from __future__ import annotations

from typing import Any

from services import exit_store
from services.exit_inventory import (
    ExitConflict,
    ExitForbidden,
    ExitInvalid,
    ExitNotFound,
    audit,
    definition_sha256,
    get_system,
    now_iso,
)
from services.exit_store import SYSTEMS

FIELD = "pii_acknowledgement"
NEEDS_PII_REVIEW = "needs_pii_review"
ACTION = "acknowledge_pii"


def _write(system_id: str, value: dict[str, Any] | None, *, actor: str,
           expected_version: int | None) -> dict[str, Any]:
    def apply(doc: dict[str, Any]) -> None:
        doc[FIELD] = value
        doc["updated_at"], doc["updated_by"] = now_iso(), actor

    try:
        return exit_store.update(SYSTEMS, system_id, apply, expected_version=expected_version)[0]
    except exit_store.StoreConflict as exc:
        raise ExitConflict(f"This system {exc}; reload it and try again.") from exc
    except KeyError as exc:
        raise ExitNotFound("Exit system not found") from exc


def acknowledge(system_id: str, *, actor: str, reason: str, expected_version: int | None) -> dict[str, Any]:
    """Record that ``actor`` reviewed the PII the system's tables hold, and why the extract may run."""
    from services.acknowledgment_contract import (
        AcknowledgmentRefused,
        audit_acknowledgments,
        resolve_acknowledgments,
    )

    if str(actor or "").strip().lower() in {"", "anonymous"}:
        raise ExitForbidden("A PII acknowledgement needs a signed-in person.")
    system = get_system(system_id)
    try:
        ack = resolve_acknowledgments(compliance=True, actor=actor, reason=reason)
    except AcknowledgmentRefused as exc:
        raise ExitInvalid(str(exc)) from exc
    if len(ack.reason) > 500:
        raise ExitInvalid("The acknowledgement reason is longer than 500 characters.")
    record = {"actor": ack.actor, "reason": ack.reason, "at": now_iso(),
              "definition_sha256": definition_sha256(system)}
    updated = _write(system_id, record, actor=ack.actor, expected_version=expected_version)
    resource = f"exit_system:{system_id}"
    audit_acknowledgments(ack, resource=resource, details={"exit_system_id": system_id})
    audit("exit.system.pii_acknowledged", system=updated, actor=ack.actor,
          details={"reason": ack.reason, "definition_sha256": record["definition_sha256"]})
    return updated


def withdraw(system_id: str, *, actor: str) -> dict[str, Any]:
    """Withdraw the system's PII acknowledgement: its next rehearsal's extracts are held again."""
    get_system(system_id)
    updated = _write(system_id, None, actor=actor, expected_version=None)
    audit("exit.system.pii_acknowledgement_withdrawn", system=updated, actor=actor, details={})
    return updated


def current(system: dict[str, Any]) -> dict[str, Any] | None:
    """The acknowledgement that applies to the system as it is defined now, or ``None``."""
    record = system.get(FIELD)
    if not isinstance(record, dict) or record.get("definition_sha256") != definition_sha256(system):
        return None
    return record


def view(system: dict[str, Any]) -> dict[str, Any] | None:
    """``ExitSystem.pii_acknowledgement``: {actor, reason, at, current}."""
    record = system.get(FIELD)
    if not isinstance(record, dict):
        return None
    return {"actor": record.get("actor"), "reason": record.get("reason"), "at": record.get("at"),
            "current": current(system) is not None}


def snapshot(system: dict[str, Any]) -> dict[str, Any] | None:
    """What a rehearsal records at start (and its extract jobs carry): {actor, reason, at}."""
    record = current(system)
    return None if record is None else {k: record.get(k) for k in ("actor", "reason", "at")}


def held_by_pii_gate(job: dict[str, Any]) -> dict[str, list[str]] | None:
    """{column: [categories]} when the job's preflight PII gate held it (unacknowledged), else ``None``."""
    preflight = job.get("preflight") if isinstance(job.get("preflight"), dict) else {}
    bundle = preflight.get("proof_bundle") if isinstance(preflight.get("proof_bundle"), dict) else {}
    compliance = bundle.get("compliance") if isinstance(bundle.get("compliance"), dict) else {}
    if not compliance.get("requires_review") or compliance.get("acknowledged"):
        return None
    risk = compliance.get("field_risk") if isinstance(compliance.get("field_risk"), dict) else {}
    fields = [str(f) for f in compliance.get("sensitive_fields") or []]
    return {f: [str(c) for c in risk.get(f) or []] for f in fields}


def held_reason(found: dict[str, list[str]], job_id: str) -> str:
    """The table's reason: the columns and categories, and the action (never a value)."""
    cols = [f"{col} ({', '.join(cats)})" if cats else col for col, cats in list(found.items())[:10]]
    listed = "; ".join(cols) + (f" (+{len(found) - 10} more)" if len(found) > 10 else "")
    return (f"The PII gate held the extract job {job_id}: {listed or 'columns that look like PII'} need PII review. "
            "A person acknowledges PII for this system (who and why are audited and kept in the evidence), "
            "then rehearses it again.")
