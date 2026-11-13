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
acknowledgement scoped to a mapping revision. It is bound to what the gate
found (review 3, M2): it needs a prior hold, stores the tables, columns and
categories it covers, and an extract that finds more reads ``needs_pii_review``.
It is re-checked when each extract job starts and when the rehearsal settles
(M1), and once lapsed it never comes back.
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


def _columns(found: dict[str, list[str]]) -> list[dict[str, Any]]:
    return [{"column": col, "categories": sorted({str(c) for c in cats or []})} for col, cats in sorted(found.items())]


def table_hold(table: dict[str, Any]) -> list[dict[str, Any]]:
    """The columns (and categories) the PII gate named for a rehearsal table that reads ``needs_pii_review``."""
    if table.get("status") != NEEDS_PII_REVIEW:
        return []
    held = table.get("pii_held")
    if isinstance(held, list) and held:
        return [{"column": str(h.get("column") or ""), "categories": [str(c) for c in h.get("categories") or []]}
                for h in held if isinstance(h, dict) and h.get("column")]
    # A rehearsal stored before the columns were kept on the table: read them from its extract job.
    job_id = str(table.get("job_id") or "")
    if not job_id:
        return []
    from services.parity_state import job_store

    found = sensitive_fields(job_store().get_job(job_id) or {})
    return _columns(found)


def pending_hold(system: dict[str, Any], rehearsals: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
    """What an acknowledgement given now would cover: the latest finished rehearsal's held tables.

    ``None`` unless the latest finished rehearsal of the system *as it is
    defined now* was held by the PII gate. An acknowledgement is of something
    the gate found and a person read, never a blank cheque given in advance
    (review 3, M2).
    """
    from services.exit_inventory import rehearsals_of

    rows = rehearsals if rehearsals is not None else rehearsals_of(system["id"])
    latest = next((r for r in rows if r.get("verdict") != "running"), None)
    if not latest or latest.get("definition_sha256") != definition_sha256(system):
        return None
    tables = [{"table": str(t.get("name") or ""), "columns": table_hold(t)} for t in latest.get("tables") or []
              if t.get("status") == NEEDS_PII_REVIEW]
    tables = [t for t in tables if t["columns"]]
    if not tables:
        return None
    return {"rehearsal_id": latest.get("id"), "n": latest.get("n"), "tables": tables}


def _merged(old: list[dict[str, Any]], new: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Coverage of two acknowledgements of one definition: every (table, column, category) of either."""
    out: dict[str, dict[str, set[str]]] = {}
    for entry in [*old, *new]:
        if not isinstance(entry, dict):
            continue
        columns = out.setdefault(str(entry.get("table") or ""), {})
        for col in entry.get("columns") or []:
            if isinstance(col, dict) and col.get("column"):
                columns.setdefault(str(col["column"]), set()).update(str(c) for c in col.get("categories") or [])
    return [{"table": table, "columns": [{"column": c, "categories": sorted(cats)} for c, cats in sorted(cols.items())]}
            for table, cols in sorted(out.items())]


def acknowledge(system_id: str, *, actor: str, reason: str, expected_version: int | None) -> dict[str, Any]:
    """Record that ``actor`` reviewed the PII the gate held in the system's tables, and why the extract may run.

    Needs a prior hold: the acknowledgement stores exactly the tables, columns
    and categories the gate named (``covers``), and a later extract that finds
    more than that reads ``needs_pii_review`` again.
    """
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
    hold = pending_hold(system)
    if hold is None:
        raise ExitInvalid(
            "There is nothing to acknowledge: the PII gate has not held a rehearsal of this system as it is "
            "defined now. Rehearse it first; the acknowledgement covers the columns the gate names.")
    earlier = current(system)
    covers = _merged(list((earlier or {}).get("covers") or []), hold["tables"])
    record = {"actor": ack.actor, "reason": ack.reason, "at": now_iso(),
              "definition_sha256": definition_sha256(system), "covers": covers,
              "held_by_rehearsal_id": hold["rehearsal_id"]}
    updated = _write(system_id, record, actor=ack.actor, expected_version=expected_version)
    resource = f"exit_system:{system_id}"
    audit_acknowledgments(ack, resource=resource, details={"exit_system_id": system_id})
    audit("exit.system.pii_acknowledged", system=updated, actor=ack.actor,
          details={"reason": ack.reason, "definition_sha256": record["definition_sha256"],
                   "held_by_rehearsal_id": hold["rehearsal_id"],
                   "covers": [{"table": t["table"], "columns": [c["column"] for c in t["columns"]]} for t in covers]})
    return updated


def withdraw(system_id: str, *, actor: str) -> dict[str, Any]:
    """Withdraw the system's PII acknowledgement: extracts that have not started are held again."""
    get_system(system_id)
    updated = _write(system_id, None, actor=actor, expected_version=None)
    audit("exit.system.pii_acknowledgement_withdrawn", system=updated, actor=actor, details={})
    return updated


def lapse(record: Any) -> dict[str, Any] | None:
    """``record`` marked as lapsed for good (the definition it was given for changed)."""
    if not isinstance(record, dict) or record.get("lapsed_at"):
        return record if isinstance(record, dict) else None
    return {**record, "lapsed_at": now_iso()}


def current(system: dict[str, Any]) -> dict[str, Any] | None:
    """The acknowledgement that applies to the system as it is defined now, or ``None``.

    One that lapsed stays lapsed: changing the definition and changing it back
    does not bring it back (``lapsed_at`` is written by the change itself, and
    by any rehearsal that finds the definition different).
    """
    record = system.get(FIELD)
    if not isinstance(record, dict) or record.get("lapsed_at"):
        return None
    if record.get("definition_sha256") != definition_sha256(system):
        return None
    return record


def note_lapse(system: dict[str, Any]) -> None:
    """Persist that the system's stored acknowledgement no longer applies (so it cannot come back)."""
    record = system.get(FIELD)
    if not isinstance(record, dict) or record.get("lapsed_at") or current(system) is not None:
        return
    lapsed = lapse(record)

    def apply(doc: dict[str, Any]) -> None:
        if isinstance(doc.get(FIELD), dict) and doc[FIELD].get("at") == record.get("at"):
            doc[FIELD] = lapsed

    try:
        exit_store.update(SYSTEMS, system["id"], apply)
    except (exit_store.StoreConflict, KeyError):
        return  # the next rehearsal records it
    audit("exit.system.pii_acknowledgement_lapsed", system=system, actor="system:exit-rehearsal",
          details={"acknowledged_at": record.get("at"), "by": record.get("actor")})


def view(system: dict[str, Any]) -> dict[str, Any] | None:
    """``ExitSystem.pii_acknowledgement``: {actor, reason, at, current, covers}."""
    record = system.get(FIELD)
    if not isinstance(record, dict):
        return None
    return {"actor": record.get("actor"), "reason": record.get("reason"), "at": record.get("at"),
            "current": current(system) is not None, "covers": list(record.get("covers") or [])}


def snapshot(system: dict[str, Any]) -> dict[str, Any] | None:
    """What a rehearsal records at start (and its extract jobs carry): {actor, reason, at, covers}."""
    record = current(system)
    return None if record is None else {**{k: record.get(k) for k in ("actor", "reason", "at")},
                                        "covers": list(record.get("covers") or [])}


def acknowledgement_lapsed(rehearsal: dict[str, Any]) -> str | None:
    """Why the acknowledgement a rehearsal snapshotted no longer holds now, or ``None`` while it does.

    Re-checked when each extract job starts and again at settle (review 3,
    M1): the acknowledgement must still be the system's current one - the same
    record, not withdrawn, the definition unchanged.
    """
    taken = rehearsal.get(FIELD)
    if not isinstance(taken, dict):
        return "the rehearsal started without a PII acknowledgement"
    system = exit_store.get(SYSTEMS, str(rehearsal.get("system_id") or ""))
    if not system:
        return "the system was removed"
    record = system.get(FIELD)
    if not isinstance(record, dict):
        return "the PII acknowledgement was withdrawn"
    if record.get("at") != taken.get("at") or record.get("actor") != taken.get("actor"):
        return "the PII acknowledgement was replaced after the rehearsal started"
    if current(system) is None:
        note_lapse(system)
        return "the system's definition changed, so the PII acknowledgement no longer applies"
    return None


def sensitive_fields(job: dict[str, Any]) -> dict[str, list[str]]:
    """{column: [categories]} the job's preflight PII gate found (acknowledged or not)."""
    preflight = job.get("preflight") if isinstance(job.get("preflight"), dict) else {}
    bundle = preflight.get("proof_bundle") if isinstance(preflight.get("proof_bundle"), dict) else {}
    compliance = bundle.get("compliance") if isinstance(bundle.get("compliance"), dict) else {}
    risk = compliance.get("field_risk") if isinstance(compliance.get("field_risk"), dict) else {}
    return {str(f): [str(c) for c in risk.get(f) or []] for f in compliance.get("sensitive_fields") or []}


def beyond_acknowledged(found: dict[str, list[str]], covers: Any, table: str) -> dict[str, list[str]]:
    """The columns / categories in ``found`` that the acknowledgement's ``covers`` do not name for ``table``."""
    covered: dict[str, set[str]] = {}
    for entry in covers or []:
        if isinstance(entry, dict) and str(entry.get("table") or "").lower() == table.lower():
            for col in entry.get("columns") or []:
                if isinstance(col, dict):
                    covered.setdefault(str(col.get("column") or "").lower(), set()).update(
                        str(c) for c in col.get("categories") or [])
    out: dict[str, list[str]] = {}
    for column, categories in found.items():
        known = covered.get(column.lower())
        extra = [c for c in categories if known is None or c not in known]
        if known is None or extra:
            out[column] = extra if known is not None else list(categories)
    return out


def held_by_pii_gate(job: dict[str, Any]) -> dict[str, list[str]] | None:
    """{column: [categories]} when the job's preflight PII gate held it (unacknowledged), else ``None``."""
    preflight = job.get("preflight") if isinstance(job.get("preflight"), dict) else {}
    bundle = preflight.get("proof_bundle") if isinstance(preflight.get("proof_bundle"), dict) else {}
    compliance = bundle.get("compliance") if isinstance(bundle.get("compliance"), dict) else {}
    if not compliance.get("requires_review") or compliance.get("acknowledged"):
        return None
    return sensitive_fields(job)


def held_reason(found: dict[str, list[str]], job_id: str) -> str:
    """The table's reason: the columns and categories, and the action (never a value)."""
    cols = [f"{col} ({', '.join(cats)})" if cats else col for col, cats in list(found.items())[:10]]
    listed = "; ".join(cols) + (f" (+{len(found) - 10} more)" if len(found) > 10 else "")
    return (f"The PII gate held the extract job {job_id}: {listed or 'columns that look like PII'} need PII review. "
            "A person acknowledges PII for this system (who and why are audited and kept in the evidence), "
            "then rehearses it again.")
