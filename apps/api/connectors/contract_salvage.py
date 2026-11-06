"""A signed continue contract governs database-side cell refusals too (QA D214).

The SQL writers hold a bad cell out in two places. The shared type matrix
catches what Python can see (a 3.7 for an INTEGER column) and stamps the
column's signed contract on the holdout. What only the database can see (a
70,000 for a SQL Server TINYINT, an emoji for a 3-byte utf8 column) surfaces
as a data error on the batch INSERT; the writer then retried row by row and
quarantined the refused rows, but only when the job posture was already
``quarantine``. Under ``strict`` / ``maximum`` the batch error was re-raised:
the operator had signed CAST_AND_CONTINUE, and the whole write was refused
with 0 rows.

The rule here is the one the matrix already applies: the column's signed
policy decides, not the job posture alone.

* The row-by-row salvage runs under a ``fail`` posture when the Map carries a
  signed continue-policy contract and no fail-closed one.
* A refused row whose column is under a continue contract is held out with
  that contract's policy. A row whose column the driver did not name is
  attributed to the job's signed continue contract when every signed mapping
  continues. A row whose named column carries no contract keeps the strict
  posture: its detail stays ``fail`` and the writer's final check refuses.
"""

from __future__ import annotations

from typing import Any

SALVAGE_POLICIES: frozenset[str] = frozenset({"quarantine", "coerce_null"})


def _signed_policies(mappings: list[Any] | None) -> list[str]:
    from services.migration_risk_contract import (
        mapping_declares_risk_contract,
        mapping_risk_contract,
    )

    out: list[str] = []
    for m in mappings or []:
        if not mapping_declares_risk_contract(m):
            continue
        contract = mapping_risk_contract(m)
        if contract is None:
            # Present but unverifiable: fail closed, as everywhere else.
            out.append("FAIL_JOB")
            continue
        policy = str(contract.execution_policy or "").strip().upper()
        out.append("CAST_AND_CONTINUE" if policy == "CAST_FAIL_QUARANTINE" else policy)
    return out


def map_signs_continue_only(mappings: list[Any] | None) -> str:
    """The signed continue policy when every signed mapping continues, else ``""``."""
    from services.migration_risk_contract import CONTINUE_POLICIES

    policies = _signed_policies(mappings)
    if not policies or any(p not in CONTINUE_POLICIES for p in policies):
        return ""
    for preferred in ("CAST_AND_CONTINUE", "QUARANTINE_ROW", "TRANSFORM_AND_CONTINUE"):
        if preferred in policies:
            return preferred
    return policies[0]


def salvage_policy(policy: Any, mappings: list[Any] | None) -> str:
    """Writer action for a database data error on a batch.

    ``quarantine`` / ``coerce_null`` are returned unchanged. A ``fail``
    posture salvages row by row (``quarantine``) when the Map signs continue
    contracts only; otherwise the posture stands and the batch error raises.
    """
    selected = str(policy or "").strip().lower()
    if selected in SALVAGE_POLICIES:
        return selected
    if selected == "fail" and map_signs_continue_only(mappings):
        return "quarantine"
    return selected


def contract_row_detail(
    detail: dict[str, Any], mappings: list[Any] | None
) -> dict[str, Any]:
    """Stamp one write-time holdout recorded under a ``fail`` posture with the
    contract that governs it."""
    selected = str(detail.get("policy") or "").strip().lower()
    if selected != "fail" or str(detail.get("execution_policy") or "").strip():
        return detail
    signed = map_signs_continue_only(mappings)
    if not signed:
        return detail
    from connectors.writer_common import contract_policy_for_target

    column = str(detail.get("target") or detail.get("column") or "").strip()
    column_policy, risk_id = (
        contract_policy_for_target(column, mappings) if column not in {"", "*"} else ("", "")
    )
    if column not in {"", "*"} and not column_policy:
        return detail  # a named, unsigned column keeps the strict posture
    out = dict(detail)
    out["policy"] = "quarantine"
    out["execution_policy"] = column_policy or signed
    if risk_id and not out.get("risk_id"):
        out["risk_id"] = risk_id
    return out


#: Holdout actions a signed continue contract can give a whole row.
_ROW_HOLDOUT_ACTIONS: frozenset[str] = frozenset({"quarantine", "skip_row"})
_POSTURE_ABORT_ACTIONS: frozenset[str] = frozenset({"fail", "retry_then_fail"})


def hold_row_under_signed_contract(row_details: list[dict[str, Any]]) -> str:
    """One row's findings: the row holdout a signed contract already decided (D267).

    A row a signed QUARANTINE_ROW / SKIP_ROW / CAST_AND_CONTINUE contract holds
    out is not written at all, so a second cell on that same row failing on an
    unsigned column under the strict job posture is not a partial write — the
    whole row is already out. Aborting there turned the operator's signed
    holdout of row 30 into "abort policy blocks partial write (FAIL_JOB)".

    The unsigned findings ride with the row and are stamped with the governing
    contract (``held_with_row``). Returns the row action (``quarantine`` /
    ``skip_row``), or ``""`` when no signed holdout covers the row or a finding
    carries a fail-closed contract of its own — then the strict posture stands.
    """
    from services.migration_risk_contract import (
        CONTINUE_POLICIES,
        disposition_for_execution_policy,
    )

    governing: dict[str, Any] | None = None
    for d in row_details:
        pol = str(d.get("execution_policy") or "").strip().upper()
        action = str(d.get("policy") or "").strip().lower()
        if pol and pol not in CONTINUE_POLICIES:
            return ""  # a signed (or unverifiable) fail-closed contract decides
        if pol in CONTINUE_POLICIES and action in _ROW_HOLDOUT_ACTIONS:
            if governing is None or action == "quarantine":
                governing = d
    if governing is None:
        return ""
    posture = [
        d
        for d in row_details
        if not str(d.get("execution_policy") or "").strip()
        and str(d.get("policy") or "").strip().lower() in _POSTURE_ABORT_ACTIONS
    ]
    if not posture:
        return ""
    action = str(governing.get("policy") or "quarantine").strip().lower()
    pol = str(governing.get("execution_policy") or "").strip().upper()
    for d in posture:
        d["policy"] = action
        d["execution_policy"] = pol
        d["disposition"] = disposition_for_execution_policy(pol)
        d["held_with_row"] = True
        if governing.get("risk_id"):
            d["risk_id"] = governing["risk_id"]
        if action == "quarantine":
            d["quarantine_required"] = True
        else:
            d["quarantine_required"] = False
    return action
