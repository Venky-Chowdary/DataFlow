"""Create-new destination DDL approval (manual schema-approval policy).

Under ``manual_review`` no DDL runs without the operator's approval: an
existing table's ADD COLUMN waits for the drift/DDL acknowledgement
(``batch_progress.effective_backfill_new_fields``), and a destination table
that does not exist yet waits for the same ``ddl_changes_acknowledged`` before
CREATE TABLE. Validate discloses the CREATE and blocks; Execute refuses.
"""

from __future__ import annotations

from typing import Any

GATE_ID = "g_create_dest_ddl"


def create_ddl_approval_required(
    *,
    schema_policy: str | None,
    table_exists: bool | None,
    acknowledged: bool,
) -> bool:
    """Whether a CREATE TABLE this run would issue still needs approval."""
    from services.schema_drift import normalize_schema_policy

    if table_exists is not False or acknowledged:
        return False
    policy = (schema_policy or "").strip()
    return bool(policy) and normalize_schema_policy(policy) == "manual_review"


def planned_create_disclosure(
    destination_table: str, mappings: list[dict[str, Any]] | None
) -> str:
    cols = []
    for m in mappings or []:
        if not isinstance(m, dict) or not m.get("target"):
            continue
        typ = str(m.get("target_type") or m.get("dest_type") or "").strip()
        cols.append(f"{m['target']} {typ}".strip())
    return f"CREATE TABLE {destination_table} ({', '.join(cols)})"


def create_ddl_gate(
    *,
    schema_policy: str | None,
    table_exists: bool | None,
    can_create: bool,
    acknowledged: bool,
    destination_table: str,
    mappings: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    """Validate's create-new DDL gate, or None when no CREATE is planned."""
    from services.schema_drift import normalize_schema_policy

    policy = (schema_policy or "").strip()
    if table_exists is not False or not can_create or not policy:
        return None
    if normalize_schema_policy(policy) != "manual_review":
        return None
    planned = planned_create_disclosure(destination_table, mappings)
    details = {
        "rule_id": GATE_ID,
        "remediation_kind": "approve_ddl_or_dedupe",
        "planned_ddl": planned,
        "ddl_acknowledged": bool(acknowledged),
        "requires_approval": not acknowledged,
    }
    if acknowledged:
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": f"Approved destination DDL: {planned}",
            "duration_ms": 0,
            "details": {**details, "acknowledged": True},
        }
    return {
        "id": GATE_ID,
        "status": "block",
        "message": (
            f"'{destination_table}' does not exist. Schema policy is manual "
            f"approval, so this run would need to run: {planned} — approve "
            "destination DDL before executing."
        ),
        "duration_ms": 0,
        "details": details,
        "suggested_actions": [
            "Approve the disclosed DDL and re-validate",
            "Create the destination table yourself",
        ],
    }


def refuse_unapproved_create(request: Any, table_exists: bool | None) -> None:
    """Execute's half of the gate: no CREATE TABLE without the approval.

    Only an operator-declared policy gates: ``manual_review`` is also the
    request default, and a default-policy create-new is the golden path.
    """
    declared = bool(getattr(request, "schema_policy_declared", False)) or (
        "schema_policy" in (getattr(request, "model_fields_set", None) or set())
    )
    if not declared:
        return
    if create_ddl_approval_required(
        schema_policy=getattr(request, "schema_policy", None),
        table_exists=table_exists,
        acknowledged=bool(getattr(request, "ddl_changes_acknowledged", False)),
    ):
        table = getattr(getattr(request, "destination", None), "table", "") or ""
        raise ValueError(
            f"Destination table '{table}' does not exist and schema policy is "
            "manual approval: CREATE TABLE was not approved. Re-run Validate, "
            "approve the destination DDL it discloses, then Execute."
        )
