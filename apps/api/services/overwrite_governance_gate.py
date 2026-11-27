"""Validate gate: what a warehouse overwrite keeps of the object's security (NF-1).

An overwrite of an existing Snowflake table, BigQuery table or Elasticsearch
index used to replace the object, losing its grants, policies, tags, streams,
IAM and aliases without a word at Validate. The overwrite now keeps the live
object (Snowflake, Elasticsearch) or re-creates its access control before any
row lands (BigQuery) — :mod:`connectors.overwrite_set_aside`. This gate
discloses, per engine, what is kept and what is not, and blocks until the
operator approves the destination DDL when a loss is measured:

- BigQuery: the connector cannot read the table's IAM policy or its row
  access policies (or their grantees), so it cannot re-create them.
- Any engine: the measurement itself could not run (unmeasured never passes
  a governed overwrite silently).

Execute repeats the measurement on the live object
(``retire_set_aside``) and refuses without the same approval.
"""

from __future__ import annotations

from typing import Any

GATE_ID = "g_overwrite_governance"
PROBE_SECONDS = 8.0


def _is_overwrite(sync_mode: str | None) -> bool:
    from services.sync_cursor import is_overwrite_sync

    return bool(is_overwrite_sync(sync_mode))


def probe_bigquery_governance(cfg: dict[str, Any], table: str) -> dict[str, Any]:
    """``{"ran", "governed", "unreadable", "message"}`` for one BigQuery table."""
    from connectors.overwrite_governance import bigquery_governance
    from connectors.overwrite_set_aside import _BigQuery, bigquery_skips_governance

    if bigquery_skips_governance(cfg):
        return {"ran": True, "governed": False, "unreadable": [], "message": "emulator: no IAM"}
    client, project, dataset, call = _BigQuery._open(cfg, cfg.get("schema"))
    try:
        gov = bigquery_governance(client, f"{project}.{dataset}.{table}", call)
    finally:
        from connectors.overwrite_set_aside import _close_quietly

        _close_quietly(client)
    return {
        "ran": True,
        "governed": gov.governed,
        "unreadable": list(gov.unreadable),
        "message": "",
    }


def probe_snowflake_governance(cfg: dict[str, Any], table: str) -> dict[str, Any]:
    """``{"ran", "governed", "unreadable", "message"}`` for one Snowflake table.

    The same reads the overwrite makes before it changes anything
    (``_Snowflake.retire``): grants, masking / row access policies and tags.
    A TEMPORARY table is reported, since its backup could not outlive the run.
    """
    from connectors.overwrite_set_aside import _close_quietly, _Snowflake, _snowflake_connection

    sf = _Snowflake()
    if sf._emulator(None, cfg):
        # fakesnow has no grants, policies or tags: no login spent on reading none.
        return {"ran": True, "governed": False, "unreadable": [], "message": "emulator: no governance"}
    schema = cfg.get("schema") or "PUBLIC"
    conn = _snowflake_connection(cfg, cfg.get("schema"))  # the plan's shared login (D187)
    try:
        with conn.cursor() as cur:
            live = sf._resolve(cur, schema, table)
            if not live:
                return {"ran": True, "governed": False, "unreadable": [], "message": "table absent"}
            if sf._kind(cur, schema, live) == "temporary":
                return {"ran": True, "governed": False, "unreadable": ["a TEMPORARY table (no backup can outlive the run)"], "message": ""}
            gov = sf._governance(cur, sf._ref(schema, live), cfg, emulator=False)
            unrestorable = ""
            if gov.has_policies is not False:
                can, why = sf._can_swap(cur, schema, live)
                if not can:
                    unrestorable = (
                        "a failed run could not be restored (restoring needs ALTER TABLE ... "
                        f"SWAP WITH, which needs OWNERSHIP, and {why})"
                    )
    finally:
        _close_quietly(conn)
    return {
        "ran": True,
        "governed": bool(gov.grants or gov.policies or gov.tag_policies or gov.tags),
        "unreadable": list(gov.unreadable),
        "unrestorable": unrestorable,
        "message": "",
    }


_PROBES = {"bigquery": probe_bigquery_governance, "snowflake": probe_snowflake_governance}


def _bounded_probe(cfg: dict[str, Any], table: str, seconds: float, engine: str = "bigquery") -> dict[str, Any]:
    from src.ai.copilot.tool_deadline import bounded_step

    try:
        run = bounded_step(
            f"reading the access control of destination {table}",
            lambda: _PROBES[engine](dict(cfg or {}), table),
            seconds=seconds,
            conn=cfg,
            role="destination",
        )
        if run.error is not None:
            raise run.error
    except Exception as exc:  # noqa: BLE001 - unmeasured, reported as such
        return {"ran": False, "governed": None, "unreadable": [], "message": f"{exc}"[:300]}
    if run.timed_out:
        return {
            "ran": False, "governed": None, "unreadable": [],
            "message": f"did not finish within {seconds:g}s",
        }
    return run.value


def build_overwrite_governance_gate(
    *,
    sync_mode: str | None,
    db_type: str | None,
    table_exists: bool | None,
    destination_config: dict[str, Any] | None,
    destination_table: str,
    acknowledged: bool,
    probe: Any = None,
    seconds: float = PROBE_SECONDS,
) -> dict[str, Any] | None:
    """The gate, or ``None`` when this run overwrites no existing warehouse object."""
    from connectors.overwrite_governance import describe_overwrite_governance

    if not _is_overwrite(sync_mode) or table_exists is False:
        return None
    disclosure = describe_overwrite_governance(str(db_type or ""))
    if disclosure is None:
        return None
    details: dict[str, Any] = {
        "rule_id": GATE_ID,
        "remediation_kind": "approve_ddl_or_grant_access",
        **{k: v for k, v in disclosure.items() if k != "requires_approval"},
        "ddl_acknowledged": bool(acknowledged),
    }
    kept = ", ".join(disclosure["preserved"])
    engine = disclosure["engine"]
    if engine == "elasticsearch":
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": (
                f"Overwrite keeps the live object of '{destination_table}': {kept} stay in place."
            ),
            "duration_ms": 0,
            "details": {**details, "requires_approval": False},
        }
    measured = (probe or (lambda c, t: _bounded_probe(c, t, seconds, engine)))(
        dict(destination_config or {}), destination_table
    )
    details["measurement"] = measured
    losses: list[str] = []
    what = (
        "the table's grants, masking / row access policies and tags" if engine == "snowflake"
        else "the table's IAM policy and row access policies"
    )
    if not measured.get("ran"):
        losses.append(f"{what} could not be read ({measured.get('message') or 'unmeasured'})")
    else:
        losses.extend(f"{item} cannot be read, so it cannot be re-created" for item in measured.get("unreadable") or [])
    details["measured_losses"] = losses
    details["requires_approval"] = bool(losses) and not acknowledged
    if measured.get("unrestorable"):
        # Approval accepts lost metadata, never a run that could not be undone.
        return {
            "id": GATE_ID,
            "status": "block",
            "message": (
                f"Overwrite of '{destination_table}' is refused: {measured['unrestorable']}. "
                "Run it with the table's owning role, or grant that role to the connector's role."
            ),
            "duration_ms": 0,
            "details": {**details, "requires_approval": False, "unrestorable": measured["unrestorable"]},
            "suggested_actions": ["Use the owning role for this connector and re-validate"],
        }
    if not losses:
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": (
                f"Overwrite keeps the live object of '{destination_table}': {kept} stay in place."
                if engine == "snowflake" else
                f"Overwrite re-creates '{destination_table}' with its {kept} before any row is written. "
                "Not kept: " + "; ".join(disclosure["not_preserved"]) + "."
            ),
            "duration_ms": 0,
            "details": details,
        }
    if acknowledged:
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": (
                f"Approved loss on overwrite of '{destination_table}': " + "; ".join(losses) + "."
            ),
            "duration_ms": 0,
            "details": {**details, "acknowledged": True},
        }
    return {
        "id": GATE_ID,
        "status": "block",
        "message": (
            f"Overwrite of '{destination_table}' would lose access control: " + "; ".join(losses)
            + (
                ". Give the connector's role what SHOW GRANTS, POLICY_REFERENCES and TAG_REFERENCES "
                "need on this table (ownership, or APPLY on its policies and tags)"
                if engine == "snowflake" else
                ". Grant the connector's service account bigquery.tables.getIamPolicy / setIamPolicy "
                "and bigquery.rowAccessPolicies.list / create"
            )
            + ", or approve the destination DDL to overwrite without them."
        ),
        "duration_ms": 0,
        "details": details,
        "suggested_actions": [
            "Grant the connector the IAM and row access policy permissions and re-validate",
            "Approve the disclosed loss (destination DDL approval) and re-validate",
        ],
    }


#: ``destination.extra`` key the engine reads at the overwrite clear.
APPROVAL_KEY = "overwrite_governance_loss_approved"


def stamp_overwrite_governance_approval(request: Any) -> None:
    """Carry Validate's destination DDL approval to the overwrite clear.

    ``retire_set_aside`` refuses an overwrite whose access control it cannot
    carry unless this is set; it is the same ``ddl_changes_acknowledged`` the
    gate above asked for.
    """
    destination = getattr(request, "destination", None)
    if destination is None:
        return
    extra = dict(getattr(destination, "extra", None) or {})
    extra[APPROVAL_KEY] = bool(getattr(request, "ddl_changes_acknowledged", False))
    destination.extra = extra


def apply_destination_ddl_approval(request: Any, table_exists: bool | None) -> None:
    """Execute's one call for Validate's destination DDL approval.

    Refuses a create nobody approved (``refuse_unapproved_create``), then
    carries the approval to the overwrite clear.
    """
    from services.create_ddl_gate import refuse_unapproved_create

    refuse_unapproved_create(request, table_exists)
    stamp_overwrite_governance_approval(request)
