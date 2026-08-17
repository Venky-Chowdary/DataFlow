"""Create a pipeline schedule from chat, through the same path Studio uses.

A schedule is a standing instruction to move data unattended, so it is staged
exactly like a transfer: the route is grounded against live schemas, preflight
has to clear, the operator sees a preview naming the cadence and the first run
instant, and nothing is written until Confirm. The mapping the schedule stores is
the mapping preflight approved — not one the runner re-derives later against a
shape nobody looked at.

Refused rather than guessed:

* An unresolvable cadence (see :mod:`schedule_cadence`).
* A row limit — a schedule has no limit field, so honouring "first 100 rows"
  would silently move the whole table on every run.
* A row filter — :class:`PipelineSchedule` persists no filter, so an accepted
  "where status = active" would move every row on every unattended run.
* A rule chat cannot apply, which ``plan_transfer`` already refuses.
* Anything preflight does not clear, with the blocker named.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .query_tools import _tool_result
from .schedule_cadence import CadenceSpec, parse_cadence
from .transfer_tools import (
    _is_execute_cleared,
    _stage_bound_contract,
    _transfer_decision,
    plan_transfer,
)

#: Sync modes whose runs need a watermark column to be incremental at all. A
#: schedule created without one would silently re-read the whole table forever.
_CURSOR_MODES = {"incremental", "incremental_append", "incremental_deduped"}


def _schedule_name(source_table: str, dest_table: str, spec: CadenceSpec) -> str:
    table = source_table or dest_table or "pipeline"
    words = spec.description.split(",")[0].strip() or spec.interval
    return f"{table} — {words}"[:120]


def _first_run(spec: CadenceSpec) -> str:
    """The instant the first run is due, or "" when the preset decides it."""
    if not spec.cron:
        return ""
    from services.cron_schedule import CronError, next_run

    try:
        return next_run(spec.cron, datetime.now(timezone.utc), spec.timezone).isoformat()
    except CronError:
        return ""


def create_schedule(
    source_connector_id: str = "",
    source_connector_name: str = "",
    source_table: str = "",
    dest_connector_id: str = "",
    dest_connector_name: str = "",
    dest_table: str = "",
    sync_mode: str = "",
    schema_policy: str = "manual_review",
    validation_mode: str = "balanced",
    cadence: str = "",
    name: str = "",
    cursor_column: str = "",
    source_timezone: str = "",
    source_read_mode: str = "",
    procedure_call: str = "",
    source_query: str = "",
    procedure_params: Any = None,
    contract_id: str = "",
    require_signed_contract: Any = None,
    source_filter: dict[str, Any] | None = None,
    upsert_key: str = "",
    dedupe_key: str = "",
    rule_questions: list[str] | None = None,
    applied_rules: list[str] | None = None,
    limit: int = 0,
):
    """Stage a pipeline schedule for explicit Confirm. This creates nothing itself."""
    tool = "create_schedule"
    spec = parse_cadence(cadence)
    if not spec.resolved:
        return _tool_result(tool, success=False, error=spec.question)
    if limit:
        return _tool_result(
            tool,
            success=False,
            error=(
                f"A schedule has no row limit, so “{limit} rows” cannot be part of one — "
                "every run would move the whole table instead. Ask me to run the "
                "limited transfer once, or schedule an incremental sync on a "
                "watermark column so each run only moves new rows."
            ),
        )
    if source_filter:
        return _tool_result(
            tool,
            success=False,
            error=(
                "A schedule cannot carry a row filter yet — the runner would move "
                "every row on every run, and reconcile green while doing it. Either "
                "drop the filter from the schedule, or ask me to run this filtered "
                "transfer once."
            ),
        )
    mode = (sync_mode or "").strip()
    if mode in _CURSOR_MODES and not (cursor_column or "").strip():
        return _tool_result(
            tool,
            success=False,
            error=(
                f"An {mode} schedule needs the watermark column it advances on — "
                "without it every run would re-read the whole table. Tell me which "
                "column carries the change time (e.g. “incremental on updated_at”)."
            ),
        )

    planned = plan_transfer(
        source_connector_id=source_connector_id,
        source_connector_name=source_connector_name,
        source_table=source_table,
        dest_connector_id=dest_connector_id,
        dest_connector_name=dest_connector_name,
        dest_table=dest_table,
        sync_mode=sync_mode,
        schema_policy=schema_policy,
        validation_mode=validation_mode,
        source_timezone=source_timezone,
        source_read_mode=source_read_mode,
        procedure_call=procedure_call,
        source_query=source_query,
        procedure_params=procedure_params,
        contract_id=contract_id,
        require_signed_contract=require_signed_contract,
        source_filter=source_filter,
        upsert_key=upsert_key,
        dedupe_key=dedupe_key,
        rule_questions=rule_questions,
        applied_rules=applied_rules,
    )
    if not planned.success:
        return _tool_result(tool, success=False, error=planned.error)

    plan = planned.output or {}
    preflight = plan.get("preflight") or {}
    if not _is_execute_cleared(preflight):
        decision = _transfer_decision(preflight) or (
            "blocked" if not preflight.get("passed") else "review"
        )
        listed = "; ".join(
            f"{b.get('id')}: {b.get('message')}"
            for b in (preflight.get("blockers") or [])[:4]
            if b.get("message")
        )
        return _tool_result(
            tool,
            success=False,
            output={**plan, "action": "plan_transfer"},
            error=(
                f"Preflight is {decision}-grade for this route, so I will not schedule "
                "it — an unattended run would fail the same way every night"
                + (f" — {listed}" if listed else "")
                + (f" (run {preflight.get('run_id')})." if preflight.get("run_id") else ".")
            ),
        )

    engine_mappings = plan.get("engine_mappings") or []
    if not engine_mappings:
        return _tool_result(
            tool,
            success=False,
            error="No column mapping was produced, so there is nothing safe to schedule.",
        )

    try:
        # Same fail-closed bind as a chat-started run: an unsigned or tripped
        # contract must not become a standing unattended instruction.
        bound = _stage_bound_contract(contract_id, require_signed_contract)
    except ValueError as exc:
        return _tool_result(tool, success=False, error=str(exc))

    source = plan["source"]
    destination = plan["destination"]
    resolved_mode = str(plan.get("sync_mode") or "full_refresh_append")
    upsert = str((plan.get("data_rules") or {}).get("upsert_key") or "")
    label_name = (name or "").strip() or _schedule_name(
        source.get("table") or "", destination.get("table") or "", spec
    )
    payload: dict[str, Any] = {
        "name": label_name,
        "source_connector_id": source["connector_id"],
        "source_table": source["table"],
        "dest_connector_id": destination["connector_id"],
        "dest_table": destination["table"],
        "interval": spec.interval,
        "cron": spec.cron,
        "timezone": spec.timezone,
        "sync_mode": resolved_mode,
        "schema_policy": str(plan.get("schema_policy") or "manual_review"),
        "validation_mode": str(plan.get("validation_mode") or "balanced"),
        # The approved mapping travels with the schedule, so an unattended run
        # writes the columns preflight judged — not a later re-derivation.
        "mappings": engine_mappings,
        "stream_contracts": plan.get("stream_contracts") or [],
        "cursor_column": (cursor_column or "").strip(),
        "primary_key": upsert,
        "source_read_mode": source.get("source_read_mode") or "",
        "procedure_call": source.get("procedure_call") or "",
        "source_query": source.get("source_query") or "",
        "procedure_params": source.get("procedure_params") or {},
        "contract_id": str(bound.get("contract_id") or ""),
        "require_signed_contract": bool(bound.get("require_signed_contract", False)),
        "enabled": True,
    }
    preview: dict[str, Any] = {
        "name": label_name,
        "source": f"{source['connector_name']}.{source['table']}",
        "destination": f"{destination['connector_name']}.{destination['table']}",
        "cadence": spec.description,
        "interval": spec.interval,
        "cron": spec.cron or "(preset interval)",
        "timezone": spec.timezone,
        "sync_mode": resolved_mode,
        "mapped_columns": plan.get("mapped_count"),
        "unmapped_source_columns": plan.get("unmapped_source_columns"),
        "preflight_run_id": preflight.get("run_id"),
        "enabled_on_create": True,
    }
    first_run = _first_run(spec)
    if first_run:
        preview["first_run_at"] = first_run
    if spec.timezone_assumed:
        # An unstated zone is a real ambiguity, so it is disclosed rather than
        # buried: 02:00 UTC is not 02:00 where the operator is.
        preview["timezone_note"] = (
            "No timezone was given, so this is UTC. Say e.g. “in Asia/Kolkata” to change it."
        )
    if payload["cursor_column"]:
        preview["cursor_column"] = payload["cursor_column"]
    if upsert:
        preview["upsert_key"] = upsert
    if payload["contract_id"]:
        preview["contract_id"] = payload["contract_id"]
        preview["require_signed_contract"] = payload["require_signed_contract"]

    from .ack_ledger import get_ack_ledger

    ack_id = get_ack_ledger().put(kind="create_schedule", payload=payload, preview=preview)
    return _tool_result(
        tool,
        success=True,
        output={
            "action": "create_schedule",
            "label": (
                f"Schedule {preview['source']} → {preview['destination']} "
                f"({spec.description})"
            ),
            "risk": "mutate",
            "requires_confirm": True,
            "ack_id": ack_id,
            "preview": preview,
            # Every run overwrites the destination, so the standing instruction
            # is destructive even though creating it is not.
            "destructive": resolved_mode == "full_refresh_overwrite",
            "plan": {k: v for k, v in plan.items() if k != "engine_mappings"},
        },
    )
