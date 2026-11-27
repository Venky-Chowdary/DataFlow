"""Plan a schedule's route server-side (QA batch 1, item 6).

A schedule created on the Schedules page carried cadence and endpoints but no
column mappings, so Run now refused ("no persisted mappings") and created no
job; the only way out was to run the whole transfer in Transfer Studio and
click Schedule. The Schedules page now asks for the same plan Transfer Studio
and the Pilot's ``create_schedule`` run — live schemas, the real mapping
pipeline and the real preflight gates (``plan_transfer``) — and the schedule is
saved only with that plan's mappings when the plan clears Execute. A plan that
does not clear is refused with its blockers and fixes: a schedule that cannot
run is never saved armed.
"""

from __future__ import annotations

from typing import Any

#: Modes whose runs advance a watermark column (the plan must bind one).
_CURSOR_MODES = frozenset({"incremental_append", "incremental_deduped", "incremental"})


class SchedulePlanRefused(ValueError):
    """The route's plan does not clear Execute; nothing may be saved armed."""

    def __init__(self, message: str, *, blockers: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.blockers = list(blockers or [])


NO_MAPPINGS_RUN_REFUSAL = (
    "This schedule has no column mappings, so a run cannot start and no job was "
    "created. Choose “Plan mappings” on the schedule (Datawrap maps the columns "
    "and runs Validate for this source and destination; enable the schedule to resume "
    "its cadence), "
    "or open Transfer Studio with this source and destination, map the columns, "
    "run Validate, and Schedule from the Studio footer."
)


def plan_schedule_route(payload: dict[str, Any]) -> dict[str, Any]:
    """The fields a schedule needs to run, from a server-side plan of its route.

    Returns ``mappings``, ``stream_contracts``, the bound cursor and key, and
    the plan's resolved mode/policies. Raises :class:`SchedulePlanRefused` when
    the plan fails or does not clear Execute (the Pilot's bar: passed and
    approve-grade), when it produced no mapping, or when an incremental mode
    has no watermark column.
    """
    from services.sync_cursor import normalize_sync_mode
    from src.ai.copilot.transfer_tools import (
        _is_execute_cleared,
        _transfer_decision,
        plan_transfer,
    )

    key = str(payload.get("primary_key") or "")
    planned = plan_transfer(
        source_connector_id=str(payload.get("source_connector_id") or ""),
        source_table=str(payload.get("source_table") or ""),
        dest_connector_id=str(payload.get("dest_connector_id") or ""),
        dest_table=str(payload.get("dest_table") or ""),
        sync_mode=str(payload.get("sync_mode") or ""),
        schema_policy=str(payload.get("schema_policy") or "manual_review"),
        validation_mode=str(payload.get("validation_mode") or "strict"),
        write_via_staging=bool(payload.get("write_via_staging")),
        source_read_mode=str(payload.get("source_read_mode") or ""),
        procedure_call=str(payload.get("procedure_call") or ""),
        source_query=str(payload.get("source_query") or ""),
        procedure_params=payload.get("procedure_params") or None,
        contract_id=str(payload.get("contract_id") or ""),
        require_signed_contract=payload.get("require_signed_contract"),
        upsert_key=key,
        primary_key=key,
        dedupe_key=str(payload.get("dedupe_key") or ""),
        dedupe_key_equality=str(payload.get("dedupe_key_equality") or ""),
        cursor_column=str(payload.get("cursor_column") or ""),
        cursor_semantics=str(payload.get("cursor_semantics") or ""),
        # Plan where this schedule's runs write (QA review Q5).
        dest_schema=str(payload.get("dest_schema") or ""),
        dest_database=str(payload.get("dest_database") or ""),
    )
    if not planned.success:
        raise SchedulePlanRefused(
            f"Datawrap could not plan this route, so the schedule was not saved: {planned.error}"
        )
    plan = planned.output or {}
    preflight = plan.get("preflight") or {}
    blockers = [b for b in (preflight.get("blockers") or []) if isinstance(b, dict)]
    if not _is_execute_cleared(preflight):
        decision = _transfer_decision(preflight) or (
            "blocked" if not preflight.get("passed") else "review"
        )
        listed = "; ".join(
            f"{b.get('id')}: {b.get('message')}" for b in blockers[:4] if b.get("message")
        )
        fixes = "; ".join(
            fix for fix in (
                str((b.get("details") or {}).get("recommended_fix") or "").strip()
                for b in blockers[:4]
            ) if fix
        )
        raise SchedulePlanRefused(
            f"Validate is {decision}-grade for this route, so the schedule was not "
            "saved — every run would be refused the same way"
            + (f": {listed}" if listed else "")
            + (f". To proceed: {fixes}" if fixes else ".")
            + " Open Transfer Studio with this source and destination to resolve it, "
            "then Schedule from the Studio footer.",
            blockers=blockers,
        )
    mappings = list(plan.get("engine_mappings") or [])
    if not mappings:
        raise SchedulePlanRefused(
            "The plan produced no column mapping, so there is nothing safe to schedule."
        )
    rules = plan.get("data_rules") or {}
    mode = str(plan.get("sync_mode") or payload.get("sync_mode") or "")
    cursor = str(rules.get("cursor_column") or payload.get("cursor_column") or "").strip()
    if normalize_sync_mode(mode, default="") in _CURSOR_MODES and not cursor:
        raise SchedulePlanRefused(
            f"An {mode} schedule needs the watermark column it advances on; choose "
            "the cursor column (the column that carries the change time) and save again."
        )
    out: dict[str, Any] = {
        "mappings": mappings,
        "stream_contracts": list(plan.get("stream_contracts") or []),
        "sync_mode": mode,
        "schema_policy": str(plan.get("schema_policy") or payload.get("schema_policy") or ""),
        "validation_mode": str(
            plan.get("validation_mode") or payload.get("validation_mode") or ""
        ),
        "cursor_column": cursor,
        "cursor_semantics": str(
            rules.get("cursor_semantics") or payload.get("cursor_semantics") or ""
        ).strip().lower(),
    }
    from services.duplicate_policy import DuplicatePolicyError, schedule_dedupe_from_plan

    try:
        # D402: store the plan's dedupe answer, so each run does what the plan said.
        out.update(schedule_dedupe_from_plan(
            plan, in_file_dedupe=str(payload.get("in_file_dedupe") or ""),
            dedupe_key=str(payload.get("dedupe_key") or ""), primary_key=key,
        ))
    except DuplicatePolicyError as exc:
        raise SchedulePlanRefused(str(exc)) from exc
    if plan.get("shape_recipe"):
        out["shape_recipe"] = plan["shape_recipe"]
        out["approved_shape_recipe_hash"] = plan.get("approved_shape_recipe_hash") or ""
    return {k: v for k, v in out.items() if v not in ("", None)}
