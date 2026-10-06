"""Design-time shape and procedure plan for one Execute.

``UniversalTransferEngine`` stays the owner of the job. This module only
decides which recipe steps and which CALL belong to which selected table,
so a customers step is not applied to orders and one CALL is not replayed
onto every stream.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.transfer.models import TransferResult


def _mongo():
    try:
        from services.mongodb_service import get_mongodb_service
    except ImportError:
        from src.services.mongodb_service import get_mongodb_service
    return get_mongodb_service()


@dataclass
class ExecuteShapePlan:
    design_runner: Any = None
    shape_runner: Any = None
    shape_by_stream: dict[str, list] = field(default_factory=dict)
    approved_shape_hash: str = ""
    failure: TransferResult | None = None


def peek_declared_source(source: Any, contracts: list | None) -> tuple[list, dict, int, list]:
    """Read the primary stream's CALL or SELECT, not the table behind it.

    When that statement exists, its result set is the schema. Table DDL
    must not overlay columns the writer will never see.
    """
    from services.multi_stream_plan import design_source_patch, patched_endpoint_extra
    from services.sync_cursor import resolve_selected_sync_contracts
    from src.transfer.engine import _authoritative_source_schema
    from src.transfer.source_peek import peek_stream_source

    names = [
        (c.name or "").strip()
        for c in resolve_selected_sync_contracts(contracts)
    ]
    patch = design_source_patch(contracts, names)
    with patched_endpoint_extra(source, patch):
        columns, schema, total_rows, sample_rows = peek_stream_source(source)
    if patch:
        schema = dict(schema)
    else:
        schema = _authoritative_source_schema(source, schema, columns)
    return columns, schema, total_rows, sample_rows


def open_execute_shape(request: Any, columns: list[str], *, job_id: str) -> ExecuteShapePlan:
    """Partition a multi-table recipe. A refusal is a failed transfer, not a write."""
    from services.multi_stream_plan import (
        approved_recipe_refusal,
        enabled_recipe_steps,
        full_recipe_hash,
        history_shape_refusal,
        partition_shape_steps,
    )
    from services.shape_models import ShapeError
    from services.sync_cursor import (
        resolve_effective_sync_mode,
        resolve_selected_sync_contracts,
        resolve_sync_contract,
    )
    from src.transfer.engine_shape import _open_shape_runner, _shape_stream_refusal

    plan = ExecuteShapePlan()
    declared = resolve_sync_contract(request.stream_contracts)
    selected = resolve_selected_sync_contracts(request.stream_contracts)
    shape_sync = resolve_effective_sync_mode(
        request.sync_mode,
        declared.sync_mode if declared else None,
    )
    shape_cursor = declared.cursor_field if declared else ""
    shape_keys = (
        declared.primary_key_columns()
        if declared and declared.primary_key
        else []
    )
    recipe_payload = getattr(request, "shape_recipe", None)
    shape_refusal = ""
    if len(selected) > 1 and enabled_recipe_steps(recipe_payload):
        shape_refusal = history_shape_refusal(shape_sync)
        if not shape_refusal:
            plan.shape_by_stream, shape_refusal = partition_shape_steps(
                recipe_payload,
                [c.name or "" for c in selected],
            )
        if not shape_refusal:
            shape_refusal = approved_recipe_refusal(
                recipe_payload,
                str(getattr(request, "approved_shape_recipe_hash", "") or ""),
            )
        if not shape_refusal:
            approved = str(getattr(request, "approved_shape_recipe_hash", "") or "").strip()
            if not approved:
                try:
                    approved = full_recipe_hash(recipe_payload)
                except ShapeError as exc:
                    shape_refusal = str(exc)
                    approved = ""
            plan.approved_shape_hash = approved
            primary_name = (selected[0].name or "").strip()
            primary_steps = plan.shape_by_stream.get(primary_name) or []
            if primary_steps and not shape_refusal:
                try:
                    from services.shape_apply import build_shape_runner

                    plan.design_runner = build_shape_runner(
                        {"steps": primary_steps},
                        source_columns=list(columns),
                    )
                except ShapeError as exc:
                    shape_refusal = f"{primary_name}: {exc}"
                    plan.design_runner = None
            if plan.design_runner is not None and not shape_refusal:
                shape_refusal = _shape_stream_refusal(
                    plan.design_runner,
                    effective_sync=shape_sync,
                    multi_stream=False,
                    cursor_field=shape_cursor,
                    key_columns=shape_keys,
                )
    else:
        plan.shape_runner = _open_shape_runner(request, columns)
        plan.design_runner = plan.shape_runner
        shape_refusal = _shape_stream_refusal(
            plan.shape_runner,
            effective_sync=shape_sync,
            multi_stream=len(selected) > 1,
            cursor_field=shape_cursor,
            key_columns=shape_keys,
        ) or ""
    if not shape_refusal:
        return plan
    remediation = (
        "Name the source table on each transform step, or run one stream."
        if "source table" in shape_refusal
        else (
            "Remove the Shape recipe for this sync mode, or shape "
            "on a full-refresh / incremental-append route."
        )
    )
    _mongo().update_job_status(
        job_id,
        "failed",
        error=shape_refusal,
        phase="failed",
        progress_pct=0,
    )
    plan.failure = TransferResult(
        success=False,
        error=shape_refusal,
        error_details={
            "reason": "shape_route_unsupported",
            "remediation": remediation,
        },
        operation=request.operation,
        job_id=job_id,
    )
    return plan


def procedure_replay_failure(
    request: Any,
    selected: list,
    *,
    job_id: str,
) -> TransferResult | None:
    """Refuse one endpoint CALL replayed onto every selected table. None when allowed."""
    if len(selected) < 2:
        return None
    from services.multi_stream_plan import review_stream_procedures

    refusal = review_stream_procedures(
        request.source,
        request.destination,
        request.stream_contracts,
        [c.name or "" for c in selected],
        sync_mode=request.sync_mode,
    )
    if not refusal:
        return None
    _mongo().update_job_status(
        job_id,
        "failed",
        error=refusal,
        phase="failed",
        progress_pct=0,
    )
    return TransferResult(
        success=False,
        error=refusal,
        error_details={
            "reason": "multi_stream_procedure_refused",
            "remediation": (
                "Put a CALL on each selected stream, or run the "
                "procedure as a single stream."
            ),
        },
        operation=request.operation,
        job_id=job_id,
    )
