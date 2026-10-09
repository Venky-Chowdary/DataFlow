"""Destination CALL, INSERT, and session hooks on the streaming writer.

``stream.py`` remains the page loop. This module binds ``plan_dest_procedure``
and ``apply_rows_via_procedure`` to one chunk, and runs before/after hooks
once around the loop rather than once per batch.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .models import EndpointConfig

logger = logging.getLogger(__name__)


def write_procedure_chunk(
    destination: EndpointConfig,
    records: list[dict[str, Any]],
    plan: Any,
) -> tuple[int, str, dict]:
    """One chunk through the destination CALL or INSERT. Hooks are not on ``plan``."""
    from services.procedure_destination import apply_rows_via_procedure

    from .adapters import _dest_procedure_execute

    engine, text, close = _dest_procedure_execute(destination)
    try:
        with engine.begin() as conn:
            written, ddl, summary = apply_rows_via_procedure(
                destination,
                records,
                execute_call=lambda sql, binds: conn.execute(text(sql), binds or {}),
                plan=plan,
            )
    finally:
        close()
    summary = dict(summary or {})
    quarantine = [row for row in (summary.get("quarantine") or []) if isinstance(row, dict)]
    summary["rejected_rows"] = int(summary.get("quarantine_count") or len(quarantine))
    summary["rejected_details"] = quarantine
    summary["load_method"] = "dest_procedure"
    summary["warnings"] = list(ddl or [])[:10]
    return written, "", summary


def open_destination_row_plan(destination: EndpointConfig) -> tuple[Any, Any]:
    """Parse the destination statement. A dialect that cannot run it fails here."""
    from services.procedure_destination import (
        ProcedureDestinationError,
        plan_dest_procedure,
        row_apply_plan_without_hooks,
    )

    try:
        plan = plan_dest_procedure(destination)
    except ProcedureDestinationError as exc:
        raise ValueError(str(exc)) from exc
    return plan, row_apply_plan_without_hooks(plan)


def decline_copy_for_destination_sql(plan: Any) -> None:
    """A CALL, dest DML, or hook is not a table copy. COPY must not skip it."""
    if plan is None:
        return
    from services.copy_fast_path import note_copy_decline

    note_copy_decline("destination procedure, query, or hook is not a table copy")


def adopt_stream_column_map(
    mappings: list[dict],
    columns: list[str],
    *,
    inherited: bool,
) -> tuple[list[dict], str]:
    """Keep a borrowed map only when it names exactly these columns."""
    if not inherited:
        return mappings, ""
    from services.multi_stream_plan import adopt_inherited_mappings

    return adopt_inherited_mappings(mappings, columns)


def runner_for_stream_steps(
    steps: list[dict] | None,
    *,
    columns: list[str],
    table: str,
    effective_sync: str,
    cursor_field: str,
    key_columns: list[str],
) -> Any:
    """Build this stream's recipe runner. ``None`` when the stream has no steps."""
    if not steps:
        return None
    from services.shape_apply import build_shape_runner
    from services.shape_models import ShapeError

    from .engine_shape import _shape_stream_refusal

    try:
        built = build_shape_runner(
            {"steps": list(steps)},
            source_columns=list(columns),
        )
    except ShapeError as exc:
        raise ValueError(f"{table}: {exc}") from exc
    if built is None:
        return None
    refusal = _shape_stream_refusal(
        built,
        effective_sync=effective_sync,
        multi_stream=False,
        cursor_field=cursor_field or "",
        key_columns=key_columns,
    )
    if refusal:
        raise ValueError(f"{table}: {refusal}")
    return built


def run_session_before(destination: EndpointConfig, plan: Any, ddl_log: list[str]) -> bool:
    """Run the before hook once. True when an after hook must run later."""
    if plan is None or plan.before_spec is None:
        return bool(plan is not None and plan.after_spec is not None)
    from .adapters import _run_dest_procedure_hook

    _run_dest_procedure_hook(destination, plan.before_spec)
    ddl_log.append(f"before_write once {plan.before_spec.identifier}")
    return plan.after_spec is not None


def run_session_after(
    destination: EndpointConfig,
    plan: Any,
    ddl_log: list[str],
    *,
    write_ok: bool,
) -> None:
    """Run the after hook once. A failure after a successful write is the job error."""
    if plan is None or plan.after_spec is None:
        return
    from .adapters import _run_dest_procedure_hook

    try:
        _run_dest_procedure_hook(destination, plan.after_spec)
        ddl_log.append(f"after_write once {plan.after_spec.identifier}")
    except Exception:
        if write_ok:
            raise
        logger.exception("destination after_write failed after the stream write failed")


def append_procedure_warnings(ddl_log: list[str], incoming: dict) -> None:
    """Surface the statement the chunk ran. The decline reason lives on the summary."""
    if incoming.get("load_method") != "dest_procedure":
        return
    for line in incoming.get("warnings") or []:
        ddl_log.append(str(line))


class CdcDestinationSessionError(ValueError):
    """CDC refused a destination CALL/INSERT, or a session hook failed.

    This is not a shared-reader outage. The multi-table fallback must not
    treat it as "reader unavailable" and start a second apply.
    """


def refuse_cdc_destination_row_apply(
    destination: Any,
    sync_mode: str,
    contracts: list[Any] | None = None,
) -> None:
    """Fail closed before a CDC reader opens.

    A destination CALL or INSERT is one statement per row, not a table
    identity. Writing the change log with the table upsert would drop the
    statement and still advance the watermark. Hooks are parsed here so a
    file sink or a bad CALL fails before a slot or binlog reader starts.
    A statement named on one stream's contract is the same refusal — it
    must not be ignored while the table upsert runs.
    """
    _refuse_one_cdc_destination(destination, sync_mode)
    if not contracts:
        return
    fmt = ""
    if hasattr(destination, "format"):
        fmt = str(getattr(destination, "format", "") or "")
    elif isinstance(destination, dict):
        fmt = str(destination.get("format") or destination.get("type") or "")
    from services.multi_stream_plan import patches_for_stream

    for raw in contracts:
        _source_patch, dest_patch = patches_for_stream(raw if isinstance(raw, dict) else None)
        if not dest_patch:
            continue
        extra: dict[str, Any] = {}
        live = getattr(destination, "extra", None)
        if isinstance(live, dict):
            extra.update(live)
        extra.update(dest_patch)
        _refuse_one_cdc_destination({"format": fmt, "type": fmt, "extra": extra}, sync_mode)


def _refuse_one_cdc_destination(destination: Any, sync_mode: str) -> None:
    from services.procedure_destination import (
        MODE_HOOKS,
        ProcedureDestinationError,
        assert_dest_procedure_sync_allowed,
        dest_write_mode_of,
        plan_dest_procedure,
    )

    try:
        assert_dest_procedure_sync_allowed(sync_mode or "cdc", destination)
        if dest_write_mode_of(destination) == MODE_HOOKS:
            plan_dest_procedure(destination)
    except ProcedureDestinationError as exc:
        raise CdcDestinationSessionError(str(exc)) from exc


@contextmanager
def cdc_destination_hooks(destination: Any, ddl_log: list[str]) -> Iterator[None]:
    """Run before/after once around CDC apply. Not once per change batch.

    Row-apply was already refused. A before-hook failure does not run the
    after hook. An after-hook failure after a successful apply is the job
    error; after a failed apply it is logged and the apply error stands.
    """
    from services.procedure_destination import ProcedureDestinationError, plan_dest_procedure

    try:
        plan = plan_dest_procedure(destination)
    except ProcedureDestinationError as exc:
        raise CdcDestinationSessionError(str(exc)) from exc
    run_after = False
    write_ok = False
    try:
        try:
            run_after = run_session_before(destination, plan, ddl_log)
        except Exception as exc:
            raise CdcDestinationSessionError(str(exc)) from exc
        yield
        write_ok = True
    finally:
        if run_after:
            try:
                run_session_after(destination, plan, ddl_log, write_ok=write_ok)
            except Exception as exc:
                if write_ok:
                    raise CdcDestinationSessionError(str(exc)) from exc


@contextmanager
def hide_session_hooks(destination: Any) -> Iterator[None]:
    """Clear before/after on the live extra so an inner stream cannot replay them.

    The session plan already captured the statements. A shared TRUNCATE in
    the before hook must not run again on the second table.
    """
    extra = getattr(destination, "extra", None)
    if not isinstance(extra, dict):
        yield
        return
    saved_before = extra.get("dest_procedure_before")
    saved_after = extra.get("dest_procedure_after")
    extra["dest_procedure_before"] = ""
    extra["dest_procedure_after"] = ""
    try:
        yield
    finally:
        if saved_before:
            extra["dest_procedure_before"] = saved_before
        else:
            extra.pop("dest_procedure_before", None)
        if saved_after:
            extra["dest_procedure_after"] = saved_after
        else:
            extra.pop("dest_procedure_after", None)
