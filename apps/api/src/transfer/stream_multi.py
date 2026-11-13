"""Sequential multi-stream (non-CDC) streaming transfers.

Split out of ``stream.py`` (a god module over its size budget). One selected
object is loaded at a time, each with its own watermark, parents before children
so a foreign key can be carried after the load, and an overwrite drops the
remapped destination rather than the primary one.
"""

from __future__ import annotations

from services.sync_cursor import contract_cursor_field
import logging
from typing import Any, Callable

from services.row_conservation import record_stream_health

from .models import EndpointConfig
from .stream_foreign_keys import (
    carry_foreign_keys_after_load as _carry_foreign_keys_after_load,
    foreign_key_context as _foreign_key_context,
    pop_deferred_single_table_foreign_keys,
    push_deferred_single_table_foreign_keys,
)
from .stream_row_accounting import begin_table_population

try:
    from services.checkpoint_service import Checkpoint, CheckpointService
    from services.error_handling import RetryBudget
except ImportError:  # pragma: no cover - tests with api root on path
    from src.services.checkpoint_service import Checkpoint, CheckpointService
    from src.services.error_handling import RetryBudget

logger = logging.getLogger(__name__)


def _publish_stream_written(job_id: str | None, stream_name: str, rows: int) -> None:
    """One durable line per table.

    The batch callback names an insert. A COPY fast path does not call it, so
    a parent table loaded by COPY never appeared in the event log. This note
    does not move ``records_processed`` — that counter is the running total.
    """
    if not job_id or not stream_name:
        return
    try:
        from services.mongodb_service import get_mongodb_service

        get_mongodb_service().update_job_status(
            job_id,
            "running",
            phase="writing",
            message=f"Wrote {int(rows):,} rows on {stream_name}…",
        )
    except Exception:
        logger.warning("stream write note failed for %s", stream_name, exc_info=True)


def _drop_destination_endpoint(destination: EndpointConfig) -> bool:
    """Clear one remapped destination the same way a single-stream overwrite does.

    Relational tables are emptied in place. Mongo is renamed aside so a failed
    load can restore the collection. Returns ``False`` only when the driver
    cannot clear the object at all.
    """
    from .engine import _drop_destination_table

    return _drop_destination_table(destination)


def run_non_cdc_multi_stream_sequential(
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[..., None] | None = None,
    *,
    sync_mode: str = "full_refresh_append",
    stream_contracts: list[dict] | None = None,
    selected: list[Any] | None = None,
    job_id: str | None = None,
    checkpoint: Checkpoint | None = None,
    checkpoint_service: CheckpointService | None = None,
    retry_budget: RetryBudget | None = None,
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    source_filter: dict[str, Any] | None = None,
    limit: int = 0,
    skip_preflight: bool = False,
    shape_by_stream: dict[str, list[dict]] | None = None,
    approved_shape_hash: str = "",
    workspace_id: str = "",
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Run full/incremental for N streams sequentially (one object at a time).

    Mirrors CDC ``_run_cdc_multi_stream_sequential``: remap source/dest per stream,
    prefer per-stream mappings, aggregate ``streams[]`` health. Overwrite DROP is
    per remapped destination (not once on the primary). A job checkpoint is
    applied only to the stream it names. Any other table reads from the start
    (at-least-once upsert of rows already written).
    """
    from services.sync_cursor import (
        isolate_stream_checkpoint,
        resolve_effective_sync_mode,
        resolve_selected_sync_contracts,
        should_drop_destination_for_sync,
    )

    # Imported here: the single-stream engine lives in ``stream``, which imports
    # this module for its historical export surface. The drop is resolved through
    # that module too, so it stays the one name a caller can substitute.
    from . import stream as stream_module
    from .stream import stream_database_transfer

    selected_list = list(selected or resolve_selected_sync_contracts(stream_contracts))
    if len(selected_list) < 2:
        return stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            on_checkpoint,
            sync_mode=sync_mode,
            stream_contracts=stream_contracts,
            job_id=job_id,
            checkpoint=checkpoint,
            checkpoint_service=checkpoint_service,
            retry_budget=retry_budget,
            backfill_new_fields=backfill_new_fields,
            validation_mode=validation_mode,
            source_filter=source_filter,
            limit=limit,
            skip_preflight=skip_preflight,
        )
    from services.duplicate_policy import (
        DuplicatePolicyError,
        multi_stream_keep_last_refusal,
        route_duplicate_options,
    )

    keep_last_refusal = multi_stream_keep_last_refusal(str(source.format or ""), stream_contracts)
    if keep_last_refusal and route_duplicate_options(
        dict(source.extra or {}), dict(destination.extra or {})
    ).get("in_file_dedupe") == "keep_last":
        raise DuplicatePolicyError(keep_last_refusal)  # never streamed uncollapsed

    # Foreign keys are the one aspect a single-table create cannot carry: the
    # parent must exist first. Measure the source references, load parents
    # before children, and add the constraints once every table has landed —
    # the ALTER then validates the rows we just wrote.
    fk_context = _foreign_key_context(source, [c.name or "" for c in selected_list])
    if fk_context.order:
        by_name = {(c.name or ""): c for c in selected_list}
        selected_list = [by_name[n] for n in fk_context.order if n in by_name] + [
            c for c in selected_list if (c.name or "") not in set(fk_context.order)
        ]

    from services.multi_stream_plan import (
        contract_for_stream,
        endpoint_session_hooks,
        patched_endpoint_extra,
        patches_for_stream,
        review_stream_procedures,
        strip_session_hooks,
    )
    from services.multi_stream_cancel import forget_previous_stream_pins
    from services.multi_stream_targets import bind_endpoint_to_stream
    from services.procedure_destination import ProcedureDestinationError, plan_dest_procedure
    from services.rule_compiler.normalize import fold

    stream_names = [(c.name or "").strip() for c in selected_list]
    last_stream_mappings: list[dict] | None = None
    procedure_refusal = review_stream_procedures(
        source,
        destination,
        stream_contracts,
        stream_names,
        sync_mode=sync_mode,
    )
    if procedure_refusal:
        raise ValueError(procedure_refusal)

    def _steps_for(name: str) -> list[dict] | None:
        if not shape_by_stream:
            return None
        if name in shape_by_stream:
            return list(shape_by_stream[name])
        want = fold(name)
        for key, steps in shape_by_stream.items():
            if fold(key) == want:
                return list(steps)
        return None

    from services.multi_stream_resume import (
        completed_streams,
        forget_all_progress,
        record_stream_completed,
        skip_reason,
    )

    # C5: on a resume, tables an earlier pass of this job finished are not
    # read again. A fresh run starts a new record (a reused job id must not
    # inherit an earlier execution's progress).
    from .resume_state import RESUMED_PASS_ATTR

    if getattr(checkpoint, RESUMED_PASS_ATTR, False):
        finished_before = completed_streams(str(job_id or ""))
    else:
        finished_before = {}
        forget_all_progress(str(job_id or ""))
    last_selected = (selected_list[-1].name or "").strip() if selected_list else ""
    from services.multi_stream_undo import begin_undo_pass

    begin_undo_pass(destination)  # C4: undo facts belong to this pass only
    total_rows = 0
    shaped_out = 0
    ddl_log: list[str] = [
        f"MULTI-STREAM sequential ({len(selected_list)} streams, sync={sync_mode}; "
        "each stream has its own watermark; at-least-once)"
    ]
    if fk_context.order:
        ddl_log.append(
            "FK dependency order: " + " -> ".join(fk_context.order)
            + (
                f" (cycle {', '.join(fk_context.cycle)}: load any order, "
                "recreate FKs with post-load ALTER)"
                if fk_context.cycle
                else ""
            )
        )
    headers: list[str] = list(schema.keys()) if schema else []
    stream_health: list[dict[str, Any]] = []
    last_summary: dict[str, Any] = {}
    remaining_limit = int(limit or 0)

    original_table = getattr(source, "table", None)
    original_collection = getattr(source, "collection", None)
    original_dest_table = getattr(destination, "table", None)
    original_dest_collection = getattr(destination, "collection", None)

    session_before, session_after = endpoint_session_hooks(destination)
    session_plan = None
    if session_before or session_after:
        try:
            session_plan = plan_dest_procedure(destination)
        except ProcedureDestinationError as exc:
            raise ValueError(str(exc)) from exc

    defer_fk = push_deferred_single_table_foreign_keys()
    run_session_after = False
    streams_ok = False
    try:
        if session_plan is not None and session_plan.before_spec is not None:
            from .adapters import _run_dest_procedure_hook

            _run_dest_procedure_hook(destination, session_plan.before_spec)
            ddl_log.append(
                f"SESSION before_write once ({session_plan.before_spec.identifier}) "
                f"— not replayed on each of {len(selected_list)} streams"
            )
        if session_plan is not None and session_plan.after_spec is not None:
            run_session_after = True
        for contract in selected_list:
            if remaining_limit == 0 and limit > 0:
                break
            stream_name = (contract.name or "").strip() or "stream"
            # Copy this table's resume position before the shared ledger is
            # cleared. The clear stops the next table inheriting an offset.
            # The copy is what lets the table that owns the checkpoint seek.
            stream_checkpoint = isolate_stream_checkpoint(checkpoint, stream_name)
            # Rows the copy hands this table (the stream then writes its own count onto it).
            handed_rows = int(getattr(stream_checkpoint, "rows_processed", 0) or 0)
            begin_table_population(checkpoint)
            # Every field naming the object moves to this stream: a field left
            # on the request's name made each stream overwrite the last.
            bind_endpoint_to_stream(
                source, stream_name, table=original_table, collection=original_collection
            )
            if original_dest_table is not None or original_dest_collection is not None:
                bind_endpoint_to_stream(
                    destination,
                    stream_name,
                    table=original_dest_table,
                    collection=original_dest_collection,
                )
            # The previous stream's pre-run pins name its object, not this one.
            forget_previous_stream_pins(destination)

            raw = contract_for_stream(stream_contracts, stream_name)
            single_contracts = [
                {
                    **raw,
                    "name": stream_name,
                    "selected": True,
                    "sync_mode": contract.sync_mode or sync_mode,
                    "cursor_field": contract.cursor_field or contract_cursor_field(raw),
                    "primary_key": contract.primary_key or raw.get("primary_key") or "",
                    "schema_policy": contract.schema_policy or raw.get("schema_policy"),
                    "validation_mode": contract.validation_mode or validation_mode,
                }
            ]
            stream_maps = single_contracts[0].get("mappings")
            use_mappings = (
                stream_maps if isinstance(stream_maps, list) and stream_maps else mappings
            )
            # The FK planner translates key columns through this map; without it
            # a reference would be emitted against a destination column name
            # that the load never wrote.
            context_map = {
                str(m.get("source") or ""): str(m.get("target") or m.get("source") or "")
                for m in use_mappings
                if isinstance(m, dict) and m.get("source")
            }
            fk_context.column_maps[stream_name] = context_map

            prior = finished_before.get(stream_name.lower())
            if prior and stream_name != last_selected:
                # No ledger and no rows of this pass: an earlier pass of the job
                # read and wrote the table (its ledger was judged then). Its rows
                # are named as carried, never as written by this pass, and its
                # reversibility checks travel with it.
                from services.reversibility_enforce import carry_checked

                carry_checked(
                    str(job_id or ""), str(prior.get("table") or stream_name),
                    prior.get("reversibility_checked"),
                )
                stream_health.append({
                    "name": stream_name, "status": "completed", "records_processed": 0,
                    # This pass's (empty) tally holds none of the checkpoint's rows.
                    "resumed_checkpoint_rows": 0,
                    "rows_from_earlier_pass": int(prior.get("rows") or 0),
                    "resumed": skip_reason(prior), "sync_mode": prior.get("sync_mode"),
                })
                ddl_log.append(f"RESUME {stream_name}: {skip_reason(prior)}")
                continue

            # Per-stream overwrite: drop remapped dest (outer engine skip when N>1).
            if should_drop_destination_for_sync(
                request_sync_mode=sync_mode,
                contract_sync_mode=single_contracts[0].get("sync_mode"),
            ):
                stream_module._drop_destination_endpoint(destination)

            status = "completed"
            error: str | None = None
            rows = 0
            summary: dict[str, Any] = {}
            stream_limit = remaining_limit if limit > 0 else 0
            source_patch, dest_patch = patches_for_stream(raw)
            if session_plan is not None:
                dest_patch = strip_session_hooks(dest_patch)
            declared_maps = isinstance(stream_maps, list) and bool(stream_maps)
            try:
                # Empty schema → re-introspect each remapped source table.
                # The CALL, when this stream names one, is on the extra only
                # for this iteration — the next stream does not inherit it.
                with patched_endpoint_extra(source, source_patch), patched_endpoint_extra(
                    destination, dest_patch
                ):
                    stream_sync = resolve_effective_sync_mode(
                        sync_mode, single_contracts[0].get("sync_mode")
                    ).lower()
                    if stream_sync in ("full_refresh_mirror", "mirror", "scd2"):
                        # SCD2/mirror per stream: the endpoints above are already
                        # remapped to this stream's table/collection, so the
                        # staging-merge path runs once per stream like the
                        # full/incremental branches. Recipe SCD2 contracts are
                        # refused upstream (one recipe can't name N streams).
                        # Real per-stream types are introspected here — passing
                        # {} would stamp every staging column TEXT.
                        from .adapters import resolve_connector_config
                        from .connector_capabilities import resolve_driver_type
                        from .adapters_introspect import _introspect_table_schema
                        from .stream_scd2 import stream_scd2_mirror_transfer

                        stream_schema = _introspect_table_schema(
                            resolve_driver_type(source.format),
                            resolve_connector_config(source),
                            stream_name,
                            [
                                str(m.get("source") or "")
                                for m in use_mappings
                                if isinstance(m, dict) and m.get("source")
                            ],
                        )
                        rows, stream_ddl, summary, headers = (
                            stream_scd2_mirror_transfer(
                                source,
                                destination,
                                use_mappings,
                                stream_schema,
                                on_checkpoint,
                                sync_mode=stream_sync,
                                stream_contracts=single_contracts,
                                job_id=job_id,
                                backfill_new_fields=backfill_new_fields,
                                validation_mode=validation_mode,
                                limit=stream_limit,
                                key_columns=(
                                    [single_contracts[0]["primary_key"]]
                                    if single_contracts[0].get("primary_key")
                                    else None
                                ),
                            )
                        )
                    else:
                        rows, stream_ddl, summary, headers = stream_database_transfer(
                            source,
                            destination,
                            use_mappings,
                            {},
                            on_checkpoint,
                            sync_mode=sync_mode,
                            stream_contracts=single_contracts,
                            job_id=job_id,
                            checkpoint=stream_checkpoint,
                            checkpoint_service=checkpoint_service,
                            retry_budget=retry_budget,
                            backfill_new_fields=backfill_new_fields,
                            validation_mode=validation_mode,
                            source_filter=source_filter,
                            limit=stream_limit,
                            skip_preflight=skip_preflight,
                            shape_steps=_steps_for(stream_name),
                            mappings_inherited=not declared_maps,
                            workspace_id=workspace_id,
                        )
                ddl_log.extend(stream_ddl)
                total_rows += rows
                last_summary = summary
                from services.multi_stream_undo import remember_stream_restore_point

                remember_stream_restore_point(destination, stream_name, summary)
                from services.reversibility_enforce import checked_for_table

                record_stream_completed(
                    str(job_id or ""), stream_name, rows=rows,
                    sync_mode=str(summary.get("sync_mode") or sync_mode),
                    table=str((summary or {}).get("table") or ""),
                    reversibility_checked=checked_for_table(
                        str(job_id or ""), str((summary or {}).get("table") or stream_name)
                    ),
                )
                last_stream_mappings = list(use_mappings or [])
                _publish_stream_written(job_id, stream_name, rows)
                shaped_out += int((summary or {}).get("rows_shaped_out") or 0)
                if limit > 0:
                    remaining_limit = max(0, remaining_limit - rows)
            except Exception as exc:
                status = "failed"
                error = str(exc)
                # C4: undo every table this run reached before the failing one,
                # each against its own pre-run facts (the failing stream already
                # undid its own batch).
                from services.multi_stream_undo import note_failed_multi_stream
                from .adapters import resolve_dest_table

                note_failed_multi_stream(
                    exc, destination, sync_mode=sync_mode, job_id=str(job_id or ""),
                    failed_table=str(
                        resolve_dest_table(
                            str(getattr(destination, "format", "") or ""), destination, stream_name
                        ) or ""
                    ),
                )
                record_stream_health(
                    stream_health,
                    name=stream_name,
                    status=status,
                    records_processed=rows,
                    summary=summary,
                    extra={"error": error},
                    sync_mode=sync_mode,
                    source=source,
                    destination=destination,
                    # A stream that ran its own CALL/SELECT read that result,
                    # not the table its name matches (QA C3 per-stream queries).
                    count_source=source_patch is None,
                )
                raise
            record_stream_health(
                stream_health,
                name=stream_name,
                status=status,
                records_processed=rows,
                summary=summary,
                extra={
                    "watermark": summary.get("watermark"),
                    "sync_mode": summary.get("sync_mode")
                    or resolve_effective_sync_mode(
                        sync_mode, single_contracts[0].get("sync_mode")
                    ),
                    "error": error,
                    # Per-stream SCD2/mirror proof surfaces for operators:
                    # key-diff verdicts live on the staging-merge summary.
                    "scd2": summary.get("scd2"),
                    "mirror": summary.get("mirror"),
                    "missing_keys": summary.get("missing_keys"),
                    "extra_keys": summary.get("extra_keys"),
                    # Rows this stream's count holds from the checkpoint it resumed from: 0 when it
                    # was handed none, else what its route recorded (review 6).
                    "resumed_checkpoint_rows": summary.get("resumed_checkpoint_rows")
                    if handed_rows else 0,
                },
                sync_mode=sync_mode,
                source=source,
                destination=destination,
                count_source=source_patch is None,
            )
        streams_ok = True
        # Every table finished: the job has no unfinished pass for a resume to
        # continue, so a later resume reads every table again (C5 skips only
        # tables of an interrupted pass).
        forget_all_progress(str(job_id or ""))
    finally:
        if (
            run_session_after
            and session_plan is not None
            and session_plan.after_spec is not None
        ):
            try:
                from .adapters import _run_dest_procedure_hook

                _run_dest_procedure_hook(destination, session_plan.after_spec)
                ddl_log.append(
                    f"SESSION after_write once ({session_plan.after_spec.identifier}) "
                    f"— not replayed on each of {len(selected_list)} streams"
                )
            except Exception:
                if streams_ok:
                    raise
                logger.exception(
                    "session after_write failed after a stream write failed"
                )
        pop_deferred_single_table_foreign_keys(defer_fk)
        if original_table is not None:
            source.table = original_table
        if original_collection is not None:
            source.collection = original_collection
        if original_dest_table is not None:
            destination.table = original_dest_table
        if original_dest_collection is not None:
            destination.collection = original_dest_collection

    last_summary = dict(last_summary or {})
    last_summary["streams"] = stream_health
    # Job-level Gate-8 checks the last table (reconcile_coverage names it so),
    # so it must use that stream's own mappings; the schema is re-read like the
    # stream's own run (an empty schema), never the first stream's.
    if last_stream_mappings is not None:
        last_summary["last_stream_mappings"] = last_stream_mappings
    last_summary["multi_stream"] = True
    last_summary["multi_stream_mode"] = "sequential"
    if shaped_out:
        # Summed across streams. The last stream's own tally is not the run.
        last_summary["rows_shaped_out"] = shaped_out
    if approved_shape_hash:
        last_summary["shape_recipe_hash"] = approved_shape_hash
    elif shape_by_stream:
        last_summary["shape_by_stream"] = sorted(shape_by_stream)
    fk_summary = _carry_foreign_keys_after_load(destination, fk_context)
    if fk_summary is not None:
        last_summary["foreign_keys"] = fk_summary
        for decision in fk_summary.get("decisions") or []:
            if decision.get("status") in {"carried", "unsupported"} and decision.get(
                "dest_ddl"
            ):
                ddl_log.append(f"{decision['status'].upper()} FK: {decision['dest_ddl']}")
    from services.reconcile_coverage import annotate_last_stream_checksum_note

    annotate_last_stream_checksum_note(last_summary)
    return total_rows, ddl_log, last_summary, headers


def last_stream_reconcile_inputs(
    dest_summary: Any, mappings: list[dict], schema: dict[str, str]
) -> tuple[list[dict], dict[str, str]]:
    """(mappings, source_schema) for the job-level Gate-8 of a multi-stream job.

    That Gate-8 reads the last table, so a job whose streams carry their own
    mappings (or their own query) must be checked with the last stream's
    mappings and a re-read schema — the first stream's made the digest of a
    column only the last table has disagree with itself.
    """
    if isinstance(dest_summary, dict) and dest_summary.get("multi_stream"):
        last = dest_summary.get("last_stream_mappings")
        if isinstance(last, list) and last and last != list(mappings or []):
            return list(last), {}
    return mappings, schema
