"""Sequential multi-stream (non-CDC) streaming transfers.

Split out of ``stream.py`` (a god module over its size budget). One selected
object is loaded at a time, each with its own watermark, parents before children
so a foreign key can be carried after the load, and an overwrite drops the
remapped destination rather than the primary one.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from services.row_conservation import record_stream_health

from .adapters import resolve_connector_config, resolve_dest_table
from .connector_capabilities import resolve_driver_type
from .models import EndpointConfig
from .stream_foreign_keys import (
    carry_foreign_keys_after_load as _carry_foreign_keys_after_load,
    foreign_key_context as _foreign_key_context,
)
from .stream_row_accounting import begin_table_population

try:
    from services.checkpoint_service import Checkpoint, CheckpointService
    from services.error_handling import RetryBudget
except ImportError:  # pragma: no cover - tests with api root on path
    from src.services.checkpoint_service import Checkpoint, CheckpointService
    from src.services.error_handling import RetryBudget

logger = logging.getLogger(__name__)


def _drop_destination_endpoint(destination: EndpointConfig) -> bool:
    """Drop the remapped destination object (overwrite sync, multi-stream).

    Raises :class:`FullRefreshDropFailed` on a failed drop for the same reason
    as the buffered path: a swallowed failure turns an overwrite into an append
    against a table that still holds the previous generation of rows. Returns
    ``False`` only when the driver cannot drop at all.
    """
    if destination.kind != "database":
        return False

    from connectors.table_manager import TableDropError, drop_table
    from services.error_handling import FullRefreshDropFailed

    try:
        db_type = resolve_driver_type(destination.format)
        cfg = resolve_connector_config(destination)
        table_name = resolve_dest_table(db_type, destination)
        schema = cfg.get("schema")
    except Exception as exc:
        raise FullRefreshDropFailed(
            "unknown", f"could not resolve destination for drop: {exc}"
        ) from exc

    from .adapters import carry_dest_spelling_across_drop

    carry_dest_spelling_across_drop(destination, db_type, cfg, table_name, schema)
    try:
        return drop_table(db_type, cfg, table_name, schema)
    except TableDropError as exc:
        logger.error("Overwrite drop failed for %s: %s", table_name, exc)
        raise FullRefreshDropFailed(table_name, str(exc.cause)) from exc
    except Exception as exc:
        logger.error("Overwrite drop failed for %s: %s", table_name, exc, exc_info=exc)
        raise FullRefreshDropFailed(table_name, str(exc)) from exc


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
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Run full/incremental for N streams sequentially (one object at a time).

    Mirrors CDC ``_run_cdc_multi_stream_sequential``: remap source/dest per stream,
    prefer per-stream mappings, aggregate ``streams[]`` health. Overwrite DROP is
    per remapped destination (not once on the primary). Delivery remains
    **at-least-once** on resume (shared job checkpoint).
    """
    from services.sync_cursor import (
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

    total_rows = 0
    ddl_log: list[str] = [
        f"MULTI-STREAM sequential ({len(selected_list)} streams, sync={sync_mode}; "
        "each stream has its own watermark; at-least-once)"
    ]
    if fk_context.order:
        ddl_log.append(
            "FK dependency order: " + " -> ".join(fk_context.order)
            + (
                f" (cycle, no valid order: {', '.join(fk_context.cycle)})"
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

    try:
        for contract in selected_list:
            if remaining_limit == 0 and limit > 0:
                break
            stream_name = (contract.name or "").strip() or "stream"
            begin_table_population(checkpoint)
            if getattr(source, "format", "") == "mongodb" or original_collection:
                source.collection = stream_name
            else:
                source.table = stream_name
            if original_dest_table is not None or original_dest_collection is not None:
                if getattr(destination, "format", "") == "mongodb" or original_dest_collection:
                    destination.collection = stream_name
                else:
                    destination.table = stream_name

            raw = next(
                (c for c in (stream_contracts or []) if c.get("name") == stream_name),
                {},
            ) or {}
            single_contracts = [
                {
                    **raw,
                    "name": stream_name,
                    "selected": True,
                    "sync_mode": contract.sync_mode or sync_mode,
                    "cursor_field": contract.cursor_field or raw.get("cursor_field") or "",
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
            try:
                # Empty schema → re-introspect each remapped source table.
                rows, stream_ddl, summary, headers = stream_database_transfer(
                    source,
                    destination,
                    use_mappings,
                    {},
                    on_checkpoint,
                    sync_mode=sync_mode,
                    stream_contracts=single_contracts,
                    job_id=job_id,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                    retry_budget=retry_budget,
                    backfill_new_fields=backfill_new_fields,
                    validation_mode=validation_mode,
                    source_filter=source_filter,
                    limit=stream_limit,
                    skip_preflight=skip_preflight,
                )
                ddl_log.extend(stream_ddl)
                total_rows += rows
                last_summary = summary
                if limit > 0:
                    remaining_limit = max(0, remaining_limit - rows)
            except Exception as exc:
                status = "failed"
                error = str(exc)
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
                },
                sync_mode=sync_mode,
                source=source,
                destination=destination,
            )
    finally:
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
    last_summary["multi_stream"] = True
    last_summary["multi_stream_mode"] = "sequential"
    fk_summary = _carry_foreign_keys_after_load(destination, fk_context)
    if fk_summary is not None:
        last_summary["foreign_keys"] = fk_summary
        for decision in fk_summary.get("decisions") or []:
            if decision.get("status") in {"carried", "unsupported"} and decision.get(
                "dest_ddl"
            ):
                ddl_log.append(f"{decision['status'].upper()} FK: {decision['dest_ddl']}")
    return total_rows, ddl_log, last_summary, headers
