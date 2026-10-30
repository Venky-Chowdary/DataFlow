from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from preflight.models import GateStatus
from services.db_type_utils import normalize_dest_kind

logger = logging.getLogger(__name__)

def _cdc_log_capture_gates(
    contracts: list[dict[str, Any]],
    *,
    source_type: str,
    source_config: dict[str, Any] | None,
    source_table: str,
    catalog_primary_key_columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """One g9c gate per selected stream: can the run read the source change log."""
    from services.cdc_log_capture_probe import build_log_capture_gate, probe_log_capture
    from services.sync_cursor import contract_cursor_field
    from services.preflight_cursor_gate import (
        catalog_identity_applies,
        resolved_stream_identity_columns,
    )

    if not source_config:
        return []
    streams = contracts or [{"name": source_table}]
    gates: list[dict[str, Any]] = []
    for contract in streams[:5]:
        table = str(
            (source_table if len(streams) == 1 else "")
            or contract.get("name")
            or contract.get("stream")
            or ""
        ).strip()
        keys = resolved_stream_identity_columns(
            contract,
            catalog_primary_key_columns=(
                catalog_primary_key_columns
                if catalog_identity_applies(contract, source_table, len(streams))
                else None
            ),
            mappings=mappings,
        )
        # wal_level is a server property. A PK that Map has not resolved yet
        # must not skip the probe — Neon (wal_level=replica) then passed
        # preflight and failed later as "Duplicate identity keys" (R3-H1).
        postgresish = str(source_type or "").strip().lower().replace("-", "_") in {
            "postgresql",
            "postgres",
            "citus",
            "cockroach",
            "cockroachdb",
            "greenplum",
            "timescaledb",
            "neon",
        }
        if not table or (not keys and not postgresish):
            continue
        probe = probe_log_capture(
            source_type,
            dict(source_config),
            table=table,
            schema=str(source_config.get("schema") or ""),
            primary_key=",".join(keys),
        )
        gate = build_log_capture_gate(
            probe,
            cursor_field=str(contract.get("poll_cursor_field") or "").strip()
            or contract_cursor_field(contract),
            cursor_semantics=str(
                contract.get("poll_cursor_semantics")
                or contract.get("cursor_semantics")
                or ""
            ),
            primary_key_columns=keys,
            pass_status=GateStatus.PASS.value,
            block_status=GateStatus.BLOCK.value,
        )
        if gate is not None:
            gates.append({**gate, "details": {**gate.get("details", {}), "stream": table}})
    if len(gates) <= 1:
        return gates
    blocked = [g for g in gates if g.get("status") == GateStatus.BLOCK.value]
    chosen = blocked or [g for g in gates if g.get("severity") == "warn"] or gates
    return [
        {
            **chosen[0],
            "message": " ".join(
                f"{g['details'].get('stream')}: {g['message']}" for g in chosen
            ),
            "details": {"streams": [g["details"] for g in gates]},
        }
    ]


def _iter_table_population_for_preflight(
    *,
    source_kind: str,
    source_format: str,
    source_connector_id: str,
    source_config: dict[str, Any] | None,
    source_table: str,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    dest_types: dict[str, str],
    dest_db: str,
    sync_mode: str = "",
    read_scope: Any = None,
    source_filter: dict[str, Any] | None = None,
    source_columns: list[str] | None = None,
    shape_recipe: dict[str, Any] | None = None,
):
    """Same projected table walk Execute uses — or None when it cannot run.

    ``shape_recipe``: the walk feeds the approved recipe, so it projects what
    the recipe reads plus every fit column no step produces — as Execute's
    walk projects its runner's input columns.

    Studio/plan/Pilot used to scan only the preview. A late DECIMAL overflow
    then passed Validate and blocked Execute. Callable extracts, row filters,
    and file sources stay out: those populations are not this walk.
    """
    kind = (source_kind or "").strip().lower()
    if kind not in {"database", "cloud"}:
        return None
    table = str(source_table or "").strip()
    if not table:
        return None
    if not (source_connector_id or source_config):
        return None
    from services.procedure_source import is_callable_source

    if is_callable_source(source_config):
        return None
    cfg = dict(source_config or {})
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    from services.sync_cursor import normalize_sync_mode

    if normalize_sync_mode(sync_mode, default="") == "cdc":
        # CDC writes a changelog, not a table rescan. Walking the table
        # judges the wrong population — preview stays, never exact.
        return None
    try:
        from src.transfer.models import EndpointConfig
        from src.transfer.source_peek import iter_bounded_table_population_rows
    except ImportError:  # pragma: no cover - api root on PYTHONPATH
        from transfer.models import EndpointConfig
        from transfer.source_peek import iter_bounded_table_population_rows

    data = dict(cfg)
    if source_connector_id:
        data["connector_id"] = source_connector_id
    data.setdefault("table", table)
    data.setdefault("collection", table)
    if source_format:
        data.setdefault("format", source_format)
        data.setdefault("type", source_format)
    endpoint = EndpointConfig.from_dict(
        kind if kind in {"database", "cloud"} else "database",
        data,
    )
    if source_columns is None and shape_recipe:
        source_columns = _recipe_walk_columns(
            shape_recipe,
            mappings,
            column_types=column_types,
            dest_types=dest_types,
            dest_db=dest_db,
            source_kind=source_kind,
            source_format=source_format or str(data.get("format") or ""),
        )
    return iter_bounded_table_population_rows(
        endpoint,
        mappings,
        column_types=column_types,
        dest_types=dest_types,
        dest_db=dest_db,
        source_kind=source_kind,
        source_format=source_format or str(data.get("format") or ""),
        read_scope=read_scope,
        source_filter=source_filter
        or (cfg.get("source_filter") if isinstance(cfg.get("source_filter"), dict) else None)
        or (extra.get("source_filter") if isinstance(extra.get("source_filter"), dict) else None),
        source_columns=source_columns,
    )


def _recipe_walk_columns(
    shape_recipe: dict[str, Any],
    mappings: list[dict[str, Any]],
    **target_kw: Any,
) -> list[str] | None:
    """Source columns a recipe-shaped fit walk must read, or None (default)."""
    from services.population_fit_scan import bounded_targets
    from services.shape_models import ShapeError, ShapeRecipe

    try:
        recipe = ShapeRecipe.parse(shape_recipe)
    except ShapeError:
        return None
    if not recipe.enabled_steps:
        return None
    targets, _undecidable, _safe = bounded_targets(
        mappings,
        dest_types=target_kw.get("dest_types"),
        source_types=target_kw.get("column_types"),
        dest_db=target_kw.get("dest_db", ""),
        source_kind=target_kw.get("source_kind", ""),
        source_format=target_kw.get("source_format", ""),
    )
    if not targets:
        return []
    produced: set[str] = set()
    for step in recipe.enabled_steps:
        produced.update(step.writes)
    wanted = list(recipe.read_columns)
    for target in targets:
        name = str(target.source or "")
        if name and name not in produced and name not in wanted:
            wanted.append(name)
    return wanted


def _apply_overwrite_emptied_gate(
    out: dict[str, Any],
    *,
    sync_mode: str,
    destination_table_exists: bool | None,
    live_dest_columns: list[str],
    mappings: list[dict[str, Any]] | None,
    regenerated: list[str],
    schema_policy: str,
    acknowledged: bool,
    dest_kind: str,
    validation_mode: str,
) -> None:
    """Name the destination values an overwrite will empty (DEF-C-025).

    MySQL and PostgreSQL keep a destination column no mapping writes (other
    engines recreate without it), but either way the reload replaces every row
    and the values it holds now are gone. That needs no schema history to see:
    the live table and the mapping already say it. Under review policies it is a decision, otherwise a
    warning — never a silent loss.
    """
    from services.overwrite_keep import overwrite_emptied_columns
    from services.sync_cursor import is_overwrite_sync

    from services.db_type_utils import overwrite_replaces_rows

    if (
        destination_table_exists is not True
        or not is_overwrite_sync(sync_mode)
        or not overwrite_replaces_rows(dest_kind)
    ):
        return
    skip = {str(c).casefold() for c in regenerated}
    if normalize_dest_kind(dest_kind) == "redis":
        from connectors.redis_reader import REDIS_ENVELOPE_COLUMNS

        # The reader's key/type envelope is not stored data: an overwrite
        # rebuilds every key under the prefix, so there is no value to lose.
        skip |= set(REDIS_ENVELOPE_COLUMNS)
    emptied = [
        c for c in overwrite_emptied_columns(live_dest_columns, mappings)
        if c.casefold() not in skip
    ]
    if not emptied:
        return
    shown = ", ".join(emptied[:8]) + (f" (+{len(emptied) - 8} more)" if len(emptied) > 8 else "")
    message = (
        f"Overwrite empties {len(emptied)} destination column(s) no mapping writes "
        f"({shown}): every row is replaced and nothing writes these columns, so "
        "the values they hold now are lost"
    )
    details = {
        "columns": emptied,
        "sync_mode": sync_mode,
        "schema_policy": schema_policy,
        "rule_id": "schema_drift.overwrite_emptied_columns",
    }
    policy = (schema_policy or "manual_review").strip().lower()
    if acknowledged or policy not in {"manual_review", "pause_on_change", "type_locked"}:
        out.setdefault("warnings", []).append(
            {"id": "overwrite_emptied_columns", "message": message, "details": details}
        )
        return
    message += " — map a source column to it, or acknowledge the schema change to empty it"
    details.update({"remediation_kind": "acknowledge_schema_drift", "ack_required": True})
    _append_block_gate(
        out,
        "overwrite_emptied_columns",
        message,
        details,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )


def _apply_empty_source_overwrite_gate(
    out: dict[str, Any],
    *,
    sync_mode: str,
    source_measured_empty: bool,
    destination_table_exists: bool | None,
    destination_config: dict[str, Any] | None,
    destination_table: str,
    acknowledged: bool,
    dest_kind: str,
    validation_mode: str,
) -> None:
    """Refuse an overwrite whose measured-empty source would wipe live rows (D56).

    The load-volume guard only compares against this route's own history, so
    the first DataFlow overwrite of a populated table from an empty source
    replaced every row with nothing and reported success. Zero source rows
    over a destination that holds rows is a decision, made explicitly with
    ``empty_source_overwrite_acknowledged``.
    """
    from services.db_type_utils import overwrite_replaces_rows
    from services.sync_cursor import is_overwrite_sync

    if (
        not source_measured_empty
        or destination_table_exists is not True
        or not is_overwrite_sync(sync_mode)
        or not overwrite_replaces_rows(dest_kind)
        or not destination_config
        or not destination_table
    ):
        return
    from services.dest_precount import count_dialect, precount_table

    try:
        dest_rows = precount_table(
            count_dialect(str(destination_config.get("type") or dest_kind)),
            dict(destination_config),
            destination_table,
        )
    except Exception as exc:  # noqa: BLE001 — an unknown count is reported below
        logger.warning("empty-source overwrite precount failed: %s", exc)
        dest_rows = None
    if dest_rows == 0:
        return
    held = f"{dest_rows:,} row(s)" if isinstance(dest_rows, int) else "rows (count unavailable)"
    details = {
        "rule_id": "overwrite_empty_source",
        "sync_mode": sync_mode,
        "destination_rows": dest_rows,
        "source_rows": 0,
    }
    message = (
        f"The source holds 0 rows and '{destination_table}' holds {held}: this "
        "overwrite would replace them all with nothing"
    )
    if acknowledged:
        out.setdefault("warnings", []).append(
            {"id": "overwrite_empty_source", "message": message, "details": details}
        )
        return
    details.update(
        {"remediation_kind": "acknowledge_empty_source_overwrite", "ack_required": True}
    )
    _append_block_gate(
        out,
        "overwrite_empty_source",
        message
        + ". Check the source (an emptied or wrong table looks exactly like "
        "this), or rerun with empty_source_overwrite_acknowledged=true to "
        "empty the destination on purpose.",
        details,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )


def _append_block_gate(
    out: dict[str, Any],
    gate_id: str,
    message: str,
    details: dict[str, Any],
    *,
    dest_kind: str,
    validation_mode: str,
) -> None:
    gate = {
        "id": gate_id,
        "status": "block",
        "message": message,
        "duration_ms": 0,
        "details": details,
    }
    from services.preflight_rules import enrich_blockers

    out["gates"] = [*out.get("gates", []), gate]
    out["blockers"] = [
        *out.get("blockers", []),
        *enrich_blockers(
            [{"id": gate["id"], "message": message, "details": details}],
            dest_kind=dest_kind,
            validation_mode=validation_mode,
        ),
    ]
    out["passed"] = False
    out["passed_count"] = sum(1 for g in out["gates"] if g.get("status") == "pass")
    out["total_gates"] = len(out["gates"])
    out["readiness_score"] = round(out["passed_count"] / max(out["total_gates"], 1) * 100, 1)


def scan_existing_table_population_fit(
    *,
    source_kind: str,
    source_format: str,
    source_connector_id: str,
    source_config: dict[str, Any] | None,
    source_table: str,
    source_file_id: str | None,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    destination_column_types: dict[str, str],
    destination_db_type: str,
    sync_mode: str,
    read_scope: Any,
    source_filter: dict[str, Any] | None,
    population_rows: Iterable[Mapping[str, Any]] | None,
    rows_are_population: bool,
    sample_rows: list[dict[str, Any]],
    row_count: int | None,
    validation_mode: str,
    destination_table_exists: bool | None,
    shape_recipe: dict[str, Any] | None,
    columns: list[str],
    deadline_monotonic: float | None,
    on_progress: Callable[[int], None] | None,
    iter_table_population: Callable[..., Iterable[Mapping[str, Any]] | None],
    population_replay: Callable[[], Iterable[Mapping[str, Any]] | None] | None = None,
) -> tuple[Any, str, Iterable[Mapping[str, Any]] | None, bool, dict[str, Any]]:
    """Scan an existing table before DDL and gates, so every consumer shares it."""
    from connectors.writer_common import transform_error_policy_for_validation_mode
    from services.population_fit_scan import scan_population_fit
    from services.row_filter import iter_filtered_rows
    from services.sync_cursor import incremental_read_narrows, iter_rows_after_watermark

    cfg_extra = (source_config or {}).get("extra")
    extra_filter = (
        cfg_extra.get("source_filter")
        if isinstance(cfg_extra, dict) and isinstance(cfg_extra.get("source_filter"), dict)
        else None
    )
    cfg_filter = (
        (source_config or {}).get("source_filter")
        if isinstance((source_config or {}).get("source_filter"), dict)
        else None
    )
    resolved_filter = dict(source_filter or cfg_filter or extra_filter or {})
    population_walk_error = ""
    replay_source: Callable[[], Iterable[Mapping[str, Any]] | None] | None = None

    if population_rows is None and str(source_file_id or "").strip():
        from services.file_parser import iter_stored_upload_rows

        stored_rows = iter_stored_upload_rows(source_file_id)
        if stored_rows is not None:
            population_rows = stored_rows
            rows_are_population = True

            def replay_source() -> Iterable[Mapping[str, Any]] | None:
                return iter_stored_upload_rows(source_file_id)

    if replay_source is None and population_rows is not None:
        # A caller that hands in a one-shot population (a file route) also
        # hands in how to read it again, so a create-new carrier can be
        # re-proven (D31).
        replay_source = population_replay

    if population_rows is None:
        try:
            table_rows = iter_table_population(
                source_kind=source_kind,
                source_format=source_format,
                source_connector_id=source_connector_id,
                source_config=source_config,
                source_table=source_table,
                mappings=mappings,
                column_types=column_types,
                dest_types=destination_column_types,
                dest_db=destination_db_type,
                sync_mode=sync_mode,
                read_scope=read_scope if incremental_read_narrows(sync_mode) else None,
                source_filter=resolved_filter or None,
                shape_recipe=shape_recipe or None,
            )
        except Exception as walk_exc:
            logger.warning(
                "table population walk failed for %s; g3f fails closed: %s",
                source_table,
                walk_exc,
            )
            population_walk_error = str(walk_exc)[:400]
            table_rows = None
        if table_rows is not None:
            table_rows = iter(table_rows)
            try:
                first = next(table_rows)
            except StopIteration:
                population_rows = iter(())
                rows_are_population = True
            except Exception as walk_exc:
                logger.warning(
                    "table population walk failed for %s; g3f fails closed: %s",
                    source_table,
                    walk_exc,
                )
                population_walk_error = str(walk_exc)[:400]
            else:
                def chained():
                    yield first
                    yield from table_rows

                walked: Iterable[Mapping[str, Any]] = chained()
                from services.shape_apply import is_typed_value_source

                _typed_source = str(source_kind or "").lower() != "file" and (
                    is_typed_value_source(source_format)
                )
                if shape_recipe:
                    from services.shape_preflight import shaped_population_rows

                    # The recipe reads the walked source rows (their own
                    # columns), under the source's value model — the shaped
                    # image's columns made a rename/derive step unparseable.
                    walked = shaped_population_rows(
                        shape_recipe,
                        walked,
                        typed_values=_typed_source,
                    ) or walked
                population_rows = walked
                rows_are_population = True

                def replay_source() -> Iterable[Mapping[str, Any]] | None:
                    again = iter_table_population(
                        source_kind=source_kind,
                        source_format=source_format,
                        source_connector_id=source_connector_id,
                        source_config=source_config,
                        source_table=source_table,
                        mappings=mappings,
                        column_types=column_types,
                        dest_types=destination_column_types,
                        dest_db=destination_db_type,
                        sync_mode=sync_mode,
                        read_scope=read_scope if incremental_read_narrows(sync_mode) else None,
                        source_filter=resolved_filter or None,
                        shape_recipe=shape_recipe or None,
                    )
                    if again is None or not shape_recipe:
                        return again
                    from services.shape_preflight import shaped_population_rows

                    return shaped_population_rows(
                        shape_recipe,
                        again,
                        typed_values=_typed_source,
                    ) or again

    scan_input: Iterable[Mapping[str, Any]] | None = (
        population_rows if population_rows is not None else sample_rows
    )
    fit_rows_total = int(row_count or 0)
    if resolved_filter:
        filtered = iter_filtered_rows(scan_input, resolved_filter)
        if filtered is not None:
            scan_input = filtered
        fit_rows_total = 0
    if incremental_read_narrows(sync_mode) and read_scope.bounded:
        narrowed = iter_rows_after_watermark(scan_input, read_scope, keep_unreadable=True)
        if narrowed is not None:
            scan_input = narrowed
        fit_rows_total = 0
    fit_report = scan_population_fit(
        scan_input,
        mappings,
        dest_types=destination_column_types,
        source_types=column_types,
        dest_db=destination_db_type,
        dialect_label=destination_db_type,
        job_error_policy=transform_error_policy_for_validation_mode(validation_mode),
        rows_total=fit_rows_total,
        rows_are_population=bool(rows_are_population and population_rows is not None),
        source_kind=source_kind,
        source_format=source_format,
        sync_mode=sync_mode,
        dest_table_exists=destination_table_exists,
        deadline_monotonic=deadline_monotonic,
        on_progress=on_progress,
    )
    if population_walk_error:
        from dataclasses import replace

        fit_report = replace(
            fit_report,
            truncated_reason="walk",
            note="Population scan stopped because the source table walk failed.",
            probe_error=population_walk_error,
        )
    if destination_table_exists is False and replay_source is not None:
        from services.population_fit_scan import create_new_population_widen

        def narrowed(rows_in: Iterable[Mapping[str, Any]]) -> Iterable[Mapping[str, Any]]:
            out_rows = rows_in
            if resolved_filter:
                filtered = iter_filtered_rows(out_rows, resolved_filter)
                if filtered is not None:
                    out_rows = filtered
            if incremental_read_narrows(sync_mode) and read_scope.bounded:
                narrowed_rows = iter_rows_after_watermark(
                    out_rows, read_scope, keep_unreadable=True
                )
                if narrowed_rows is not None:
                    out_rows = narrowed_rows
            return out_rows

        def replay() -> Iterable[Mapping[str, Any]] | None:
            again = replay_source()
            return None if again is None else narrowed(again)

        widen = create_new_population_widen(
            fit_report,
            mappings,
            replay,
            scan_kwargs=dict(
                dest_types=destination_column_types,
                source_types=column_types,
                dest_db=destination_db_type,
                dialect_label=destination_db_type,
                job_error_policy=transform_error_policy_for_validation_mode(
                    validation_mode
                ),
                rows_total=fit_rows_total,
                rows_are_population=bool(rows_are_population),
                source_kind=source_kind,
                source_format=source_format,
                sync_mode=sync_mode,
                dest_table_exists=destination_table_exists,
            ),
        )
        if widen is not None:
            fit_report = widen.report
    return fit_report, population_walk_error, population_rows, rows_are_population, resolved_filter
