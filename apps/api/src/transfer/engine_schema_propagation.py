"""Destination schema propagation, backfill and primary-key resolution (re-exported by src.transfer.engine)."""

from __future__ import annotations

from .models import EndpointConfig
from typing import Any
import logging

logger = logging.getLogger("src.transfer.engine")


def _destination_schema_types(
    destination: EndpointConfig, sync_mode: str = ""
) -> dict[str, str]:
    """Introspect destination column types for schema-aware preflight and transforms.

    Relational overwrite keeps an existing table, so its live types stay in
    force. Engines that still drop and recreate return an empty type map.
    """
    from src.transfer.engine import (  # lazy: honours monkeypatches on the original module
        _engine_module,
    )
    schema, _exists = _engine_module._destination_schema_probe(
        destination, sync_mode=sync_mode
    )
    return schema


def _record_schema_propagation(
    mongo: Any,
    job_id: str,
    columns: list[str],
    mappings: list[dict[str, Any]],
    dest_schema_types: dict[str, str] | None,
) -> None:
    """Say on the job which source columns the run added, and which it left out.

    A column added to an existing destination is applied silently otherwise,
    and a live source column no mapping covers (a run that skipped preflight)
    simply never moves: the operator found out by reading the destination
    (D165). Both are named here.
    """
    from services.mapping_constraints import write_mappings

    live = {str(c).lower() for c in (dest_schema_types or {})}
    added = (
        [
            str(m.get("source") or "")
            for m in write_mappings(mappings)
            if str(m.get("target") or "").lower() not in live
        ]
        if live
        else []
    )
    covered = {
        str(m.get("source") or "").lower()
        for m in mappings or []
        if isinstance(m, dict)
    }
    skipped = [str(c) for c in columns or [] if str(c).lower() not in covered]
    if not added and not skipped:
        return
    if skipped:
        logger.warning(
            "Job %s: source column(s) %s are neither mapped nor declared omitted "
            "and will not be written",
            job_id,
            ", ".join(skipped),
        )
    mongo.update_job_status(
        job_id,
        "running",
        schema_propagation={"added_columns": added, "skipped_columns": skipped},
    )


def _forget_cached_table_shapes(*endpoints: Any) -> None:
    """A run reads the tables as they are now, not as an earlier run cached them."""
    from services.reflection_cache import invalidate_named_tables

    invalidate_named_tables(
        *(
            name
            for endpoint in endpoints
            for name in (
                getattr(endpoint, "table", None),
                getattr(endpoint, "collection", None),
            )
        )
    )


def _run_backfill_new_fields(
    request: Any,
    mappings: list[dict[str, Any]],
    dest_schema_types: dict[str, str] | None,
    dest_table_exists: bool | None,
) -> bool:
    """Whether the writer must evolve an existing destination (ADD COLUMN / widen).

    Asked of the evolved plan (what ``_apply_schema_auto_propagate`` stamped),
    not the request's pre-evolution copy. A destination that does not exist is
    created whole: the ``create_new`` stamps then describe CREATE TABLE, not an
    ADD COLUMN, so they must not arm the backfill lane (it would also forfeit
    the engine-copy fast path for every plain first run).
    """
    from src.transfer.engine import (  # lazy: honours monkeypatches on the original module
        effective_backfill_new_fields,
    )
    return effective_backfill_new_fields(
        backfill_new_fields=request.backfill_new_fields,
        schema_policy=request.schema_policy,
        mappings=None if dest_table_exists is False else mappings,
        dest_columns=list(dest_schema_types or {}) if dest_table_exists is True else None,
    )


def _apply_schema_auto_propagate(
    *,
    request: Any,
    columns: list[str],
    schema: dict[str, str],
    mappings: list[dict[str, Any]],
    dest_schema_types: dict[str, str] | None,
) -> list[dict[str, Any]]:
    """Validate≡Execute schema auto-propagate — pause on hard-break, extend maps."""
    from src.transfer.engine import (  # lazy: honours monkeypatches on the original module
        _enrich_mappings_with_types,
        resolve_sync_contract,
        should_drop_destination_for_sync,
    )
    from services.schema_drift import (
        apply_propagate_mappings,
        detect_schema_drift,
        effective_schema_policy,
    )

    # Validate's policy, not the raw field: a backfill request is propagate.
    schema_policy = effective_schema_policy(
        getattr(request, "schema_policy", None),
        backfill_new_fields=bool(getattr(request, "backfill_new_fields", False)),
    )
    contract = resolve_sync_contract(getattr(request, "stream_contracts", None))
    from services.db_type_utils import dest_schema_is_recreated_on_overwrite

    clears_rows = should_drop_destination_for_sync(
        request_sync_mode=getattr(request, "sync_mode", None),
        contract_sync_mode=contract.sync_mode if contract else None,
    )
    dest_recreated = clears_rows and dest_schema_is_recreated_on_overwrite(
        str(getattr(getattr(request, "destination", None), "format", "") or "")
    )
    drift = detect_schema_drift(
        source_columns=columns,
        source_schema=schema,
        target_columns=list((dest_schema_types or {}).keys()),
        target_schema=dest_schema_types or {},
        mappings=mappings,
        destination_db_type=(getattr(request.destination, "format", "") or "").lower(),
        schema_policy=schema_policy,
        dest_recreated=dest_recreated,
    )
    evolution = drift.get("schema_evolution") or {}
    from services.schema_drift import execute_must_pause, propose_dest_widens

    # D107: detect_schema_drift ran without table_exists above, so its
    # live-contract lane never emitted destination-widen proposals. Emit them
    # here against the introspected destination types — additive widen_type
    # entries propagate/acknowledgement flows already understand.
    _widen_props = propose_dest_widens(
        mappings,
        source_schema=schema,
        target_schema=dest_schema_types or {},
        dest_db=(getattr(request.destination, "format", "") or "").lower(),
    )
    if _widen_props:
        _additive = list(evolution.get("additive") or [])
        _have = {str(a.get("column") or "").lower() for a in _additive}
        _additive.extend(
            p for p in _widen_props
            if str(p.get("column") or "").lower() not in _have
        )
        evolution["additive"] = _additive

    acknowledged = bool(getattr(request, "schema_drift_acknowledged", False))
    if execute_must_pause(evolution, schema_drift_acknowledged=acknowledged):
        raise ValueError(
            (drift.get("issues") or ["Breaking schema change — sync paused"])[0]
        )
    from services.db_type_utils import dest_declares_column_ddl
    from services.schema_drift import (
        PROPAGATE_POLICIES,
        destination_evolution_approved,
        plan_destination_evolution,
    )

    # The decision Validate made, from the same function and the same plan:
    # propagate (or a backfill request) evolves the destination automatically,
    # manual_review only after the acknowledgement; type_locked / pause_on_change
    # / block_all never ALTER. A schemaless sink has no DDL to evolve.
    if (
        dest_schema_types
        and dest_declares_column_ddl(
            str(getattr(request.destination, "format", "") or "")
        )
        and destination_evolution_approved(schema_policy, acknowledged=acknowledged)
    ):
        mappings, _evolved_types, _ = plan_destination_evolution(
            mappings,
            source_columns=columns,
            source_schema=schema,
            dest_types=dest_schema_types,
            dest_db=(getattr(request.destination, "format", "") or "").lower(),
            adopt_unmapped=schema_policy in PROPAGATE_POLICIES,
        )
    if not evolution.get("should_propagate"):
        return mappings
    mappings, applied = apply_propagate_mappings(
        mappings,
        source_columns=columns,
        source_schema=schema,
        evolution=evolution,
        schema_policy=schema_policy,
    )
    if not applied:
        return mappings
    return _enrich_mappings_with_types(
        mappings,
        column_types=schema,
        dest_types=dest_schema_types,
    )


def _infer_primary_key(columns: list[str], mappings: list[dict[str, Any]]) -> str:
    """Infer the primary key target column for mirror/upsert/key-addressed transfers.

    Prefers ``id`` / ``*_id`` / ``uuid``, then natural keys (``code``, ``iso``,
    ``name``, ``sku``, …). Never invents a weak attribute like ``capital`` when
    no strong candidate exists — that caused Redis keys such as
    ``countries:Abu_Dhabi`` when column order put ``capital`` first.
    """
    if not columns:
        return ""
    from services.primary_key import infer_redis_conflict_columns

    target_cols = [
        str(m.get("target") or m.get("source") or "")
        for m in (mappings or [])
        if (m.get("target") or m.get("source"))
    ]
    if not target_cols:
        target_cols = list(columns)
    inferred = infer_redis_conflict_columns(target_cols, mappings or [], None)
    if inferred:
        return inferred[0]
    # Refuse first-column fallback — weak attrs must be set explicitly on Map.
    return ""


def _declared_destination_key(
    destination: EndpointConfig,
    mappings: list[dict[str, Any]],
) -> list[str]:
    """The destination table's declared primary key, when the write covers it.

    Read from the destination catalog probe, so this is evidence rather than the
    name-shape inference used for key-addressed sinks. Returns ``[]`` when the
    table declares no key or when any key column is unmapped — an upsert keyed on
    a column this write never supplies would silently insert duplicates.
    """
    declared = [
        str(col).strip()
        for col in ((destination.extra or {}).get("primary_key_columns") or [])
        if str(col or "").strip()
    ]
    if not declared:
        return []
    written = {
        str(m.get("target") or "").strip().lower()
        for m in (mappings or [])
        if str(m.get("target") or "").strip() and not m.get("intentional_omit")
    }
    if any(col.lower() not in written for col in declared):
        return []
    return declared
