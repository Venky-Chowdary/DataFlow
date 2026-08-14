"""Foreign-key carry for streaming loads — measure on the source, add after load.

Extracted from ``stream.py`` (Phase F8 god-module decomposition). Referential
constraints cannot be created alongside the rows: a child table would reject its
own inserts while its parent is still empty. So the streaming path measures the
source references up front, uses them to load parents first, and only re-adds the
constraints once every table has landed.

Neither step may abort a transfer. A source whose catalog cannot be read reports
its keys as ``unknown`` rather than absent, so the operator sees an unmeasured
constraint instead of a silently dropped one.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from .adapters import resolve_connector_config
from .connector_capabilities import resolve_driver_type
from .models import EndpointConfig

logger = logging.getLogger(__name__)


@dataclass
class ForeignKeyContext:
    """Measured source references for the tables of one multi-stream job."""

    source_keys: dict[str, Any] = field(default_factory=dict)
    order: list[str] = field(default_factory=list)
    cycle: list[str] = field(default_factory=list)
    column_maps: dict[str, dict[str, str]] = field(default_factory=dict)


def foreign_key_context(source: EndpointConfig, tables: list[str]) -> ForeignKeyContext:
    """Measure source foreign keys and derive a parents-first load order.

    A failure here never blocks the transfer: the keys are then reported as
    unmeasured (``unknown``) rather than absent, and the streams keep the
    operator's declared order.
    """
    context = ForeignKeyContext()
    names = [t for t in tables if t]
    # A single table still declares references: its parents simply live on the
    # destination already rather than in this job, so the keys are measured and
    # planned against the destination catalog. Ordering is the only part that
    # needs more than one stream.
    if source.kind != "database" or not names:
        return context
    try:
        from services.foreign_key_orchestration import (
            dependency_order,
            measure_source_foreign_keys,
        )

        src_type = resolve_driver_type(source.format)
        src_cfg = resolve_connector_config(source)
        context.source_keys = measure_source_foreign_keys(src_type, src_cfg, names)
        if len(names) > 1:
            context.order, context.cycle = dependency_order(names, context.source_keys)
    except Exception as exc:
        logger.debug("foreign key ordering skipped: %s", exc, exc_info=exc)
    return context


def carry_foreign_keys_after_load(
    destination: EndpointConfig,
    context: ForeignKeyContext,
    table_map: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """Add the planned constraints once every table has landed, then re-read."""
    if not context.source_keys or destination.kind != "database":
        return None
    try:
        from services.foreign_key_orchestration import carry_foreign_keys, summarize

        dest_type = resolve_driver_type(destination.format)
        dest_cfg = resolve_connector_config(destination)
        from services.dialect_profiles import catalog_namespace

        dest_schema = catalog_namespace(dest_type, dest_cfg)
        # Multi-stream lands each source table under its own name; a rename
        # would arrive through the contract and change only this map.
        table_map = dict(table_map or {}) or {t: t for t in context.source_keys}
        summary = summarize(
            carry_foreign_keys(
                dest_dialect=dest_type,
                dest_cfg=dest_cfg,
                dest_schema=dest_schema,
                source_keys=context.source_keys,
                table_map=table_map,
                column_maps=context.column_maps,
                dest_columns={
                    t: list(cols.values()) for t, cols in context.column_maps.items()
                },
            )
        )
        if context.cycle:
            summary["cycle"] = list(context.cycle)
        return summary
    except Exception as exc:
        logger.warning("foreign key carry failed: %s", exc, exc_info=exc)
        return {
            "decisions": [],
            "counts": {"unknown": len(context.source_keys)},
            "integrity_violations": 0,
            "carried": 0,
            "verdict": "unknown",
            "error": f"{type(exc).__name__}: {exc}",
        }
