"""Foreign-key carry for streaming loads — measure on the source, add after load.

Extracted from ``stream.py`` (Phase F8 god-module decomposition). Referential
constraints cannot be created alongside the rows: a child table would reject its
own inserts while its parent is still empty. So the streaming path measures the
source references up front, uses them to load parents first, and only re-adds the
constraints once every table has landed.

Neither step may abort a transfer. A source whose catalog cannot be read reports
its keys as ``unknown`` rather than absent, so the operator sees an unmeasured
constraint instead of a silently dropped one.

A detected FK cycle is loaded in declared order (no fake topo sort) and the
edges are still added after every table lands. ``cycle_resolved`` is True only
when every cycle edge re-reads as ``carried``.
"""

from __future__ import annotations

import contextvars
import logging
from dataclasses import dataclass, field
from typing import Any

from .adapters import resolve_connector_config
from .connector_capabilities import resolve_driver_type
from .models import EndpointConfig

logger = logging.getLogger(__name__)

# Multi-stream sets this while each table loads. The per-table carry only sees
# that table, so on one server it binds the foreign key to the source parent.
# The job carry, after every table has landed, is the one that knows the map.
_DEFER_SINGLE_TABLE_FK: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "df_defer_single_table_fk",
    default=False,
)


def push_deferred_single_table_foreign_keys() -> contextvars.Token[bool]:
    return _DEFER_SINGLE_TABLE_FK.set(True)


def pop_deferred_single_table_foreign_keys(token: contextvars.Token[bool]) -> None:
    _DEFER_SINGLE_TABLE_FK.reset(token)


def single_table_foreign_key_carry_deferred() -> bool:
    return bool(_DEFER_SINGLE_TABLE_FK.get())


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
                cycle_tables=list(context.cycle),
            ),
            cycle=list(context.cycle),
        )
        if context.order:
            summary["dependency_order"] = list(context.order)
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


#: SQL-ish destinations the write-path orphan guard can address through
#: ``connectors.generic_sql._engine`` + ``services.fk_tuple_scan``.
_FK_GUARD_DIALECTS = frozenset(
    {
        "postgresql",
        "redshift",
        "mysql",
        "mariadb",
        "sqlite",
        "duckdb",
        "mssql",
        "sqlserver",
        "oracle",
        "snowflake",
    }
)


@dataclass
class _FKGuardRelation:
    """One source foreign key translated for a destination parent lookup."""

    child_columns: list[str]  # source header names carrying the FK values
    parent_table: str  # destination parent table (same-name default)
    parent_columns: list[str]  # destination parent column names
    match: str
    label: str


class FKOrphanGuard:
    """Per-batch orphan quarantine against destination parent tables.

    A foreign key the source declared is a *promise about the rows*, not just
    a constraint to re-add later. Rows whose parent does not exist at the
    destination are orphans the moment they land — QA fk__pg-maria/fk__pg-pg
    measured 30 child rows written while the parent was empty and 3 orphan
    keys (997, 998, 999) committed beside them. The guard anti-joins each
    batch's distinct FK tuples against the destination parent before the
    write and holds the orphans back as quarantined rejects instead.
    """

    def __init__(
        self,
        *,
        dest_cfg: dict[str, Any],
        dest_dialect: str,
        relations: list[_FKGuardRelation],
    ) -> None:
        self._dest_cfg = dest_cfg
        self._dest_dialect = dest_dialect
        self._relations = relations
        # Tuples proven present stay present for the run (append semantics).
        # Misses are never cached — a parent arriving mid-run must verify.
        self._present: list[set[tuple[Any, ...]]] = [set() for _ in relations]
        self._checked = False

    def partition(
        self, headers: list[str], rows: list[list[Any]]
    ) -> tuple[list[list[Any]], list[dict[str, Any]]]:
        """Return ``(rows to write, orphan rejected_details)``."""
        from services.fk_tuple_scan import (
            sql_existing_parent_tuples,
            tuple_match_class,
        )
        from services.value_serializer import present_cell_text

        if not rows or not self._relations:
            return rows, []
        header_idx = {str(h): i for i, h in enumerate(headers or [])}
        orphan_idx: set[int] = set()
        details: list[dict[str, Any]] = []

        def _key(tup: tuple[Any, ...]) -> tuple[str | None, ...]:
            # Compare source and destination cells on the same normalized wire —
            # "997" vs 997 is one parent, not an orphan.
            return tuple(present_cell_text(v) for v in tup)

        for rel_i, rel in enumerate(self._relations):
            positions = [header_idx.get(c) for c in rel.child_columns]
            if any(p is None for p in positions):
                # FK columns not on this read page — nothing to verify.
                continue
            need: dict[tuple[Any, ...], list[int]] = {}
            row_keys: dict[int, tuple[Any, ...]] = {}
            for i, row in enumerate(rows):
                if i in orphan_idx:
                    continue
                tup = tuple(row[p] for p in positions)  # type: ignore[index]
                role = tuple_match_class(_key(tup), rel.match)
                if role == "skip":
                    continue
                row_keys[i] = tup
                if role == "violation":
                    orphan_idx.add(i)
                    details.append(
                        self._detail(i, rel, tup, "MATCH FULL partial NULL")
                    )
                    continue
                if _key(tup) not in self._present[rel_i]:
                    need.setdefault(tup, []).append(i)

            if need:
                try:
                    found = sql_existing_parent_tuples(
                        self._dest_cfg,
                        parent_table=rel.parent_table,
                        parent_columns=rel.parent_columns,
                        values=list(need),
                    )
                except Exception as exc:
                    # Parent probe failed — fail closed per the quarantine rule:
                    # an unverified child batch must not silently land.
                    raise ValueError(
                        f"FK orphan guard could not verify {rel.label} — "
                        f"refusing to write unverified child rows: {exc}"
                    ) from exc
                self._checked = True
                found_keys = {_key(tuple(v)) for v in found}
                self._present[rel_i].update(found_keys)
                for tup, idxs in need.items():
                    if _key(tup) in found_keys:
                        continue
                    for i in idxs:
                        if i in orphan_idx:
                            continue
                        orphan_idx.add(i)
                        details.append(self._detail(i, rel, tup, "no parent row"))

        kept = [row for i, row in enumerate(rows) if i not in orphan_idx]
        return kept, details

    @staticmethod
    def _detail(
        row_idx: int, rel: _FKGuardRelation, tup: tuple[Any, ...], why: str
    ) -> dict[str, Any]:
        return {
            "row": row_idx + 1,
            "column": "+".join(rel.child_columns),
            "reason": (
                f"Orphan foreign key ({why}): {rel.label} "
                f"= {tuple(str(v) for v in tup)}"
            ),
            "error_class": "orphan_foreign_key",
            "fk_child_columns": list(rel.child_columns),
            "fk_parent": rel.label.split("->")[-1].strip() if "->" in rel.label else rel.label,
        }


def build_fk_orphan_guard(
    source: EndpointConfig,
    destination: EndpointConfig,
    table: str,
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
) -> FKOrphanGuard | None:
    """Build the write-path orphan guard for one stream, or ``None``.

    Scope is deliberate: single-stream transfers only. A multi-stream job
    already loads parents first (``dependency_order``) and its cycle members
    defer to post-load ``ALTER`` validation, so intra-job parents must never
    be write-guarded — the parent table may legitimately not have landed yet.
    """
    if single_table_foreign_key_carry_deferred():
        return None
    if destination.kind != "database" or source.kind != "database":
        return None
    if (dest_type or "").lower() not in _FK_GUARD_DIALECTS:
        return None
    fk_context = foreign_key_context(source, [table])
    keys = fk_context.source_keys.get(table)
    if keys is None or not getattr(keys, "items", None):
        return None
    relations: list[_FKGuardRelation] = []
    for fk in keys.items:
        child_cols = [str(c).strip() for c in (fk.columns or []) if str(c).strip()]
        parent_cols = [
            str(c).strip() for c in (fk.referenced_columns or []) if str(c).strip()
        ]
        parent_table = str(getattr(fk, "referenced_table", "") or "").strip()
        if not child_cols or not parent_cols or not parent_table:
            continue
        # Self-referential FKs resolve inside this load; post-load ALTER owns them.
        if parent_table.lower() == (table or "").lower():
            continue
        relations.append(
            _FKGuardRelation(
                child_columns=child_cols,
                parent_table=parent_table,
                parent_columns=parent_cols,
                match=str(getattr(fk, "match", "") or ""),
                label=f"{table}.{','.join(child_cols)} -> {parent_table}.{','.join(parent_cols)}",
            )
        )
    if not relations:
        return None
    return FKOrphanGuard(
        dest_cfg=dest_cfg, dest_dialect=dest_type, relations=relations
    )


def carry_single_table_foreign_keys(
    source: EndpointConfig,
    destination: EndpointConfig,
    table: str,
    dest_table: str,
    mappings: list[dict] | None,
    dest_summary: dict[str, Any],
    ddl_log: list[str],
) -> None:
    """Carry one table's references after its load.

    The parent is already on the destination instead of arriving in this run,
    so without this the child lands with its foreign keys dropped and the run
    still goes green. A multi-stream job defers this: the per-table map does
    not know the parent is landing in the destination schema, and a same-server
    source parent would be bound instead.
    """
    if single_table_foreign_key_carry_deferred():
        return
    fk_context = foreign_key_context(source, [table])
    if not fk_context.source_keys:
        return
    fk_context.column_maps[table] = {
        str(m.get("source") or ""): str(m.get("target") or "")
        for m in (mappings or [])
        if m.get("source") and m.get("target")
    }
    fk_summary = carry_foreign_keys_after_load(
        destination, fk_context, {table: dest_table}
    )
    if fk_summary is None:
        return
    dest_summary["foreign_keys"] = fk_summary
    for decision in fk_summary.get("decisions") or []:
        if decision.get("status") in {"carried", "unsupported"} and decision.get(
            "dest_ddl"
        ):
            ddl_log.append(
                f"{str(decision['status']).upper()} FK: {decision['dest_ddl']}"
            )
