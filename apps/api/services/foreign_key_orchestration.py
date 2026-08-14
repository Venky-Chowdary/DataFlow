"""Run the foreign-key carry across a multi-table transfer.

Sequencing, not policy: :mod:`services.foreign_key_carry` decides what may be
carried and what must be refused; this module measures the source keys, orders
the tables so parents load first, and — after the load — issues the constraint
DDL and re-reads the destination catalog.

Constraints are added at the end on purpose. ``ALTER TABLE ADD CONSTRAINT``
validates the rows already present, so a successful add is proof the loaded
data has no orphans, while adding them up front would either reject valid rows
that arrive out of order or force a per-row parent lookup the engine does
better in one pass.
"""

from __future__ import annotations

import logging
from typing import Any

from services.foreign_key_carry import (
    ForeignKeyDecision,
    apply_foreign_keys,
    order_tables_by_dependency,
    plan_foreign_keys,
    verify_foreign_keys,
)
from services.foreign_key_metadata import (
    SUPPORTED_DIALECTS,
    ForeignKeys,
    probe_foreign_keys,
)

logger = logging.getLogger(__name__)


def measure_source_foreign_keys(
    dialect: str, cfg: dict[str, Any], tables: list[str]
) -> dict[str, ForeignKeys]:
    """Measure foreign keys for every selected source table.

    One connection for the whole set: a per-table connect on a 40-table job is
    the difference between a plan that costs milliseconds and one an operator
    notices.
    """
    key = (dialect or "").strip().lower()
    from services.dialect_profiles import catalog_namespace

    schema = catalog_namespace(key, cfg)
    if key not in SUPPORTED_DIALECTS:
        return {
            table: ForeignKeys(
                dialect=key,
                status="unavailable",
                table=table,
                schema=schema,
                detail=f"Foreign key catalog probe not implemented for '{key}'.",
            )
            for table in tables
        }
    from connectors.generic_sql import _engine

    out: dict[str, ForeignKeys] = {}
    try:
        engine = _engine(cfg)
        with engine.connect() as conn:
            for table in tables:
                out[table] = probe_foreign_keys(key, conn, schema, table)
    except Exception as exc:  # noqa: BLE001 — an unreachable catalog is a state
        logger.debug("source foreign key measurement failed: %s", exc, exc_info=exc)
        for table in tables:
            out.setdefault(
                table,
                ForeignKeys(
                    dialect=key,
                    status="unavailable",
                    table=table,
                    schema=schema,
                    detail=f"{type(exc).__name__}: {exc}",
                ),
            )
    return out


def dependency_order(
    tables: list[str], source_keys: dict[str, ForeignKeys]
) -> tuple[list[str], list[str]]:
    """Order the selected tables parents-first. Returns (order, cycle members)."""
    dependencies = {
        table: {
            fk.referenced_table
            for fk in keys.items
            if fk.referenced_table and fk.referenced_table.lower() != table.lower()
        }
        for table, keys in source_keys.items()
    }
    return order_tables_by_dependency(tables, dependencies)


def _destination_tables(engine: Any, dialect: str, schema: str) -> set[str] | None:
    """List destination tables, or ``None`` when the catalog cannot be read.

    ``None`` matters: "I could not list the destination" must not be planned as
    "the referenced parent is missing". SQLite has no schema layer — a file
    path must not be handed to the inspector as a namespace.
    """
    try:
        import sqlalchemy as sa

        key = (dialect or "").strip().lower()
        ns = None if key in {"sqlite", "duckdb"} else (schema or None)
        inspector = sa.inspect(engine)
        return {t.lower() for t in inspector.get_table_names(schema=ns)}
    except Exception as exc:  # noqa: BLE001 — unreadable catalog is a state
        logger.debug("destination table list failed on %s: %s", dialect, exc)
        return None


def carry_foreign_keys(
    *,
    dest_dialect: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    source_keys: dict[str, ForeignKeys],
    table_map: dict[str, str],
    column_maps: dict[str, dict[str, str]],
    dest_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    """Add and verify the constraints for every table of a finished load.

    Returns one serializable decision per source key — the shape the fidelity
    certificate, the proof bundle and the operator view all read.
    """
    from connectors.generic_sql import _engine
    from services.dialect_profiles import catalog_namespace, schema_from_cfg

    qual_schema = schema_from_cfg(dest_dialect, dest_cfg, schema=dest_schema or None)
    catalog_ns = catalog_namespace(dest_dialect, dest_cfg, schema=dest_schema or None)

    decisions: list[ForeignKeyDecision] = []
    try:
        engine = _engine(dest_cfg)
    except Exception as exc:  # noqa: BLE001 — surface, do not pretend
        return [
            {
                "table": table,
                **ForeignKeyDecision(
                    name="*",
                    status="unknown",
                    reason=(
                        "Could not connect to the destination to add or verify "
                        f"foreign keys: {type(exc).__name__}: {exc}"
                    ),
                    dest_table=table,
                ).__dict__,
            }
            for table in source_keys
        ]

    known = _destination_tables(engine, dest_dialect, catalog_ns)
    for source_table, keys in source_keys.items():
        dest_table = table_map.get(source_table, source_table)
        plan = plan_foreign_keys(
            source_foreign_keys=keys.to_dict(),
            dest_dialect=dest_dialect,
            dest_schema=qual_schema,
            dest_table=dest_table,
            dest_columns=dest_columns.get(source_table, []),
            column_map=column_maps.get(source_table, {}),
            table_map=table_map,
            dest_existing_tables=known,
            referenced_column_maps=column_maps,
        )
        if plan.statements:
            def execute(sql: str) -> None:
                # Each constraint gets its own transaction: one rejected key
                # must not roll back the ones that took.
                with engine.begin() as conn:
                    conn.exec_driver_sql(sql)

            settled = apply_foreign_keys(plan, execute)
        else:
            settled = list(plan.decisions)
        if any(d.status == "planned" for d in settled):
            with engine.connect() as conn:
                dest_keys = probe_foreign_keys(
                    dest_dialect, conn, catalog_ns, dest_table
                )
            settled = verify_foreign_keys(settled, dest_keys)
        decisions.extend(settled)

    return [{"table": d.dest_table, **d.__dict__} for d in decisions]


def summarize(decisions: list[dict[str, Any]]) -> dict[str, Any]:
    """Counts plus the honest headline for the operator."""
    counts: dict[str, int] = {}
    for decision in decisions:
        status = str(decision.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    violations = [d for d in decisions if d.get("integrity_violation")]
    return {
        "decisions": decisions,
        "counts": counts,
        "integrity_violations": len(violations),
        "carried": counts.get("carried", 0),
        "verdict": (
            "referential_integrity_violated"
            if violations
            else (
                "carried"
                if counts.get("carried")
                and not counts.get("unsupported")
                and not counts.get("unknown")
                else "partial"
            )
        ),
    }
