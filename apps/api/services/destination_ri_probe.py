"""Destination referential-integrity proof: the parent rows actually exist.

A carried foreign key is a *promise*; it is only worth anything if the engine
was enforcing it while the rows landed. Two real migration outcomes this
module separates, which a catalog diff alone cannot:

``enforced``   the destination carries the FK, so the engine itself refused
               orphans as they were written — no scan needed
``scanned``    the destination has no such constraint (dropped for load speed,
               or never created), so the child rows are anti-joined against
               the parent and orphans are counted for real

Anything else — parent table missing, composite FK, unreadable catalog — is
reported unavailable with a reason. An unproven relationship never counts as
clean, because "no orphans found" and "no scan ran" look identical in a report
and only one of them is true.
"""

from __future__ import annotations

import logging
from typing import Any

import sqlalchemy as sa

from services.physical_state_diff import resolve_stored_name

logger = logging.getLogger(__name__)

__all__ = ["verify_destination_referential_integrity"]

MAX_EXAMPLES = 10


def _fold(name: Any) -> str:
    return str(name or "").strip().casefold()


def _orphan_scan(
    conn: Any,
    *,
    child: Any,
    child_column: str,
    parent: Any,
    parent_column: str,
) -> dict[str, Any]:
    """Anti-join the child against the parent through the reflected columns."""
    c_col = child.c.get(child_column)
    p_col = parent.c.get(parent_column)
    if c_col is None or p_col is None:
        return {"available": False, "reason": "join column missing from catalog"}

    joined = child.outerjoin(parent, c_col == p_col)
    where = sa.and_(c_col.is_not(None), p_col.is_(None))
    count = int(
        conn.execute(sa.select(sa.func.count()).select_from(joined).where(where)).scalar()
        or 0
    )
    examples = [
        row[0]
        for row in conn.execute(
            sa.select(c_col).select_from(joined).where(where).limit(MAX_EXAMPLES)
        ).fetchall()
        if row and row[0] is not None
    ]
    return {
        "available": True,
        "orphan_count": count,
        "examples": [str(v) for v in examples],
    }


def _reflect(conn: Any, meta: sa.MetaData, name: str, schema: str | None) -> Any:
    return sa.Table(name, meta, autoload_with=conn, schema=schema)


def verify_destination_referential_integrity(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str = "",
    table: str,
    foreign_keys: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Prove every source relationship still holds in the destination data.

    ``foreign_keys`` are the relationships the *source* guaranteed (each with
    ``constrained_columns``, ``referred_table``, ``referred_columns``). When
    omitted, the destination's own catalog FKs are used — those are enforced by
    definition, so the interesting call passes the source's.
    """
    if not table:
        return {"verified": False, "reason": "no table name to inspect"}

    from connectors.generic_sql import get_sqlalchemy_engine

    try:
        engine = get_sqlalchemy_engine({**cfg, "type": db_type})
    except Exception as exc:  # noqa: BLE001 — a refused connection is evidence
        return {"verified": False, "reason": f"cannot connect: {exc}"}

    relations: list[dict[str, Any]] = []
    schema_arg = schema or None
    with engine.connect() as conn:
        inspector = sa.inspect(conn)
        child_name = resolve_stored_name(
            inspector.get_table_names(schema=schema_arg), table
        )
        if child_name is None:
            return {
                "verified": False,
                "reason": f"table {table} not found in destination catalog",
            }

        dest_fks = inspector.get_foreign_keys(child_name, schema=schema_arg)
        enforced = {
            (
                "+".join(_fold(c) for c in fk.get("constrained_columns") or ()),
                _fold(fk.get("referred_table")),
            )
            for fk in dest_fks
            if fk.get("constrained_columns")
        }
        wanted = list(foreign_keys if foreign_keys is not None else dest_fks)
        if not wanted:
            return {
                "verified": True,
                "reason": "source declares no foreign keys",
                "relations": [],
            }

        meta = sa.MetaData()
        table_names = inspector.get_table_names(schema=schema_arg)
        for fk in wanted:
            child_cols = [str(c) for c in fk.get("constrained_columns") or () if c]
            parent_cols = [str(c) for c in fk.get("referred_columns") or () if c]
            parent_table = str(fk.get("referred_table") or "")
            key = (
                "+".join(_fold(c) for c in child_cols),
                _fold(parent_table),
            )
            rel: dict[str, Any] = {
                "columns": child_cols,
                "referred_table": parent_table,
                "referred_columns": parent_cols,
            }
            if key in enforced:
                rel.update(status="enforced", available=True, orphan_count=0)
                relations.append(rel)
                continue
            if len(child_cols) != 1 or len(parent_cols) != 1:
                rel.update(
                    status="unavailable",
                    available=False,
                    reason="composite foreign keys are not scanned",
                )
                relations.append(rel)
                continue
            stored_parent = resolve_stored_name(table_names, parent_table)
            if stored_parent is None:
                rel.update(
                    status="unavailable",
                    available=False,
                    reason=f"parent table {parent_table} absent from destination",
                )
                relations.append(rel)
                continue
            try:
                child_tbl = _reflect(conn, meta, child_name, schema_arg)
                parent_tbl = _reflect(conn, meta, stored_parent, schema_arg)
                child_col = resolve_stored_name(
                    [c.name for c in child_tbl.columns], child_cols[0]
                )
                parent_col = resolve_stored_name(
                    [c.name for c in parent_tbl.columns], parent_cols[0]
                )
                if child_col is None or parent_col is None:
                    raise LookupError("join column not resolvable in destination")
                scan = _orphan_scan(
                    conn,
                    child=child_tbl,
                    child_column=child_col,
                    parent=parent_tbl,
                    parent_column=parent_col,
                )
            except Exception as exc:  # noqa: BLE001 — a failed scan is evidence
                logger.warning("destination RI scan failed: %s", exc)
                scan = {"available": False, "reason": f"scan failed: {exc}"}
            if scan.get("available"):
                rel.update(status="scanned", **scan)
            else:
                rel.update(status="unavailable", **scan)
            relations.append(rel)

    orphaned = [r for r in relations if int(r.get("orphan_count") or 0) > 0]
    unavailable = [r for r in relations if not r.get("available")]
    return {
        "verified": not orphaned and not unavailable,
        "relations": relations,
        "orphan_relations": [
            f"{'+'.join(r['columns'])}->{r['referred_table']}" for r in orphaned
        ],
        "unavailable_relations": [
            f"{'+'.join(r['columns'])}->{r['referred_table']}" for r in unavailable
        ],
        "orphan_rows": sum(int(r.get("orphan_count") or 0) for r in relations),
    }
