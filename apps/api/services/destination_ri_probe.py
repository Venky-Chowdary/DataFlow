"""Destination referential-integrity proof: the parent rows actually exist.

A carried foreign key is a *promise*; it is only worth anything if the engine
was enforcing it while the rows landed. Two real migration outcomes this
module separates, which a catalog diff alone cannot:

``enforced``   the destination carries the FK, so the engine itself refused
               orphans as they were written — no scan needed
``scanned``    the destination has no such constraint (dropped for load speed,
               or never created), so the child rows are anti-joined against
               the parent and orphans are counted for real

Composite keys are scanned as a tuple under SQL ``MATCH SIMPLE``: a child row
with any NULL in the key is unconstrained and is not an orphan.

Anything else — parent table missing, unreadable catalog, failed scan — is
reported unavailable with a reason. An unproven relationship never counts as
clean, because "no orphans found" and "no scan ran" look identical in a report
and only one of them is true.
"""

from __future__ import annotations

import logging
from typing import Any

import sqlalchemy as sa

from services.physical_state_diff import catalog_table_names, resolve_stored_name
from services.value_serializer import present_cell_text

logger = logging.getLogger(__name__)

__all__ = ["verify_destination_referential_integrity"]

MAX_EXAMPLES = 10


def _orphan_example_text(row: Any) -> str:
    """One orphan key on the reader wire. SQL NULL is not a customer token."""
    return "+".join(present_cell_text(v) or "" for v in row)


def _fold(name: Any) -> str:
    return str(name or "").strip().casefold()


def _orphan_scan(
    conn: Any,
    *,
    child: Any,
    child_columns: list[str],
    parent: Any,
    parent_columns: list[str],
) -> dict[str, Any]:
    """Anti-join the child against the parent through the reflected columns.

    Composite keys join on every column pair at once; MATCH SIMPLE means a key
    with any NULL component imposes no constraint, so those rows are excluded
    rather than counted as orphans.
    """
    c_cols = [child.c.get(name) for name in child_columns]
    p_cols = [parent.c.get(name) for name in parent_columns]
    if any(c is None for c in c_cols) or any(p is None for p in p_cols):
        return {"available": False, "reason": "join column missing from catalog"}

    on_clause = sa.and_(*[c == p for c, p in zip(c_cols, p_cols)])
    joined = child.outerjoin(parent, on_clause)
    where = sa.and_(
        *[c.is_not(None) for c in c_cols],
        p_cols[0].is_(None),
    )
    count = int(
        conn.execute(sa.select(sa.func.count()).select_from(joined).where(where)).scalar()
        or 0
    )
    examples = [
        _orphan_example_text(row)
        for row in conn.execute(
            sa.select(*c_cols).select_from(joined).where(where).limit(MAX_EXAMPLES)
        ).fetchall()
    ]
    return {
        "available": True,
        "orphan_count": count,
        "examples": examples,
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
            catalog_table_names(
                inspector, schema_arg, conn=conn, dialect=str(db_type)
            ),
            table,
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
        table_names = catalog_table_names(
            inspector, schema_arg, conn=conn, dialect=str(db_type)
        )
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
            if not child_cols or len(child_cols) != len(parent_cols):
                rel.update(
                    status="unavailable",
                    available=False,
                    reason="relationship has no usable column pairing",
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
                resolved_child = [
                    resolve_stored_name([c.name for c in child_tbl.columns], name)
                    for name in child_cols
                ]
                resolved_parent = [
                    resolve_stored_name([c.name for c in parent_tbl.columns], name)
                    for name in parent_cols
                ]
                if any(c is None for c in resolved_child) or any(
                    p is None for p in resolved_parent
                ):
                    raise LookupError("join column not resolvable in destination")
                scan = _orphan_scan(
                    conn,
                    child=child_tbl,
                    child_columns=[str(c) for c in resolved_child],
                    parent=parent_tbl,
                    parent_columns=[str(p) for p in resolved_parent],
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
