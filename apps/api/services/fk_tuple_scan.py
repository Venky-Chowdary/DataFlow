"""MATCH SIMPLE tuple anti-join / tuple-IN — one owner for source and dest RI.

Composite foreign keys are scanned as a whole tuple. A child row with any
NULL key component is unconstrained (SQL MATCH SIMPLE) and is not an orphan.
Single-column FKs use the same predicates with arity 1.

Dest post-write (`destination_ri_probe`) and source preflight
(`population_orphan_probe`, `sample_orphan_probe`) must not invent a second
algorithm.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence

from services.value_serializer import present_cell_text

MAX_EXAMPLES = 10
PARENT_IN_CHUNK = 200


def orphan_example_text(row: Any) -> str:
    """One orphan key on the reader wire. SQL NULL is not a customer token."""
    if not isinstance(row, (list, tuple)):
        return present_cell_text(row) or ""
    return "+".join(present_cell_text(v) or "" for v in row)


def match_simple_predicates(c_cols: Sequence[Any], p_cols: Sequence[Any]) -> tuple[Any, Any]:
    """Join ON every pair; WHERE every child col is NOT NULL and parent is missing."""
    import sqlalchemy as sa

    if not c_cols or len(c_cols) != len(p_cols):
        raise ValueError("FK column pairing arity mismatch")
    on_clause = sa.and_(*[c == p for c, p in zip(c_cols, p_cols)])
    where = sa.and_(
        *[c.is_not(None) for c in c_cols],
        p_cols[0].is_(None),
    )
    return on_clause, where


def scan_orphan_anti_join(
    conn: Any,
    *,
    child: Any,
    child_columns: Sequence[Any],
    parent: Any,
    parent_columns: Sequence[Any],
    max_examples: int = MAX_EXAMPLES,
) -> dict[str, Any]:
    """Anti-join child against parent. ``child_columns`` / ``parent_columns`` are ColumnElements."""
    import sqlalchemy as sa

    if any(c is None for c in child_columns) or any(p is None for p in parent_columns):
        return {"available": False, "reason": "join column missing from catalog"}
    if len(child_columns) != len(parent_columns) or not child_columns:
        return {"available": False, "reason": "relationship has no usable column pairing"}

    on_clause, where = match_simple_predicates(child_columns, parent_columns)
    joined = child.outerjoin(parent, on_clause)
    count = int(
        conn.execute(sa.select(sa.func.count()).select_from(joined).where(where)).scalar()
        or 0
    )
    examples = [
        orphan_example_text(row)
        for row in conn.execute(
            sa.select(*child_columns).select_from(joined).where(where).limit(max_examples)
        ).fetchall()
    ]
    return {
        "available": True,
        "orphan_count": count,
        "examples": examples,
    }


def _split_table(qualified: str, default_schema: str | None) -> tuple[str | None, str]:
    from connectors.sql_identifiers import split_qualified_table

    return split_qualified_table(qualified, default_schema)


def sql_population_orphan_scan(
    cfg: dict[str, Any],
    *,
    child_table: str,
    parent_table: str,
    child_columns: Sequence[str],
    parent_columns: Sequence[str],
    max_examples: int = 25,
) -> dict[str, Any]:
    """Full-table MATCH SIMPLE anti-join using unbound ``sa.table`` / ``sa.column``."""
    import sqlalchemy as sa

    from connectors.generic_sql import _engine

    child_cols = [str(c).strip() for c in child_columns if str(c).strip()]
    parent_cols = [str(c).strip() for c in parent_columns if str(c).strip()]
    if not child_cols or len(child_cols) != len(parent_cols):
        raise ValueError("incomplete table/column for population orphan scan")

    schema = (cfg.get("schema") or "").strip() or None
    child_schema, child_name = _split_table(child_table, schema)
    parent_schema, parent_name = _split_table(parent_table, schema)
    if not child_name or not parent_name:
        raise ValueError("incomplete table/column for population orphan scan")

    child = sa.table(
        child_name,
        *[sa.column(c) for c in child_cols],
        schema=child_schema,
    )
    parent = sa.table(
        parent_name,
        *[sa.column(p) for p in parent_cols],
        schema=parent_schema,
    )
    c_els = [child.c[c] for c in child_cols]
    p_els = [parent.c[p] for p in parent_cols]

    engine = _engine(cfg)
    with engine.connect() as conn:
        scan = scan_orphan_anti_join(
            conn,
            child=child,
            child_columns=c_els,
            parent=parent,
            parent_columns=p_els,
            max_examples=max_examples,
        )
    if not scan.get("available"):
        raise ValueError(scan.get("reason") or "population orphan scan unavailable")
    return {
        "orphan_count": int(scan.get("orphan_count") or 0),
        "examples": list(scan.get("examples") or []),
    }


def _tuple_in_clause(p_cols: Sequence[Any], chunk: Sequence[Sequence[Any]]) -> Any:
    import sqlalchemy as sa

    try:
        return sa.tuple_(*p_cols).in_([tuple(row) for row in chunk])
    except Exception:
        return sa.or_(
            *[sa.and_(*[col == val for col, val in zip(p_cols, row)]) for row in chunk]
        )


def sql_existing_parent_tuples(
    cfg: dict[str, Any],
    *,
    parent_table: str,
    parent_columns: Sequence[str],
    values: Sequence[Sequence[Any]],
) -> list[tuple[Any, ...]]:
    """Return the subset of ``values`` that exist in the parent as whole tuples."""
    import sqlalchemy as sa

    from connectors.generic_sql import _engine

    parent_cols = [str(c).strip() for c in parent_columns if str(c).strip()]
    rows = [tuple(v) for v in values if v is not None and len(tuple(v)) == len(parent_cols)]
    if not rows or not parent_table or not parent_cols:
        return []

    schema, table_name = _split_table(
        parent_table, (cfg.get("schema") or "").strip() or None
    )
    tbl = sa.table(
        table_name,
        *[sa.column(c) for c in parent_cols],
        schema=schema,
    )
    p_els = [tbl.c[c] for c in parent_cols]
    found: list[tuple[Any, ...]] = []
    engine = _engine(cfg)
    with engine.connect() as conn:
        for i in range(0, len(rows), PARENT_IN_CHUNK):
            chunk = rows[i : i + PARENT_IN_CHUNK]
            stmt = sa.select(*p_els).select_from(tbl).where(_tuple_in_clause(p_els, chunk))
            for rec in conn.execute(stmt).fetchall():
                found.append(tuple(rec))
    return found


def distinct_fk_tuples(
    sample_rows: list[dict[str, Any]] | None,
    columns: Sequence[str],
    *,
    present_key,
    limit: int = 500,
) -> list[tuple[Any, ...]]:
    """Distinct MATCH SIMPLE tuples from the Validate sample.

    A row with any NULL / blank component is unconstrained and is omitted.
    ``present_key`` is the same cell presenter the sample probe already uses.
    """
    cols = [str(c).strip() for c in columns if str(c).strip()]
    if not sample_rows or not cols:
        return []
    seen: set[tuple[str, ...]] = set()
    out: list[tuple[Any, ...]] = []
    for row in sample_rows:
        if not isinstance(row, dict):
            continue
        raw = tuple(row.get(c) for c in cols)
        keys = tuple(present_key(v) for v in raw)
        if any(k is None for k in keys):
            continue
        if keys in seen:
            continue
        seen.add(keys)
        out.append(raw)
        if len(out) >= limit:
            break
    return out


def orphan_tuples(
    child_values: Iterable[Sequence[Any]],
    parent_values: Iterable[Sequence[Any]],
    *,
    present_key,
) -> list[tuple[Any, ...]]:
    """Child tuples with no parent match (string-normalized, whole-tuple)."""
    parent_keys = set()
    for v in parent_values:
        keys = tuple(present_key(x) for x in v)
        if any(k is None for k in keys):
            continue
        parent_keys.add(keys)
    orphans: list[tuple[Any, ...]] = []
    seen: set[tuple[str, ...]] = set()
    for v in child_values:
        tup = tuple(v)
        keys = tuple(present_key(x) for x in tup)
        if any(k is None for k in keys) or keys in seen:
            continue
        if keys not in parent_keys:
            seen.add(keys)
            orphans.append(tup)
    return orphans
