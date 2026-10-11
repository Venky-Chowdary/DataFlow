"""Tuple anti-join / tuple-IN — one owner for source and dest RI.

Composite foreign keys are scanned as a whole tuple. The catalog match type
selects the rule. Unreported is MATCH SIMPLE: a child row with any NULL key
component is unconstrained and is not an orphan. MATCH FULL allows a row only
when every key column is NULL or every key column matches a parent row. A
partial NULL is an orphan under FULL. MATCH PARTIAL is stored by PostgreSQL
and is not implemented, so that fact is not a completed scan.

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
    if isinstance(row, (str, bytes)):
        return present_cell_text(row) or ""
    try:
        cells = list(row)
    except TypeError:
        return present_cell_text(row) or ""
    return "+".join(present_cell_text(v) or "" for v in cells)


def normalize_match(value: Any) -> str:
    """Catalog match spelling.

    ``""`` when the catalog did not name one. ``simple``, ``full``, and
    ``partial`` are the SQL match types. PostgreSQL stores ``s``, ``f``, and
    ``p``. ``unknown`` is a present spelling this rule does not recognize, so
    the scan does not invent SIMPLE or FULL for it.
    """
    if value is None:
        return ""
    text = " ".join(str(value).strip().lower().replace("_", " ").split())
    if not text:
        return ""
    if text.startswith("match "):
        text = text[6:].strip()
    if text in {"s", "simple"}:
        return "simple"
    if text in {"f", "full"}:
        return "full"
    if text in {"p", "partial"}:
        return "partial"
    return "unknown"


def match_scan_refusal(match: str) -> str:
    """Why this match type is not a completed orphan scan. Empty when it is."""
    kind = normalize_match(match)
    if kind == "partial":
        return (
            "MATCH PARTIAL is stored and is not a completed scan. "
            "PostgreSQL accepts the type and does not implement it."
        )
    if kind == "unknown":
        return (
            "Foreign key match type could not be read, so the rows were not certified."
        )
    return ""


def match_rule_label(match: str) -> str:
    """Operator name for a match type. Unreported is the SQL default."""
    kind = normalize_match(match)
    if kind == "full":
        return "MATCH FULL"
    if kind == "partial":
        return "MATCH PARTIAL"
    if kind == "unknown":
        return "an unreadable match type"
    if kind == "simple":
        return "MATCH SIMPLE"
    return "MATCH SIMPLE (unreported)"


def match_rules_agree(planned: str, measured: str) -> bool:
    """True when the destination match rule keeps the source promise.

    Unreported is MATCH SIMPLE. A destination MATCH FULL still rejects every
    orphan MATCH SIMPLE would reject. A destination MATCH SIMPLE does not
    keep a source MATCH FULL promise: a partial NULL is unconstrained there.
    """
    want = normalize_match(planned) or "simple"
    got = normalize_match(measured) or "simple"
    if want in {"partial", "unknown"} or got in {"partial", "unknown"}:
        return False
    if want == "full":
        return got == "full"
    return got in {"simple", "full"}


def match_rule_disagreement(planned: str, measured: str) -> str:
    """Why the destination match rule does not keep the source promise.

    Empty when :func:`match_rules_agree` is true. Unreported is MATCH SIMPLE.
    MATCH PARTIAL is stored by PostgreSQL and is not implemented, so two
    PARTIAL facts are not an agreement.
    """
    if match_rules_agree(planned, measured):
        return ""
    want = normalize_match(planned)
    got = normalize_match(measured)
    if want == "partial" or got == "partial":
        return (
            "MATCH PARTIAL is stored and is not a completed comparison. "
            "PostgreSQL accepts the type and does not implement it."
        )
    if want == "unknown" or got == "unknown":
        return (
            "Foreign key match type could not be read, so the relationship "
            "was not certified."
        )
    return (
        f"Destination records {match_rule_label(measured)}; "
        f"the source rule is {match_rule_label(planned)}."
    )


def tuple_match_class(keys: Sequence[Any], match: str) -> str:
    """``skip``, ``check``, or ``violation`` for one child tuple.

    ``keys`` are presenter results. ``None`` means the cell is absent.
    Unreported and simple: any absence is ``skip``. Full: every cell absent
    is ``skip``; a mix is ``violation``; every cell present is ``check``.
    """
    kind = normalize_match(match) or "simple"
    missing = [key is None for key in keys]
    if kind == "full":
        if all(missing):
            return "skip"
        if any(missing):
            return "violation"
        return "check"
    if any(missing):
        return "skip"
    return "check"


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


def match_full_predicates(c_cols: Sequence[Any], p_cols: Sequence[Any]) -> tuple[Any, Any]:
    """MATCH FULL: all-NULL is allowed; a partial NULL is a violation.

    A row whose key columns are all present is an orphan when the parent
    tuple is missing. A row with some NULL columns and some present columns
    is an orphan whether or not a parent row shares the present values.
    """
    import sqlalchemy as sa

    if not c_cols or len(c_cols) != len(p_cols):
        raise ValueError("FK column pairing arity mismatch")
    on_clause = sa.and_(*[c == p for c, p in zip(c_cols, p_cols)])
    all_present = sa.and_(*[c.is_not(None) for c in c_cols])
    all_missing = sa.and_(*[c.is_(None) for c in c_cols])
    partial = sa.and_(sa.not_(all_present), sa.not_(all_missing))
    missing_parent = sa.and_(all_present, p_cols[0].is_(None))
    return on_clause, sa.or_(missing_parent, partial)


def scan_orphan_anti_join(
    conn: Any,
    *,
    child: Any,
    child_columns: Sequence[Any],
    parent: Any,
    parent_columns: Sequence[Any],
    max_examples: int = MAX_EXAMPLES,
    match: str = "",
    child_key_columns: Sequence[Any] = (),
) -> dict[str, Any]:
    """Anti-join child against parent. ``child_columns`` / ``parent_columns`` are ColumnElements.

    ``child_key_columns`` (the child's own key) adds ``child_examples`` as
    ``"<child key> -> <missing parent key>"`` so the evidence names the row.

    ``match`` is the catalog fact. Empty scans MATCH SIMPLE and does not
    claim the catalog named that type.
    """
    import sqlalchemy as sa

    refusal = match_scan_refusal(match)
    kind = normalize_match(match)
    if refusal:
        return {
            "available": False,
            "orphan_count": 0,
            "examples": [],
            "match": kind,
            "reason": refusal,
        }
    if any(c is None for c in child_columns) or any(p is None for p in parent_columns):
        return {"available": False, "reason": "join column missing from catalog"}
    if len(child_columns) != len(parent_columns) or not child_columns:
        return {"available": False, "reason": "relationship has no usable column pairing"}

    effective = kind or "simple"
    predicates = match_full_predicates if effective == "full" else match_simple_predicates
    on_clause, where = predicates(child_columns, parent_columns)
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
    out = {
        "available": True,
        "orphan_count": count,
        "examples": examples,
        "match": effective,
        "match_reported": kind in {"simple", "full"},
    }
    keys = [k for k in child_key_columns if k is not None]
    if count and keys:
        width = len(keys)
        out["child_examples"] = [
            f"{orphan_example_text(row[:width])} -> {orphan_example_text(row[width:])}"
            for row in conn.execute(
                sa.select(*keys, *child_columns)
                .select_from(joined)
                .where(where)
                .limit(max_examples)
            ).fetchall()
        ]
    return out


def _split_table(qualified: str, default_schema: str | None) -> tuple[str | None, str]:
    from connectors.sql_identifiers import split_qualified_table

    return split_qualified_table(qualified, default_schema)


SELF_REF_PARENT_ALIAS = "df_fk_parent"


def _quoted_table(name: str, columns: Sequence[str], schema: str | None):
    """Bind columns to a table so same-named keys do not join a column to itself.

    Identifiers are quoted so reserved names (``order``, ``user``) and mixed
    case survive dialect folding.
    """
    import sqlalchemy as sa

    def q(ident: str):
        return sa.quoted_name(str(ident), quote=True)

    schema_q = q(schema) if schema else None
    return sa.table(q(name), *[sa.column(q(c)) for c in columns], schema=schema_q)


def _table_col(table: Any, name: str) -> Any:
    """Resolve a column by wire name after quoting / aliasing."""
    cols = table.c
    if name in cols:
        return cols[name]
    want = str(name)
    for key in cols.keys():
        if str(key) == want:
            return cols[key]
    raise KeyError(name)


def alias_parent_if_self_ref(child: Any, parent: Any) -> Any:
    """Self-referential FK: parent must be a distinct FROM alias.

    MATCH SIMPLE is ``child LEFT JOIN parent ON … WHERE parent.key IS NULL``.
    When both sides are the same relation, unaliased SQL is
    ``FROM emp LEFT JOIN emp`` and every column is ambiguous (SQLite
    OperationalError; Postgres/MySQL error). Dest post-write RI uses the
    same helper so source preflight and dest scan cannot diverge.
    """
    if child is None or parent is None:
        return parent
    if child is parent:
        return parent.alias(SELF_REF_PARENT_ALIAS)
    child_name = str(getattr(child, "name", "") or "")
    parent_name = str(getattr(parent, "name", "") or "")
    child_schema = str(getattr(child, "schema", None) or "")
    parent_schema = str(getattr(parent, "schema", None) or "")
    if child_name and parent_name and child_name == parent_name and child_schema == parent_schema:
        return parent.alias(SELF_REF_PARENT_ALIAS)
    return parent


def sql_population_orphan_scan(
    cfg: dict[str, Any],
    *,
    child_table: str,
    parent_table: str,
    child_columns: Sequence[str],
    parent_columns: Sequence[str],
    max_examples: int = 25,
    match: str = "",
) -> dict[str, Any]:
    """Full-table anti-join using bound, quoted identifiers and the catalog match."""
    refusal = match_scan_refusal(match)
    if refusal:
        return {
            "available": False,
            "orphan_count": 0,
            "examples": [],
            "match": normalize_match(match),
            "reason": refusal,
        }
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

    child = _quoted_table(child_name, child_cols, child_schema)
    parent = alias_parent_if_self_ref(
        child, _quoted_table(parent_name, parent_cols, parent_schema)
    )
    c_els = [_table_col(child, c) for c in child_cols]
    p_els = [_table_col(parent, p) for p in parent_cols]

    engine = _engine(cfg)
    with engine.connect() as conn:
        scan = scan_orphan_anti_join(
            conn,
            child=child,
            child_columns=c_els,
            parent=parent,
            parent_columns=p_els,
            max_examples=max_examples,
            match=match,
        )
    if not scan.get("available"):
        return scan
    return scan


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
    tbl = _quoted_table(table_name, parent_cols, schema)
    p_els = [_table_col(tbl, c) for c in parent_cols]
    found: list[tuple[Any, ...]] = []
    engine = _engine(cfg)
    with engine.connect() as conn:
        for i in range(0, len(rows), PARENT_IN_CHUNK):
            chunk = rows[i : i + PARENT_IN_CHUNK]
            stmt = sa.select(*p_els).select_from(tbl).where(_tuple_in_clause(p_els, chunk))
            for rec in conn.execute(stmt).fetchall():
                found.append(tuple(rec))
    return found


def partition_fk_tuples(
    sample_rows: list[dict[str, Any]] | None,
    columns: Sequence[str],
    *,
    present_key,
    match: str = "",
    limit: int = 500,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    """``(tuples to look up, MATCH FULL partial-NULL violations)``.

    ``present_key`` is the same cell presenter the sample probe already uses.
    """
    cols = [str(c).strip() for c in columns if str(c).strip()]
    if not sample_rows or not cols:
        return [], []
    seen_check: set[tuple[Any, ...]] = set()
    seen_violation: set[tuple[Any, ...]] = set()
    to_check: list[tuple[Any, ...]] = []
    violations: list[tuple[Any, ...]] = []
    for row in sample_rows:
        if not isinstance(row, dict):
            continue
        raw = tuple(row.get(c) for c in cols)
        keys = tuple(present_key(v) for v in raw)
        role = tuple_match_class(keys, match)
        if role == "skip":
            continue
        if role == "violation":
            if keys in seen_violation:
                continue
            seen_violation.add(keys)
            violations.append(raw)
        else:
            if keys in seen_check:
                continue
            seen_check.add(keys)
            to_check.append(raw)
        if len(to_check) + len(violations) >= limit:
            break
    return to_check, violations


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
    to_check, _violations = partition_fk_tuples(
        sample_rows, columns, present_key=present_key, match="", limit=limit
    )
    return to_check


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
