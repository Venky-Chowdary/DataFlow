"""Shared keyset (seek) pagination — transfer + CDC SSOT (Phase F2).

Lexicographic successor predicates and bookmark codecs live here so PostgreSQL,
MySQL, SQL Server, Oracle, and CDC incremental snapshots cannot disagree on
composite primary-key page boundaries.

SQL Server refuses row-value ``(a,b) > (x,y)``; every dialect uses the portable
OR/AND expansion produced by :func:`keyset_successor_predicate`.
"""

from __future__ import annotations

from typing import Any, Sequence

# Unit separator — stable for composite bookmarks (CDC + transfer).
KEYSET_SEP = "\x1f"
# Legacy transfer watermark for 2-col ``cursor|pk`` (pre-F2).
_LEGACY_PIPE_SEP = "|"


def keyset_successor_predicate(
    quoted_pk_columns: Sequence[str],
    last_pk: str,
    placeholder: str = "%s",
) -> tuple[str, list[Any]]:
    """Build a strict lexicographic ``> last_pk`` predicate for a chunk read.

    For key columns ``(a, b, c)`` this produces::

        (a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?)

    Raises ``ValueError`` when ``last_pk`` does not carry one part per key
    column — silent arity mismatch would read the wrong range.
    """
    cols = list(quoted_pk_columns)
    if not cols:
        raise ValueError("keyset predicate requires at least one primary key column")
    parts = decode_keyset_bookmark(last_pk, expected_parts=len(cols))
    clauses: list[str] = []
    params: list[Any] = []
    for i, col in enumerate(cols):
        equalities = " AND ".join(f"{cols[j]} = {placeholder}" for j in range(i))
        greater = f"{col} > {placeholder}"
        clauses.append(f"({greater})" if not equalities else f"({equalities} AND {greater})")
        params.extend(parts[:i])
        params.append(parts[i])
    return " OR ".join(clauses), params


def encode_keyset_bookmark(parts: Sequence[Any]) -> str:
    """Encode ordered key parts into a bookmark string."""
    vals = ["" if p is None else str(p) for p in parts]
    if not vals:
        raise ValueError("keyset bookmark requires at least one part")
    if len(vals) == 1:
        return vals[0]
    return KEYSET_SEP.join(vals)


def decode_keyset_bookmark(bookmark: str, *, expected_parts: int) -> list[str]:
    """Decode a bookmark into ``expected_parts`` string parts.

    Accepts ``KEYSET_SEP`` (preferred) and legacy ``cursor|pk`` for 2-col.
    """
    if expected_parts < 1:
        raise ValueError("expected_parts must be >= 1")
    raw = "" if bookmark is None else str(bookmark)
    if expected_parts == 1:
        return [raw]
    if KEYSET_SEP in raw:
        parts = raw.split(KEYSET_SEP)
    elif expected_parts == 2 and _LEGACY_PIPE_SEP in raw:
        # Only split on the first pipe — PK values may contain '|'.
        left, right = raw.split(_LEGACY_PIPE_SEP, 1)
        parts = [left, right]
    else:
        parts = [raw]
    if len(parts) != expected_parts:
        raise ValueError(
            f"composite keyset bookmark arity mismatch: expected {expected_parts} "
            f"parts, got {len(parts)}"
        )
    return parts


def max_keyset_bookmark(
    rows: list[list[Any]],
    headers: list[str],
    key_columns: Sequence[str],
) -> str | None:
    """Maximum lexicographic bookmark for ``key_columns`` over a page of rows."""
    cols = [c for c in key_columns if c]
    if not cols or not rows:
        return None
    try:
        idxs = [headers.index(c) for c in cols]
    except ValueError:
        return None

    best_parts: list[str] | None = None
    for row in rows:
        parts: list[str] = []
        skip = False
        for i in idxs:
            if i >= len(row) or row[i] is None or str(row[i]) == "":
                skip = True
                break
            parts.append(str(row[i]))
        if skip:
            continue
        if best_parts is None or parts > best_parts:
            best_parts = parts
    if best_parts is None:
        return None
    return encode_keyset_bookmark(best_parts)


def sqlalchemy_keyset_clause(
    sa: Any,
    columns: Sequence[Any],
    bookmark: str,
) -> Any:
    """SQLAlchemy OR/AND clause for ``columns > bookmark`` (MSSQL-safe)."""
    cols = list(columns)
    if not cols:
        raise ValueError("keyset clause requires columns")
    parts = decode_keyset_bookmark(bookmark, expected_parts=len(cols))
    or_terms: list[Any] = []
    for i, col in enumerate(cols):
        marker = sa.cast(sa.literal(parts[i]), col.type)
        eqs = [cols[j] == sa.cast(sa.literal(parts[j]), cols[j].type) for j in range(i)]
        greater = col > marker
        or_terms.append(greater if not eqs else sa.and_(*eqs, greater))
    return sa.or_(*or_terms) if len(or_terms) > 1 else or_terms[0]


# Engines that may use transfer keyset when a PK (possibly composite) is known.
KEYSET_CAPABLE_SOURCES = frozenset(
    {
        "postgresql",
        "redshift",
        "mysql",
        "snowflake",
        "mongodb",
        "bigquery",
        "sqlite",
        "generic_sql",
        "sqlserver",
        "oracle",
        # Salesforce SOQL caps OFFSET at 2000 rows — Id seek is the only way to
        # page a large SObject, so keyset is mandatory rather than an optimization.
        "salesforce",
    }
)
