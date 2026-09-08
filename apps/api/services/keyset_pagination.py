"""Shared keyset (seek) pagination — transfer + CDC SSOT (Phase F2).

Lexicographic successor predicates and bookmark codecs live here so PostgreSQL,
MySQL, SQL Server, Oracle, and CDC incremental snapshots cannot disagree on
composite primary-key page boundaries.

SQL Server refuses row-value ``(a,b) > (x,y)``; every dialect uses the portable
OR/AND expansion produced by :func:`keyset_successor_predicate`.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

from services.value_serializer import present_cell_text

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
    """Encode ordered key parts into a bookmark string.

    Each part uses :func:`present_cell_text` so ``True`` and dest ``"true"``
    share one token and reader-null cells become empty parts, not the
    ``SQL_NULL_SENTINEL`` spelling. Callers that skip missing rows must do
    so before encode — this function preserves arity.
    """
    vals = [present_cell_text(p) or "" for p in parts]
    if not vals:
        raise ValueError("keyset bookmark requires at least one part")
    if len(vals) == 1:
        return vals[0]
    return KEYSET_SEP.join(vals)


def present_cursor_bookmark(value: Any) -> str | None:
    """Keyset resume token, or None when the cell is absent (first page).

    ``if cursor_after`` dropped integer ``0``. Reader-null sentinels are
    truthy and became ``WHERE col > '__DF_SQL_NULL__'``. Composite
    ``KEYSET_SEP`` bookmarks stay intact. Incremental empty-string
    watermarks are a different polarity — do not use this in CDC coalesce.
    """
    return present_cell_text(value)


def is_cursor_only_bookmark(bookmark: str | None) -> bool:
    """True when ``bookmark`` carries a single cursor value and no tie-break part."""
    raw = "" if bookmark is None else str(bookmark)
    return KEYSET_SEP not in raw and _LEGACY_PIPE_SEP not in raw


def decode_keyset_bookmark(bookmark: str, *, expected_parts: int) -> list[str]:
    """Decode a bookmark into ``expected_parts`` string parts.

    Accepts ``KEYSET_SEP`` (preferred) and legacy ``cursor|pk`` for 2-col.
    """
    if expected_parts < 1:
        raise ValueError("expected_parts must be >= 1")
    raw = "" if bookmark is None else str(bookmark)
    if expected_parts == 1:
        if KEYSET_SEP in raw:
            raise ValueError(
                "composite keyset bookmark arity mismatch: expected 1 part, "
                f"got {raw.count(KEYSET_SEP) + 1}"
            )
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


def split_cursor_bookmark(
    bookmark: str | None,
    *,
    has_tiebreak: bool,
) -> tuple[str, str]:
    """Split an incremental watermark into ``(cursor_value, tiebreak_value)``.

    A composite watermark is only readable by a seek that orders on both the
    cursor column and its tie-break column. Handed to a single-column seek it
    would be bound whole against the cursor column's type, so the engine either
    rejects the statement or compares against a value no row can hold — the poll
    returns nothing and the sync silently stops advancing. Refuse instead.
    """
    raw = "" if bookmark is None else str(bookmark)
    if not has_tiebreak:
        if KEYSET_SEP in raw:
            raise ValueError(
                "composite watermark requires the tie-break column it was "
                "written with; single-column cursor read cannot use it"
            )
        return raw, ""
    parts = raw.split(KEYSET_SEP, 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    if _LEGACY_PIPE_SEP in raw:
        left, right = raw.split(_LEGACY_PIPE_SEP, 1)
        return left, right
    return raw, ""


def _numeric_order_key(value: str) -> Decimal | None:
    """Bind one bookmark part as a number, or ``None`` when it cannot.

    Dest-canonical storage text (``1.234``, ``1.2300``) uses ``Decimal(text)``
    first so Auto wire does not refuse a resolved dest value. Locale money the
    write path stores (``$1,234`` / ``€1.234``) falls through to
    ``decimal_wire_value``. Auto ``1,234`` stays unbound — the column then
    orders as text rather than inventing thousands.
    """
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    try:
        parsed = Decimal(text)
        if parsed.is_finite():
            return parsed
    except (ArithmeticError, InvalidOperation, ValueError):
        pass
    from services.transform_engine import decimal_wire_value

    return decimal_wire_value(text)


def _column_order_keys(values: list[str]) -> list[Any]:
    """Sort keys that order ``values`` the way the source database orders them.

    The seek predicate casts the bookmark back to the column type, so the page
    maximum must be chosen in the column's own ordering. Picking it as text made
    ``'99'`` the maximum of an integer page ending at ``200``: the next page
    seeked from 99, re-read rows already transferred, and — because re-read rows
    are charged against the same row budget — the scan ran out before the tail.

    ``Decimal(v)`` on the whole page also failed closed into text when one cell
    was locale money (``$1,234``), so ``'99'`` beat ``'200'`` again. Bind each
    part the write path would store; if any part cannot bind, keep text for the
    whole column — do not invent a numeric max from Auto ``1,234``.
    """
    keys = [_numeric_order_key(v) for v in values]
    if keys and all(k is not None for k in keys):
        return keys
    return list(values)


def compare_keyset_bookmark(left: str, right: str) -> int | None:
    """Compare two keyset bookmarks in column order.

    Numeric parts use Decimal (so ``99`` < ``200``). Mixed/non-numeric parts
    use text. Returns ``None`` when arity differs or either side is empty —
    callers must not invent ``<=`` / skip from incomparable bookmarks.
    """
    a = "" if left is None else str(left)
    b = "" if right is None else str(right)
    if not a or not b:
        return None
    left_parts = a.split(KEYSET_SEP) if KEYSET_SEP in a else [a]
    right_parts = b.split(KEYSET_SEP) if KEYSET_SEP in b else [b]
    if len(left_parts) != len(right_parts):
        return None
    for lp, rp in zip(left_parts, right_parts):
        keys = _column_order_keys([lp, rp])
        if keys[0] < keys[1]:
            return -1
        if keys[0] > keys[1]:
            return 1
    return 0


def max_keyset_bookmark(
    rows: list[list[Any]],
    headers: list[str],
    key_columns: Sequence[str],
) -> str | None:
    """Maximum bookmark for ``key_columns`` over a page, in column order."""
    cols = [c for c in key_columns if c]
    if not cols or not rows:
        return None
    try:
        idxs = [headers.index(c) for c in cols]
    except ValueError:
        return None

    candidates: list[list[str]] = []
    for row in rows:
        parts: list[str] = []
        skip = False
        for i in idxs:
            if i >= len(row):
                skip = True
                break
            text = present_cell_text(row[i])
            if text is None:
                skip = True
                break
            parts.append(text)
        if skip:
            continue
        candidates.append(parts)
    if not candidates:
        return None

    ordered_columns = [
        _column_order_keys([parts[pos] for parts in candidates])
        for pos in range(len(cols))
    ]
    best = max(
        range(len(candidates)),
        key=lambda r: tuple(col[r] for col in ordered_columns),
    )
    return encode_keyset_bookmark(candidates[best])


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
        # Databricks reads go through generic_sql; a declared PK must seek.
        "databricks",
    }
)


def safe_keyset_unique_columns(
    unique_keys: list[Any] | None,
    columns: list[str],
    nullable: dict[str, bool] | None = None,
) -> list[str]:
    """Unique-key columns that are safe to seek — never a nullable/advisory UK.

    Keyset on a nullable unique index skips NULL/tied rows (silent loss).
    Advisory / NOT ENFORCED keys and expression indexes cannot bookmark.
    Unknown nullability defaults to nullable (fail closed).
    """
    colset = {c for c in (columns or []) if c}
    nulls = nullable or {}
    for uk in unique_keys or []:
        if isinstance(uk, dict):
            if uk.get("enforced") is False:
                continue
            if uk.get("primary"):
                continue
            if uk.get("expression") or uk.get("expression_columns"):
                continue
            uk_cols = [c for c in (uk.get("columns") or []) if c in colset]
        elif isinstance(uk, (list, tuple)):
            uk_cols = [c for c in uk if c in colset]
        else:
            continue
        if not uk_cols:
            continue
        if any(bool(nulls.get(c, True)) for c in uk_cols):
            continue
        return uk_cols
    return []


def cursor_unique_evidence(
    cursor_column: str,
    *,
    primary_key_columns: Sequence[str],
    unique_keys: list[Any] | None,
    nullable: dict[str, bool] | None,
) -> bool:
    """True when the source catalog proves ``cursor_column`` alone is unique.

    A single-column primary key or a single-column enforced, non-nullable
    unique key on the cursor. A composite key that merely *contains* the
    cursor is not evidence — two rows can share the cursor value.
    """
    if not cursor_column:
        return False
    pk = [c for c in primary_key_columns if c]
    if pk == [cursor_column]:
        return True
    for uk in unique_keys or []:
        if isinstance(uk, dict):
            uk_cols = list(uk.get("columns") or [])
        elif isinstance(uk, (list, tuple)):
            uk_cols = list(uk)
        else:
            continue
        if uk_cols != [cursor_column]:
            continue
        if safe_keyset_unique_columns([uk], [cursor_column], nullable) == [cursor_column]:
            return True
    return False


#: Sources whose every row carries an engine-enforced unique identity even when
#: the stream contract declares no primary key. It is the tie-break an
#: incremental cursor seeks on, so a cursor with ties (``updated_seq``,
#: ``updated_at``) pages as ``(cursor, identity)`` instead of skipping peers.
INTRINSIC_ROW_IDENTITY: dict[str, str] = {"mongodb": "_id"}


def intrinsic_tiebreak_column(src_type: str, cursor_column: str) -> str:
    """The engine-guaranteed unique column a cursor read may tie-break on."""
    ident = INTRINSIC_ROW_IDENTITY.get((src_type or "").strip().lower(), "")
    if not ident or ident == cursor_column:
        return ""
    return ident


def incremental_tiebreak_column(
    src_type: str, cursor_column: str, primary_key_columns: Sequence[str]
) -> str:
    """The column an incremental cursor read seeks on beside the cursor.

    One owner for the stream engine, the preflight read scope, and the
    population-fit scan: a composite watermark can only be decoded by a read
    that names the same tie-break column it was written with. Contract primary
    key first; otherwise the source engine's intrinsic row identity.
    """
    cursor = (cursor_column or "").strip()
    if not cursor:
        return ""
    for col in primary_key_columns:
        if col and col != cursor:
            return str(col)
    return intrinsic_tiebreak_column(src_type, cursor)


def incremental_read_needs_filtered_scan(
    *,
    src_type: str,
    incremental: bool,
    cursor_column: str,
    tiebreak_column: str,
    cursor_is_unique: bool,
    callable_source: bool,
) -> str:
    """Why an incremental read must page one filtered snapshot scan, or ``""``.

    The advancing-page-max read (``WHERE cursor > page_max``) and the keyset
    seek both assume a strict order on the seek key. A cursor with no unique
    tie-break has ties, and every peer row past a page edge is lost silently.
    Those sources hold one ``WHERE cursor > run_watermark ORDER BY cursor``
    cursor and page it with ``fetchmany`` instead. A callable source already
    filters its spool against the run watermark and OFFSET-pages it.

    Raises ``ValueError`` when the source can neither tie-break nor hold a
    filtered scan: the only remaining read loses rows, so the run refuses.
    """
    if not incremental or not cursor_column or callable_source:
        return ""
    if tiebreak_column or cursor_is_unique:
        return ""
    from connectors.sql_snapshot_scan import FILTERED_SCAN_SOURCES

    engine = (src_type or "").strip().lower()
    if engine not in FILTERED_SCAN_SOURCES:
        raise ValueError(
            f"{engine or 'this source'} cannot page an incremental read on cursor "
            f"{cursor_column!r} without a unique tie-break: rows sharing a cursor "
            "value past a page edge would be skipped silently. Declare a primary "
            "key or a non-nullable unique column on the stream contract, or run "
            "this sync as full refresh."
        )
    return (
        f"incremental cursor {cursor_column!r} has no unique tie-break "
        "(no primary key or enforced non-null unique key on the source): a read "
        "that seeks past each page's maximum would skip rows sharing that cursor "
        f"value, so this run pages one filtered snapshot scan "
        f"(WHERE {cursor_column} > watermark ORDER BY {cursor_column}) instead"
    )


@dataclass(frozen=True)
class KeysetDecision:
    """How a stream will page, and the ordered columns it will seek on."""

    use_keyset: bool
    order_cols: list[str]
    pagination_mode: str
    resume_fallback: bool
    #: Set when an incremental cursor has no unique tie-break: a strict ``>``
    #: seek on that cursor would skip every peer row sharing a page-edge value.
    seek_refused_reason: str = ""


def decide_keyset_pagination(
    *,
    src_type: str,
    keyset_order_cols: Sequence[str],
    keyset_col: str,
    keyset_tiebreak: str,
    incremental: bool,
    offset: int,
    chunk_index: int,
    cursor_after: Any,
    snapshot_scan: bool,
    cursor_is_unique: bool = False,
) -> KeysetDecision:
    """Choose seek vs scan vs OFFSET paging — one owner for the whole engine.

    Seeking needs unique evidence: without a declared key a strict ``>`` on a
    tied bookmark skips the peers sharing that value, so no evidence means
    scan (one SELECT + fetchmany) when a snapshot is held, else OFFSET
    (quadratic, but it cannot lose rows). An incremental run may seek on
    its declared cursor plus a tie-break instead. A resume that carries a row
    offset but no bookmark also refuses to seek, or the seek would restart at
    the top of the table and re-read rows already committed — the stream then
    drains the held scan past that offset instead of OFFSET-paging.

    An incremental cursor is only seekable when it is unique itself
    (``cursor_is_unique``) or paired with a unique tie-break. A non-unique
    cursor alone (``updated_at``, a batch sequence) is the Airbyte timestamp
    trap: the first page ends inside a run of tied values and ``cursor > edge``
    never returns the rest of that run. Such a run pages the *filtered*
    snapshot scan (``WHERE cursor > watermark`` held on one server cursor)
    instead, and ``seek_refused_reason`` says why.
    """
    order_cols = [c for c in keyset_order_cols if c]
    capable = str(src_type or "") in KEYSET_CAPABLE_SOURCES
    use_keyset = bool(order_cols) and capable
    if not use_keyset and incremental and keyset_col and capable:
        use_keyset = True
        if keyset_col not in order_cols:
            order_cols = [keyset_col] + ([keyset_tiebreak] if keyset_tiebreak else [])
    seek_refused_reason = ""
    if (
        use_keyset
        and incremental
        and keyset_col
        and not keyset_tiebreak
        and not cursor_is_unique
        and [c for c in order_cols if c != keyset_col] == []
    ):
        use_keyset = False
        seek_refused_reason = (
            f"incremental cursor {keyset_col!r} has no unique tie-break "
            "(no primary/unique key on the source): a seek past a page edge "
            "would skip rows sharing that cursor value, so this run pages one "
            f"filtered snapshot scan (WHERE {keyset_col} > watermark) instead"
        )
    resume_fallback = False
    if use_keyset and (offset > 0 or chunk_index > 0) and cursor_after in (None, ""):
        use_keyset = False
        resume_fallback = True
    if seek_refused_reason:
        mode = "filtered_scan"
    else:
        mode = "keyset" if use_keyset else ("scan" if snapshot_scan else "offset")
    return KeysetDecision(
        use_keyset=use_keyset,
        order_cols=order_cols,
        pagination_mode=mode,
        resume_fallback=resume_fallback,
        seek_refused_reason=seek_refused_reason,
    )
