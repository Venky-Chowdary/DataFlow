"""CDC initial-snapshot resume — Debezium-class PK seek (not OFFSET).

Fresh dumps keep one SELECT + fetchmany. A crash mid-dump must not restart
from row 0 (duplicates + skipped tail under concurrent writes) and must not
page with ``OFFSET`` / ``ROW_NUMBER`` (O(n²), insert/delete drift).

Resume classification is shared so MySQL binlog, SQL Server CT/native, Oracle
flashback, and PostgreSQL logical decoding cannot disagree:

* ``last_pk`` present → lexicographic keyset successor (F2 SSOT)
* else legacy ``offset > 0`` → OFFSET / ROW_NUMBER (old tokens only)
* else → held cursor scan

Streaming watermarks (binlog file/pos/GTID, LSN, SCN, CT version) stay on the
token. ``last_pk`` is snapshot progress only and is cleared on streaming handoff.
"""

from __future__ import annotations

from typing import Any, Literal, Sequence

from services.keyset_pagination import (
    keyset_successor_predicate,
    max_keyset_bookmark,
)

SnapshotResumeMode = Literal["keyset", "offset", "scan"]


def classify_snapshot_resume(*, last_pk: str = "", offset: int = 0) -> SnapshotResumeMode:
    """Pick the snapshot page algorithm from the persisted resume token."""
    if str(last_pk or "").strip():
        return "keyset"
    if int(offset or 0) > 0:
        return "offset"
    return "scan"


def resolve_pk_headers(headers: Sequence[str], pk_columns: Sequence[str]) -> list[str]:
    """Map declared PK names onto result headers (Oracle-style case fold)."""
    lower = {str(h).lower(): h for h in headers}
    resolved: list[str] = []
    for pk in pk_columns:
        hit = lower.get(str(pk).lower())
        if hit is None:
            return []
        resolved.append(hit)
    return resolved


def last_pk_from_records(
    records: list[dict[str, Any]],
    pk_columns: Sequence[str],
) -> str:
    """Column-order page maximum — never last-row or text-max (``99`` < ``200``)."""
    if not records or not pk_columns:
        return ""
    headers = [str(h) for h in records[0].keys()]
    resolved = resolve_pk_headers(headers, pk_columns)
    if not resolved:
        return ""
    rows = [[rec.get(h) for h in headers] for rec in records]
    return max_keyset_bookmark(rows, headers, resolved) or ""


def quoted_pk_columns(pk_columns: Sequence[str], quote_char: str) -> list[str]:
    """Quote declared PK columns for a keyset predicate / ORDER BY."""
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    return [
        quote_sql_identifier(require_safe_identifier(str(c), preserve_case=True), quote_char)
        for c in pk_columns
        if str(c).strip()
    ]


def _oracle_named_binds(
    where: str, params: list[Any], extra: dict[str, Any] | None = None
) -> tuple[str, dict[str, Any]]:
    named: dict[str, Any] = {}
    pieces: list[str] = []
    parts = where.split("%s")
    for i, part in enumerate(parts):
        pieces.append(part)
        if i < len(params):
            key = f"k{i}"
            named[key] = params[i]
            pieces.append(f":{key}")
    named.update(extra or {})
    return "".join(pieces), named


def snapshot_keyset_sql(
    *,
    table_ref: str,
    quoted_pk_columns: Sequence[str],
    last_pk: str,
    limit: int,
    dialect: str,
    select_list: str = "*",
) -> tuple[str, list[Any] | dict[str, Any]]:
    """``(sql, params)`` for one PK-seek snapshot page.

    Identifiers must already be quoted. ``last_pk`` and ``limit`` are binds
    (except SQL Server ``TOP (n)`` / Oracle ``ROWNUM``, which take ``int(limit)``).
    """
    cols = list(quoted_pk_columns)
    if not cols:
        raise ValueError("snapshot keyset SQL requires quoted primary-key columns")
    n = max(1, int(limit))
    order_sql = ", ".join(cols)
    dialect = (dialect or "").strip().lower()
    if dialect == "oracle":
        where, params = keyset_successor_predicate(cols, last_pk, placeholder="%s")
        where_sql, binds = _oracle_named_binds(where, params, extra={"lim": n})
        sql = (
            f"SELECT * FROM ("  # nosec B608
            f"SELECT {select_list} FROM {table_ref} WHERE {where_sql} "
            f"ORDER BY {order_sql}"
            f") WHERE ROWNUM <= :lim"
        )
        return sql, binds
    if dialect in {"sqlserver", "mssql"}:
        where, params = keyset_successor_predicate(cols, last_pk, placeholder="%s")
        sql = (
            f"SELECT TOP ({n}) {select_list} FROM {table_ref} "  # nosec B608
            f"WHERE {where} ORDER BY {order_sql}"
        )
        return sql, params
    where, params = keyset_successor_predicate(cols, last_pk, placeholder="%s")
    sql = (
        f"SELECT {select_list} FROM {table_ref} WHERE {where} "  # nosec B608
        f"ORDER BY {order_sql} LIMIT %s"
    )
    return sql, [*params, n]


def streaming_handoff_fields(token: dict[str, Any] | None) -> dict[str, Any]:
    """Copy stream watermarks off a mid-snapshot token. Never invent a new tip."""
    if not isinstance(token, dict):
        return {}
    out: dict[str, Any] = {}
    for key in ("file", "pos", "gtid", "lsn", "seqval", "capture_instance", "scn", "version"):
        if token.get(key) not in (None, ""):
            out[key] = token[key]
    return out
