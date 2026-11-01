"""Shared pg/mysql pair helpers — the connect and identifier wrappers the
pairwise copy modules used to each carry their own copy of.

One owner per concern (AGENTS/enterprise rule): ``mysql_connect`` and
``pg_connect`` both open write-mode connections with autocommit off so the
pair fast path can commit or roll back a batch range atomically; the ident
quoters delegate to the canonical ``quote_sql_identifier``.
"""

from __future__ import annotations

from typing import Any, Callable


def mysql_ident(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier

    return quote_sql_identifier(name, "`")


def pg_ident(name: str) -> str:
    from connectors.sql_identifiers import (
        quote_sql_identifier,
        require_safe_identifier,
    )

    return quote_sql_identifier(require_safe_identifier(name, preserve_case=True))


def mysql_write_conn(cfg: dict[str, Any]) -> Any:
    from connectors.mysql_conn import get_connection as mysql_connect

    conn = mysql_connect(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 3306),
        database=cfg.get("database", ""),
        username=cfg.get("username") or cfg.get("user") or "",
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
        purpose="write",
    )
    conn.autocommit = False
    return conn


def pg_write_conn(cfg: dict[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection as pg_connect

    conn = pg_connect(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 5432),
        database=cfg.get("database") or cfg.get("dbname") or "",
        username=cfg.get("username") or cfg.get("user") or "",
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
    )
    conn.autocommit = False
    return conn


# --- PK range-cut helpers (canonical owner; re-exported by copy_pg_mysql) ---


def key_ranges_from_cuts(cuts: list[Any]) -> list[tuple[Any | None, Any | None]]:
    """Equal-height ranges from interior cuts: ``[None, c0), [c0, c1), … [cN, None)``."""
    uniq: list[Any] = []
    for cut in cuts:
        if cut is None:
            continue
        if not uniq or uniq[-1] != cut:
            uniq.append(cut)
    if not uniq:
        return [(None, None)]
    ranges: list[tuple[Any | None, Any | None]] = [(None, uniq[0])]
    for i in range(len(uniq) - 1):
        ranges.append((uniq[i], uniq[i + 1]))
    ranges.append((uniq[-1], None))
    return ranges


def integer_pk_cuts(lo: int, hi: int, workers: int) -> list[int]:
    """Interior cuts on a closed integer interval ``[lo, hi]`` (Spark JDBC style)."""
    n = max(int(workers or 1), 1)
    if n <= 1:
        return []
    width = int(hi) - int(lo) + 1
    if width <= 1:
        return []
    n = min(n, width)
    cuts: list[int] = []
    for i in range(1, n):
        cut = int(lo) + (i * width) // n
        if cut <= int(lo) or cut > int(hi):
            continue
        if not cuts or cut != cuts[-1]:
            cuts.append(cut)
    return cuts


def pk_range_predicate(ident: str, lo: Any, hi: Any, *, null_shard: bool = False) -> str:
    if null_shard:
        return f"{ident} IS NULL"
    parts: list[str] = []
    if lo is not None:
        parts.append(f"{ident} >= {lo}")
    if hi is not None:
        parts.append(f"{ident} < {hi}")
    return " AND ".join(parts)


def mysql_pk_range_clause(
    ident: str, lo: Any, hi: Any, *, null_shard: bool = False
) -> tuple[str, list[Any]]:
    if null_shard:
        return f"{ident} IS NULL", []
    parts: list[str] = []
    params: list[Any] = []
    if lo is not None:
        parts.append(f"{ident} >= %s")
        params.append(lo)
    if hi is not None:
        parts.append(f"{ident} < %s")
        params.append(hi)
    if not parts:
        return "1=1", []
    return " AND ".join(parts), params


def plan_pk_range_partitions(
    src_cur: Any,
    source_ref: str,
    src_ident: str,
    cuts: list[Any],
    source_count: int,
    render_predicate: Callable[[Any, Any], str],
) -> list[dict[str, Any]]:
    """PK range-partition plan shared by the pg↔mysql bulk paths.

    ``cuts`` are interior cut values already sampled by the caller's dialect
    helper (empty when the table does not parallelise).  The plan is a
    leading ``IS NULL`` shard when the key column holds NULLs, then one
    partition per range from ``key_ranges_from_cuts``.  ``render_predicate``
    turns ``(lo, hi)`` bounds into SQL text for the source dialect; an empty
    or ``1=1`` predicate renders the unbounded partition.  Each partition's
    ``source_count`` is the real source COUNT(*) for its predicate and the
    counts must re-add to the caller's ``source_count`` snapshot — a
    mismatch means the table changed under the plan, so we fail closed.
    """
    key_ranges = key_ranges_from_cuts(cuts or [])
    src_cur.execute(
        f"SELECT COUNT(*) FROM {source_ref} "  # nosec B608
        f"WHERE {src_ident} IS NULL"
    )
    nulls = int(src_cur.fetchone()[0])
    unbounded = len(key_ranges) == 1 and key_ranges[0] == (None, None)
    plan: list[tuple[str, Any, Any, bool]] = []
    if nulls and not unbounded:
        plan.append((f"{src_ident} IS NULL", None, None, True))
    for lo, hi in key_ranges:
        plan.append((render_predicate(lo, hi), lo, hi, False))
    partitions: list[dict[str, Any]] = []
    for pred, lo, hi, is_null in plan:
        if pred and pred != "1=1":
            src_cur.execute(
                f"SELECT COUNT(*) FROM {source_ref} WHERE {pred}"  # nosec B608
            )
        else:
            src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
            pred = ""
        partitions.append({
            "lo": lo,
            "hi": hi,
            "null_shard": is_null,
            "source_count": int(src_cur.fetchone()[0]),
            "predicate": pred,
            "action": "load",
        })
    accounted = sum(int(p["source_count"]) for p in partitions)
    if accounted != source_count:
        raise ValueError(
            f"PK range source COUNTs {accounted} != snapshot {source_count}"
        )
    return partitions
