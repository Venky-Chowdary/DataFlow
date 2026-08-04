"""Live query / sample / result-analysis tools for Datawrap Pilot.

Never invent SQL results. Paths:
  saved connector → read-only check → query_router._run_query → result_store
  follow-ups → result_store.resolve → real profile / filter algorithms
"""

from __future__ import annotations

import logging
import math
import re
from collections import Counter
from datetime import datetime
from typing import Any

from .schema_tools import AmbiguousConnectorError, _safe_connector, list_connector_objects

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")
_DEFAULT_SAMPLE = 25
_MAX_SAMPLE = 100
_MAX_QUERY_ROWS = 200
_BOOL_TRUE = {"true", "t", "yes", "y", "1"}
_BOOL_FALSE = {"false", "f", "no", "n", "0"}
_DATE_HINTS = (
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%SZ",
    "%m/%d/%Y",
    "%d/%m/%Y",
)
_MISSING_TABLE_RE = re.compile(
    r"(?:undefinedtable|does not exist|doesn't exist|unknown relation|"
    r"no such table|invalid object name|relation\s+[\"'].+[\"']\s+does not exist|"
    r"table .+ (?:doesn't|does not) exist)",
    re.I,
)


def _object_names_from_list(listed: Any) -> list[str]:
    objs = []
    if getattr(listed, "success", False) and isinstance(listed.output, dict):
        objs = listed.output.get("objects") or listed.output.get("tables") or []
    names: list[str] = []
    for o in objs:
        if isinstance(o, dict):
            n = o.get("name") or o.get("table") or o.get("id")
            if n:
                names.append(str(n))
        elif isinstance(o, str):
            names.append(o)
    return names


def resolve_table_name(
    conn: dict[str, Any],
    table: str,
    *,
    connector_name: str = "",
) -> tuple[str | None, str | None, list[str]]:
    """Exact / case-insensitive / fuzzy table resolve against live inventory.

    Returns ``(resolved_name, note, candidates)``.
    ``resolved_name`` None means ask the operator (candidates listed).
    """
    import difflib

    wanted = (table or "").strip()
    if not wanted:
        return None, None, []
    listed = list_connector_objects(
        connector_id=str(conn.get("id") or conn.get("_id") or ""),
        connector_name=str(conn.get("name") or connector_name or ""),
    )
    names = _object_names_from_list(listed)
    if not names:
        return wanted, None, []
    lower_map = {n.lower(): n for n in names}
    if wanted in names:
        return wanted, None, names
    if wanted.lower() in lower_map:
        resolved = lower_map[wanted.lower()]
        note = f"Using `{resolved}` (matched case-insensitively)." if resolved != wanted else None
        return resolved, note, names
    # Unqualified vs schema.table
    bare = wanted.split(".")[-1].lower()
    bare_hits = [n for n in names if n.split(".")[-1].lower() == bare]
    if len(bare_hits) == 1:
        return bare_hits[0], f"Using `{bare_hits[0]}`.", names
    close = difflib.get_close_matches(wanted.lower(), list(lower_map.keys()), n=5, cutoff=0.72)
    if len(close) == 1:
        resolved = lower_map[close[0]]
        return resolved, f"Using `{resolved}` (closest match to `{wanted}`).", names
    if close:
        return None, None, [lower_map[c] for c in close]
    return None, None, names[:12]


def _tool_result(name: str, *, success: bool, output: Any = None, error: str = ""):
    from .tools import ToolResult

    return ToolResult(name=name, success=success, output=output, error=error)


def _quote_ident(name: str, dialect: str) -> str:
    parts = name.split(".")
    d = (dialect or "").lower()
    if d in {"mysql", "mariadb", "tidb"}:
        return ".".join(f"`{p}`" for p in parts)
    if d in {"mssql", "sqlserver"}:
        return ".".join(f"[{p}]" for p in parts)
    return ".".join(f'"{p}"' for p in parts)


def _sample_sql(table: str, dialect: str, limit: int) -> str:
    quoted = _quote_ident(table, dialect)
    d = (dialect or "").lower()
    if d in {"mssql", "sqlserver"}:
        return f"SELECT TOP {int(limit)} * FROM {quoted}"
    return f"SELECT * FROM {quoted} LIMIT {int(limit)}"


def _is_nullish(v: Any) -> bool:
    return v is None or v == ""


def _try_float(v: Any) -> float | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace(",", "")
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _try_bool(v: Any) -> bool | None:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and v in (0, 1):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in _BOOL_TRUE:
            return True
        if s in _BOOL_FALSE:
            return False
    return None


def _try_datetime(v: Any) -> bool:
    if isinstance(v, datetime):
        return True
    if not isinstance(v, str):
        return False
    s = v.strip()
    if len(s) < 8:
        return False
    for fmt in _DATE_HINTS:
        try:
            datetime.strptime(s[:26].rstrip("Z"), fmt.rstrip("Z"))
            return True
        except ValueError:
            continue
    # ISO-ish
    if "T" in s and len(s) >= 10:
        try:
            datetime.fromisoformat(s.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False
    return False


def _infer_kind(non_null: list[Any]) -> str:
    if not non_null:
        return "empty"
    n = len(non_null)
    floats = sum(1 for v in non_null if _try_float(v) is not None)
    ints = sum(
        1
        for v in non_null
        if (f := _try_float(v)) is not None and float(f).is_integer()
    )
    bools = sum(1 for v in non_null if _try_bool(v) is not None)
    dates = sum(1 for v in non_null if _try_datetime(v))
    threshold = max(1, int(math.ceil(n * 0.8)))
    if bools >= threshold and floats < threshold:
        return "boolean"
    if ints >= threshold:
        return "integer"
    if floats >= threshold:
        return "number"
    if dates >= threshold:
        return "datetime"
    return "string"


def _numeric_stats(vals: list[float]) -> dict[str, Any]:
    if not vals:
        return {}
    n = len(vals)
    s = sum(vals)
    mean = s / n
    var = sum((x - mean) ** 2 for x in vals) / n
    ordered = sorted(vals)
    mid = n // 2
    if n % 2:
        p50 = ordered[mid]
    else:
        p50 = (ordered[mid - 1] + ordered[mid]) / 2
    return {
        "min": ordered[0],
        "max": ordered[-1],
        "mean": round(mean, 6),
        "stdev": round(math.sqrt(var), 6),
        "p50": p50,
    }


def _analyze_rows(rows: list[dict[str, Any]], columns: list[str]) -> dict[str, Any]:
    """Column profile from real sampled values — no invented cells."""
    profiles: list[dict[str, Any]] = []
    n_rows = len(rows)
    for col in columns[:40]:
        vals = [r.get(col) for r in rows]
        non_null = [v for v in vals if not _is_nullish(v)]
        nulls = n_rows - len(non_null)
        kind = _infer_kind(non_null)
        distinct = len({str(v) for v in non_null})
        # Top values by frequency
        top: list[dict[str, Any]] = []
        if non_null:
            counts = Counter(str(v) if not isinstance(v, (int, float, bool)) else v for v in non_null)
            for val, cnt in counts.most_common(5):
                s = str(val)
                if len(s) > 80:
                    s = s[:77] + "…"
                top.append({"value": s, "count": cnt})
        # Examples (first distinct)
        sample: list[str] = []
        seen: set[str] = set()
        for v in non_null:
            s = str(v)
            if len(s) > 80:
                s = s[:77] + "…"
            if s in seen:
                continue
            seen.add(s)
            sample.append(s)
            if len(sample) >= 3:
                break

        col_prof: dict[str, Any] = {
            "column": col,
            "non_null": len(non_null),
            "nulls": nulls,
            "null_rate": round(nulls / n_rows, 4) if n_rows else 0.0,
            "distinct_in_sample": distinct,
            "cardinality_ratio": round(distinct / len(non_null), 4) if non_null else 0.0,
            "inferred_kind": kind,
            "examples": sample,
            "top_values": top,
        }
        if kind in {"integer", "number"}:
            nums = [f for v in non_null if (f := _try_float(v)) is not None]
            col_prof["numeric"] = _numeric_stats(nums)
        if kind == "string":
            lengths = [len(str(v)) for v in non_null]
            if lengths:
                col_prof["string"] = {
                    "min_len": min(lengths),
                    "max_len": max(lengths),
                    "avg_len": round(sum(lengths) / len(lengths), 2),
                }
        if kind == "boolean":
            truths = sum(1 for v in non_null if _try_bool(v) is True)
            col_prof["boolean"] = {
                "true": truths,
                "false": len(non_null) - truths,
            }
        profiles.append(col_prof)

    null_heavy = [p["column"] for p in profiles if p["null_rate"] >= 0.25]
    high_card = [
        p["column"]
        for p in profiles
        if p["cardinality_ratio"] >= 0.9 and p["non_null"] >= 5
    ]
    return {
        "row_count_sampled": n_rows,
        "column_count": len(columns),
        "columns": profiles,
        "signals": {
            "null_heavy_columns": null_heavy[:12],
            "high_cardinality_columns": high_card[:12],
        },
    }


def _store_result(
    *,
    rows: list[dict[str, Any]],
    columns: list[str],
    column_schema: Any,
    meta: dict[str, Any],
    session_id: str = "",
    source: str = "",
) -> str:
    from .result_store import get_result_store

    schema = column_schema if isinstance(column_schema, dict) else {}
    return get_result_store().put(
        rows=rows,
        columns=columns,
        column_schema=schema,
        meta=meta,
        session_id=session_id,
        source=source,
    )


def sample_connector_object(
    connector_id: str = "",
    connector_name: str = "",
    table: str = "",
    limit: int = _DEFAULT_SAMPLE,
    analyze: bool = True,
    session_id: str = "",
):
    """Sample rows from a saved-connector table/collection (read-only)."""
    tool = "sample_connector_object"
    table = (table or "").strip()
    if not table or not _SAFE_IDENT.match(table):
        return _tool_result(
            tool,
            success=False,
            error=(
                "Provide a simple table/collection name "
                "(letters, numbers, underscore, optional schema.table)."
            ),
        )
    limit = max(1, min(int(limit or _DEFAULT_SAMPLE), _MAX_SAMPLE))
    conn, err = _safe_connector(connector_id, connector_name, tool)
    if err:
        return err

    ctype = str(conn.get("type") or conn.get("format") or "").lower()
    cid = str(conn.get("id") or conn.get("_id") or "")
    resolve_note = None
    try:
        from services.connector_store import get_connector
        from src.routers.query_router import QueryExecuteRequest, _run_query

        saved = get_connector(cid)
        if not saved:
            return _tool_result(tool, success=False, error="Connector not found in store.")

        resolved, resolve_note, candidates = resolve_table_name(
            conn, table, connector_name=str(conn.get("name") or connector_name or ""),
        )
        if resolved is None:
            shown = ", ".join(f"`{n}`" for n in (candidates or [])[:12])
            more = f" (+{len(candidates) - 12} more)" if candidates and len(candidates) > 12 else ""
            label = conn.get("name") or "this connector"
            if candidates:
                return _tool_result(
                    tool,
                    success=False,
                    error=(
                        f"No table `{table}` on **{label}**. "
                        f"Did you mean {shown}{more}? "
                        f'Example: "sample {candidates[0]} on {label}".'
                    ),
                )
            return _tool_result(
                tool,
                success=False,
                error=(
                    f"No table `{table}` on **{label}**, and I could not list objects. "
                    f'Ask "list tables on {label}".'
                ),
            )
        table = resolved

        if ctype == "mongodb":
            body = QueryExecuteRequest(
                connector_id=cid,
                query="{}",
                collection=table,
                limit=limit,
            )
        else:
            body = QueryExecuteRequest(
                connector_id=cid,
                query=_sample_sql(table, ctype, limit),
                limit=limit,
            )
        rows, columns, schema, truncated = _run_query(saved, body)
        preview_rows = rows[: min(limit, 25)]
        meta = {
            "connector_id": cid,
            "connector_name": conn.get("name"),
            "type": ctype,
            "table": table,
            "limit": limit,
            "truncated": truncated or len(rows) >= limit,
        }
        if resolve_note:
            meta["resolve_note"] = resolve_note
        result_id = _store_result(
            rows=rows,
            columns=columns,
            column_schema=schema,
            meta=meta,
            session_id=session_id,
            source=tool,
        )
        out: dict[str, Any] = {
            **meta,
            "result_id": result_id,
            "columns": columns,
            "column_schema": schema,
            "row_count": len(rows),
            "rows": preview_rows,
            "read_only": True,
        }
        if analyze and rows:
            out["analysis"] = _analyze_rows(rows, columns)
        return _tool_result(tool, success=True, output=out)
    except AmbiguousConnectorError as exc:
        return _tool_result(tool, success=False, error=exc.message)
    except Exception as exc:
        logging.getLogger(__name__).warning("sample_connector_object failed: %s", exc, exc_info=True)
        msg = str(exc)
        if _MISSING_TABLE_RE.search(msg):
            try:
                _, _, candidates = resolve_table_name(
                    conn, table, connector_name=str(conn.get("name") or connector_name or ""),
                )
            except Exception:
                candidates = []
            label = conn.get("name") or "this connector"
            if candidates:
                shown = ", ".join(f"`{n}`" for n in candidates[:12])
                return _tool_result(
                    tool,
                    success=False,
                    error=(
                        f"No table `{table}` on **{label}**. "
                        f"Which table? {shown}. "
                        f'Example: "sample {candidates[0]} on {label}".'
                    ),
                )
            return _tool_result(
                tool,
                success=False,
                error=(
                    f"No table `{table}` on **{label}**. "
                    f'Ask "list tables on {label}" to see what is available.'
                ),
            )
        return _tool_result(tool, success=False, error=f"Sample failed: {exc}")


def run_connector_query(
    connector_id: str = "",
    connector_name: str = "",
    query: str = "",
    collection: str = "",
    limit: int = _MAX_QUERY_ROWS,
    analyze: bool = False,
    session_id: str = "",
):
    """Execute a read-only SQL / Mongo query on a saved connector."""
    tool = "run_query"
    sql = (query or "").strip()
    if not sql:
        return _tool_result(tool, success=False, error="Provide a read-only SQL SELECT (or Mongo JSON filter).")

    conn, err = _safe_connector(connector_id, connector_name, tool)
    if err:
        return err

    ctype = str(conn.get("type") or conn.get("format") or "").lower()
    cid = str(conn.get("id") or conn.get("_id") or "")
    limit = max(1, min(int(limit or _MAX_QUERY_ROWS), _MAX_QUERY_ROWS))

    try:
        from services.connector_store import get_connector
        from src.routers.query_router import QueryExecuteRequest, _is_safe_sql, _run_query

        saved = get_connector(cid)
        if not saved:
            return _tool_result(tool, success=False, error="Connector not found in store.")

        if ctype == "mongodb":
            body = QueryExecuteRequest(
                connector_id=cid,
                query=sql,
                collection=(collection or "").strip(),
                limit=limit,
            )
        else:
            if not _is_safe_sql(sql):
                return _tool_result(
                    tool,
                    success=False,
                    error="Only read-only SQL is allowed (SELECT / WITH / SHOW / DESCRIBE / EXPLAIN).",
                )
            body = QueryExecuteRequest(connector_id=cid, query=sql, limit=limit)

        rows, columns, schema, truncated = _run_query(saved, body)
        preview_rows = rows[: min(25, len(rows))]
        meta = {
            "connector_id": cid,
            "connector_name": conn.get("name"),
            "type": ctype,
            "query": sql,
            "collection": collection or None,
            "limit": limit,
            "truncated": truncated,
        }
        result_id = _store_result(
            rows=rows,
            columns=columns,
            column_schema=schema,
            meta=meta,
            session_id=session_id,
            source=tool,
        )
        out: dict[str, Any] = {
            **meta,
            "result_id": result_id,
            "columns": columns,
            "column_schema": schema,
            "row_count": len(rows),
            "rows": preview_rows,
            "read_only": True,
        }
        if analyze and rows:
            out["analysis"] = _analyze_rows(rows, columns)
        return _tool_result(tool, success=True, output=out)
    except AmbiguousConnectorError as exc:
        return _tool_result(tool, success=False, error=exc.message)
    except Exception as exc:
        logging.getLogger(__name__).warning("run_query failed: %s", exc, exc_info=True)
        detail = str(exc)
        if hasattr(exc, "detail"):
            detail = str(getattr(exc, "detail"))
        return _tool_result(tool, success=False, error=f"Query failed: {detail}")


def analyze_stored_result(
    result_id: str = "",
    session_id: str = "",
    column: str = "",
):
    """Re-profile a stored sample/query result (follow-up without re-query)."""
    tool = "analyze_result"
    from .result_store import get_result_store

    doc = get_result_store().resolve(result_id=result_id, session_id=session_id)
    if not doc:
        return _tool_result(
            tool,
            success=False,
            error=(
                "No stored result to analyze. Sample a table or run a query first "
                '(e.g. "sample airports on Local Postgres").'
            ),
        )
    rows = list(doc.get("rows") or [])
    columns = list(doc.get("columns") or [])
    if column:
        col = column.strip()
        # Case-insensitive column match
        match = next((c for c in columns if c.lower() == col.lower()), None)
        if not match:
            return _tool_result(
                tool,
                success=False,
                error=f"Column '{col}' not in stored result. Available: {', '.join(columns[:20])}",
            )
        columns = [match]
    analysis = _analyze_rows(rows, columns)
    meta = doc.get("meta") or {}
    return _tool_result(
        tool,
        success=True,
        output={
            "result_id": doc.get("result_id"),
            "source": doc.get("source"),
            "connector_name": meta.get("connector_name"),
            "table": meta.get("table"),
            "query": meta.get("query"),
            "row_count": doc.get("row_count"),
            "analysis": analysis,
            "from_store": True,
        },
    )


def _cmp(a: Any, b: Any, op: str) -> bool:
    fa, fb = _try_float(a), _try_float(b)
    if fa is not None and fb is not None:
        if op == "gt":
            return fa > fb
        if op == "gte":
            return fa >= fb
        if op == "lt":
            return fa < fb
        if op == "lte":
            return fa <= fb
        if op == "eq":
            return fa == fb
        if op == "ne":
            return fa != fb
    sa, sb = str(a), str(b)
    if op == "eq":
        return sa == sb
    if op == "ne":
        return sa != sb
    if op == "gt":
        return sa > sb
    if op == "gte":
        return sa >= sb
    if op == "lt":
        return sa < sb
    if op == "lte":
        return sa <= sb
    return False


def filter_stored_result(
    result_id: str = "",
    session_id: str = "",
    column: str = "",
    op: str = "eq",
    value: str = "",
    limit: int = 25,
):
    """Filter rows in a stored result — real predicate over stored cells."""
    tool = "filter_result"
    from .result_store import get_result_store

    doc = get_result_store().resolve(result_id=result_id, session_id=session_id)
    if not doc:
        return _tool_result(
            tool,
            success=False,
            error=(
                "No stored result to filter. Sample a table or run a query first."
            ),
        )
    columns = list(doc.get("columns") or [])
    col = (column or "").strip()
    if not col:
        return _tool_result(tool, success=False, error="Provide a column to filter on.")
    match = next((c for c in columns if c.lower() == col.lower()), None)
    if not match:
        return _tool_result(
            tool,
            success=False,
            error=f"Column '{col}' not found. Available: {', '.join(columns[:20])}",
        )
    op_n = (op or "eq").strip().lower().replace(" ", "_")
    aliases = {
        "equals": "eq",
        "=": "eq",
        "==": "eq",
        "!=": "ne",
        "<>": "ne",
        "not_equals": "ne",
        "greater_than": "gt",
        ">": "gt",
        ">=": "gte",
        "less_than": "lt",
        "<": "lt",
        "<=": "lte",
        "isnull": "is_null",
        "null": "is_null",
        "notnull": "not_null",
        "like": "contains",
        "includes": "contains",
    }
    op_n = aliases.get(op_n, op_n)
    allowed = {"eq", "ne", "contains", "gt", "gte", "lt", "lte", "is_null", "not_null", "in"}
    if op_n not in allowed:
        return _tool_result(
            tool,
            success=False,
            error=f"Unsupported op '{op}'. Use: {', '.join(sorted(allowed))}",
        )

    rows = list(doc.get("rows") or [])
    matched: list[dict[str, Any]] = []
    for r in rows:
        cell = r.get(match)
        ok = False
        if op_n == "is_null":
            ok = _is_nullish(cell)
        elif op_n == "not_null":
            ok = not _is_nullish(cell)
        elif op_n == "contains":
            ok = str(value).lower() in str(cell if cell is not None else "").lower()
        elif op_n == "in":
            parts = [p.strip() for p in str(value).split(",") if p.strip()]
            ok = str(cell) in parts or (str(_try_float(cell)) in parts if _try_float(cell) is not None else False)
        else:
            ok = _cmp(cell, value, op_n)
        if ok:
            matched.append(r)

    limit = max(1, min(int(limit or 25), 100))
    # Persist filtered view as a new result for further follow-ups
    meta = dict(doc.get("meta") or {})
    meta["filter"] = {"column": match, "op": op_n, "value": value}
    meta["parent_result_id"] = doc.get("result_id")
    new_id = _store_result(
        rows=matched,
        columns=columns,
        column_schema=doc.get("column_schema") or {},
        meta=meta,
        session_id=session_id or str(doc.get("session_id") or ""),
        source=tool,
    )
    return _tool_result(
        tool,
        success=True,
        output={
            "result_id": new_id,
            "parent_result_id": doc.get("result_id"),
            "filter": {"column": match, "op": op_n, "value": value},
            "match_count": len(matched),
            "source_row_count": len(rows),
            "columns": columns,
            "rows": matched[:limit],
            "connector_name": meta.get("connector_name"),
            "table": meta.get("table"),
            "from_store": True,
        },
    )
