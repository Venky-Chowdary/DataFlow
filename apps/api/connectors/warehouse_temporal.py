"""Destination-native temporal bind helpers for Snowflake and BigQuery.

Reuses ``sql_temporal.coerce_sql_temporal`` so Validate wire probes and writers
share one parse path. Warehouse engines accept ISO-8601 more often than MySQL,
but transform-engine ``…T…Z`` still causes silent type mismatch or load rejects
when columns are DATE / TIMESTAMP_NTZ / BQ DATETIME — normalize explicitly.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any

from connectors.sql_temporal import (
    coerce_sql_temporal,
    format_wire_value,
    is_temporal_ddl,
    sql_base_type,
    wire_check_temporal,
)

_SF_TEMPORAL = frozenset({
    "DATE",
    "TIME",
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    "TIMESTAMPTZ",
})

_BQ_TEMPORAL = frozenset({
    "DATE",
    "TIME",
    "DATETIME",
    "TIMESTAMP",
})


def snowflake_temporal_ddl(sf_type: str) -> str | None:
    base = sql_base_type(sf_type)
    if base in _SF_TEMPORAL:
        return base
    return None


def bigquery_temporal_ddl(bq_type: str) -> str | None:
    base = sql_base_type(bq_type)
    if base in _BQ_TEMPORAL:
        return base
    return None


def format_snowflake_bind(value: Any, sf_type: str) -> Any:
    """Return a Snowflake-friendly bind/CSV cell for temporal DDL, else value."""
    ddl = snowflake_temporal_ddl(sf_type)
    if not ddl:
        return value
    coerced = coerce_sql_temporal(value, ddl if ddl != "TIMESTAMP_NTZ" else "TIMESTAMP")
    if isinstance(coerced, datetime):
        if ddl == "DATE":
            return coerced.date().isoformat()
        # TIMESTAMP_NTZ / TIMESTAMP: naive UTC wall clock, space separator (no T/Z).
        if coerced.tzinfo is not None:
            coerced = coerced.astimezone(timezone.utc).replace(tzinfo=None)
        if coerced.microsecond:
            return coerced.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
        return coerced.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(coerced, date) and not isinstance(coerced, datetime):
        return coerced.isoformat()
    if isinstance(coerced, time):
        return coerced.isoformat()
    return value


def format_bigquery_bind(value: Any, bq_type: str) -> Any:
    """Return a BigQuery JSON/API-friendly temporal value."""
    ddl = bigquery_temporal_ddl(bq_type)
    if not ddl:
        return value
    coerced = coerce_sql_temporal(value, ddl)
    if isinstance(coerced, datetime):
        if ddl == "DATE":
            return coerced.date().isoformat()
        if ddl == "TIMESTAMP":
            # RFC3339 UTC
            if coerced.tzinfo is None:
                coerced = coerced.replace(tzinfo=timezone.utc)
            else:
                coerced = coerced.astimezone(timezone.utc)
            text = coerced.isoformat().replace("+00:00", "Z")
            return text
        # DATETIME: no timezone
        if coerced.tzinfo is not None:
            coerced = coerced.astimezone(timezone.utc).replace(tzinfo=None)
        if coerced.microsecond:
            return coerced.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".")
        return coerced.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(coerced, date) and not isinstance(coerced, datetime):
        return coerced.isoformat()
    if isinstance(coerced, time):
        return coerced.isoformat()
    return value


def wire_check_warehouse(value: Any, ddl_type: str, *, engine: str) -> dict[str, Any]:
    """Wire probe for Snowflake/BigQuery — same contract as ``wire_check_temporal``."""
    eng = (engine or "").strip().lower()
    base = sql_base_type(ddl_type)
    if eng in {"snowflake"} and base not in _SF_TEMPORAL and not is_temporal_ddl(ddl_type):
        return {"ok": True, "wire_value": None, "reason": "", "needs_normalize": False}
    if eng in {"bigquery"} and base not in _BQ_TEMPORAL and not is_temporal_ddl(ddl_type):
        return {"ok": True, "wire_value": None, "reason": "", "needs_normalize": False}

    # Reuse shared parse; then check warehouse-specific wire form.
    check = wire_check_temporal(value, ddl_type if base != "TIMESTAMP_NTZ" else "TIMESTAMP")
    if not check["ok"]:
        return check

    if eng == "snowflake":
        wire = format_snowflake_bind(value, ddl_type)
    elif eng == "bigquery":
        wire = format_bigquery_bind(value, ddl_type)
    else:
        wire = format_wire_value(value, ddl_type)

    needs = False
    if isinstance(value, str) and isinstance(wire, str):
        raw = value.strip()
        if raw != wire and ("T" in raw or raw.endswith(("Z", "z"))):
            needs = True
    return {
        "ok": True,
        "wire_value": wire if isinstance(wire, str) else (str(wire) if wire is not None else None),
        "reason": f"Will normalize to {wire} for {eng} {base} bind" if needs else check.get("reason") or "",
        "needs_normalize": needs or bool(check.get("needs_normalize")),
    }


def coerce_mapped_rows_snowflake(
    mapped_rows: list[tuple],
    target_types: list[str],
) -> list[tuple]:
    """Normalize temporal cells in mapped tuples before COPY/INSERT."""
    if not mapped_rows or not any(snowflake_temporal_ddl(t) for t in target_types):
        return mapped_rows
    out: list[tuple] = []
    for row in mapped_rows:
        cells = list(row)
        for i, typ in enumerate(target_types):
            if i >= len(cells) or cells[i] is None:
                continue
            if snowflake_temporal_ddl(typ):
                cells[i] = format_snowflake_bind(cells[i], typ)
        out.append(tuple(cells))
    return out


def records_for_bigquery(
    batch: list[tuple],
    target_cols: list[str],
    logical_or_bq_types: list[str],
) -> list[dict[str, Any]]:
    """Build insert_rows_json / load_table_from_json records with temporal normalize."""
    records: list[dict[str, Any]] = []
    for row in batch:
        rec: dict[str, Any] = {}
        for i, col in enumerate(target_cols):
            val = row[i] if i < len(row) else None
            typ = logical_or_bq_types[i] if i < len(logical_or_bq_types) else "STRING"
            if val is not None and bigquery_temporal_ddl(typ):
                rec[col] = format_bigquery_bind(val, typ)
            else:
                rec[col] = val
        records.append(rec)
    return records


def quarantine_from_bigquery_errors(
    errors: list[Any],
    batch: list[tuple],
    target_cols: list[str],
    *,
    row_offset: int,
    policy: str,
) -> tuple[list[dict[str, Any]], set[int]]:
    """Map BigQuery insert_rows_json error payloads → rejected_details + bad indices."""
    details: list[dict[str, Any]] = []
    bad: set[int] = set()
    for err in errors or []:
        if not isinstance(err, dict):
            continue
        try:
            idx = int(err.get("index", -1))
        except (TypeError, ValueError):
            idx = -1
        if idx < 0 or idx >= len(batch):
            continue
        bad.add(idx)
        msgs = err.get("errors") or []
        reason_parts: list[str] = []
        col_name = "*"
        for m in msgs:
            if isinstance(m, dict):
                reason_parts.append(str(m.get("message") or m.get("reason") or m))
                loc = m.get("location") or m.get("field") or ""
                if loc and loc in target_cols:
                    col_name = str(loc)
            else:
                reason_parts.append(str(m))
        reason = "; ".join(reason_parts)[:300] or "BigQuery insert rejected row"
        sample = ""
        if col_name != "*" and col_name in target_cols:
            try:
                sample = str(batch[idx][target_cols.index(col_name)])[:120]
            except Exception:
                sample = ""
        elif batch[idx]:
            sample = str(batch[idx][0])[:120]
        details.append({
            "row": row_offset + idx,
            "column": col_name,
            "value": sample,
            "reason": reason,
            "policy": policy,
        })
    return details, bad
