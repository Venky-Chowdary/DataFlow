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
    input_has_timezone,
    is_temporal_ddl,
    parse_sql_datetime,
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
    "INTERVAL",
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
    if ddl == "DATE":
        coerced = coerce_sql_temporal(value, "DATE")
        if isinstance(coerced, date) and not isinstance(coerced, datetime):
            return coerced.isoformat()
        if isinstance(coerced, datetime):
            return coerced.date().isoformat()
        return value
    if ddl in {"TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMESTAMPTZ"}:
        coerced = parse_sql_datetime(value, aware_utc=True)
    else:
        # TIMESTAMP_NTZ / bare TIMESTAMP / DATETIME / TIME: keep civil digits.
        # Do NOT astimezone(UTC) then strip — that invents a different local time
        # for offset wires (e.g. 12:00-05:00 → 17:00). TZ→NTZ polarity is a
        # Validate/Accept-risk concern; bind must not silently rewrite the clock.
        coerced = parse_sql_datetime(value, wall_clock=True)
    if isinstance(coerced, datetime):
        if coerced.tzinfo is not None:
            coerced = coerced.replace(tzinfo=None)
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
    if ddl == "INTERVAL":
        from services.value_serializer import format_bigquery_interval

        return format_bigquery_interval(value)
    if ddl == "DATE":
        coerced = coerce_sql_temporal(value, "DATE")
        if isinstance(coerced, date) and not isinstance(coerced, datetime):
            return coerced.isoformat()
        if isinstance(coerced, datetime):
            return coerced.date().isoformat()
        return value
    if ddl == "TIMESTAMP":
        # BQ TIMESTAMP is a UTC instant. Never invent Z on a naive wall-clock —
        # that silently relocates civil times. Require offset/Z, or use DATETIME.
        if not input_has_timezone(value):
            raise ValueError(
                "BigQuery TIMESTAMP refuses naive wall-clock (would invent UTC). "
                "Provide an offset/Z, or map to DATETIME for timezone-less values."
            )
        coerced = parse_sql_datetime(value, aware_utc=True)
        if not isinstance(coerced, datetime):
            return value
        coerced = coerced.astimezone(timezone.utc)
        return coerced.isoformat().replace("+00:00", "Z")
    if ddl == "TIME":
        coerced = coerce_sql_temporal(value, "TIME")
        if isinstance(coerced, time):
            return coerced.isoformat()
        if isinstance(coerced, datetime):
            return coerced.time().isoformat()
        return value
    # DATETIME: wall-clock only — keep civil digits.
    coerced = parse_sql_datetime(value, wall_clock=True)
    if isinstance(coerced, datetime):
        if coerced.tzinfo is not None:
            coerced = coerced.replace(tzinfo=None)
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

    # BigQuery INTERVAL is not a calendar temporal — normalize via dedicated formatter.
    if eng == "bigquery" and base == "INTERVAL":
        wire = format_bigquery_bind(value, ddl_type)
        return {
            "ok": True,
            "wire_value": wire if isinstance(wire, str) else (str(wire) if wire is not None else None),
            "reason": "Will normalize to BigQuery INTERVAL Y-M D H:M:S form" if wire != value else "",
            "needs_normalize": isinstance(value, str) and wire != value.strip(),
        }

    # Reuse shared parse; then check warehouse-specific wire form.
    check = wire_check_temporal(value, ddl_type if base != "TIMESTAMP_NTZ" else "TIMESTAMP")
    if not check["ok"]:
        return check

    try:
        if eng == "snowflake":
            wire = format_snowflake_bind(value, ddl_type)
        elif eng == "bigquery":
            wire = format_bigquery_bind(value, ddl_type)
        else:
            wire = format_wire_value(value, ddl_type)
    except ValueError as exc:
        return {
            "ok": False,
            "wire_value": None,
            "reason": str(exc),
            "needs_normalize": False,
        }

    needs = False
    if isinstance(value, str) and isinstance(wire, str):
        raw = value.strip()
        if raw != wire and ("T" in raw or raw.endswith(("Z", "z")) or "+" in raw[10:] or raw.count("-") >= 3):
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


# JSON numbers are parsed as binary64 by BigQuery's ingestion layer, so only
# integers inside the RFC 7159 / ECMAScript exact range may travel as numbers.
_BQ_EXACT_JSON_INT_MAX = 2**53 - 1


def bigquery_json_cell(value: Any) -> Any:
    """Render one cell for BigQuery JSON ingestion without losing exact digits.

    BigQuery's JSON wire format has no NUMERIC/BIGNUMERIC/BYTES notion: a JSON
    number is inferred as FLOAT64, so exact decimals must arrive as strings, and
    integers beyond ±(2^53−1) must too — "pass it as a string to avoid data
    corruption" (BigQuery loading-JSON docs). ``sanitize_json_value`` owns the
    canonical shape rules (Decimal → exact text, bytes → base64, UUID → text,
    recursive for repeated/STRUCT fields); JSON-native scalars are handed back
    untouched so FLOAT64 semantics stay exactly as the source produced them.
    """
    from services.value_serializer import sanitize_json_value

    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, float):
        import math

        if not math.isfinite(value):
            raise ValueError(
                f"non-finite float refused for BigQuery JSON wire: {value!r}"
            )
        return value
    if isinstance(value, int):
        return str(value) if abs(value) > _BQ_EXACT_JSON_INT_MAX else value
    return sanitize_json_value(value)


def bigquery_repeated_element(bq_or_logical_type: str) -> str | None:
    """Return the element type when a column is a BigQuery REPEATED field.

    Bare ``array`` / ``json`` map to a BigQuery JSON column, where a JSON *text*
    payload is the correct wire form. Only a parameterized ``ARRAY<T>`` becomes a
    REPEATED field, and those reject text outright — BigQuery answers a JSON
    string with ``invalid value type string for ARRAY column``, so the two cases
    must not share a serialization path.
    """
    text = (bq_or_logical_type or "").strip()
    if not text.lower().startswith("array<") or not text.endswith(">"):
        return None
    return text[len("array<") : -1].strip() or None


def bigquery_repeated_cell(value: Any, element_type: str) -> Any:
    """Build a real JSON array for a REPEATED column, preserving element digits."""
    from connectors.writer_common import parse_array_wire_elements

    elements, _error = parse_array_wire_elements(value)
    if elements is None:
        # Ambiguous or unusable payload: pass it through so BigQuery rejects it
        # into quarantine rather than us inventing an array shape.
        return bigquery_json_cell(value)
    if bigquery_temporal_ddl(element_type):
        return [
            format_bigquery_bind(el, element_type) if el is not None else None
            for el in elements
        ]
    return [bigquery_json_cell(el) for el in elements]


def records_for_bigquery(
    batch: list[tuple],
    target_cols: list[str],
    logical_or_bq_types: list[str],
) -> list[dict[str, Any]]:
    """Build insert_rows_json / load_table_from_json records with temporal + bool/JSON normalize."""
    from connectors.sql_bind import normalize_sql_bind_value
    from services.value_serializer import is_missing_sentinel

    records: list[dict[str, Any]] = []
    for row in batch:
        rec: dict[str, Any] = {}
        for i, col in enumerate(target_cols):
            val = row[i] if i < len(row) else None
            typ = logical_or_bq_types[i] if i < len(logical_or_bq_types) else "STRING"
            # Sparse CDC: omit DF_MISSING — never leak sentinel or invent NULL via MERGE.
            if is_missing_sentinel(val):
                continue
            element_type = bigquery_repeated_element(typ)
            if val is not None and element_type is not None:
                rec[col] = bigquery_repeated_cell(val, element_type)
            elif val is not None and bigquery_temporal_ddl(typ):
                rec[col] = format_bigquery_bind(val, typ)
            elif val is not None:
                rec[col] = bigquery_json_cell(
                    normalize_sql_bind_value(val, typ, engine="bigquery")
                )
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
