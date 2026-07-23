"""Shared SQL date/time parsing for MySQL and PostgreSQL writers.

CSV/JSON sources commonly emit ISO-8601 with ``T`` and ``Z``. MySQL DATETIME
rejects that literal; Postgres is more lenient but still benefits from a single
canonical parse path so both destinations behave the same.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any


def sql_base_type(source_type: str) -> str:
    """Strip length/precision suffixes: DATETIME(6) → DATETIME."""
    upper = (source_type or "").upper().strip()
    if "(" in upper:
        upper = upper.split("(", 1)[0].strip()
    return upper


def parse_sql_datetime(value: Any, *, aware_utc: bool = False) -> datetime | None:
    """Parse ISO-8601 / common CSV timestamps.

    Default returns **naive UTC** (MySQL DATETIME / TIMESTAMP without TZ).
    When ``aware_utc=True`` (Postgres TIMESTAMPTZ), keep ``tzinfo=UTC`` so the
    driver does not reinterpret naive values in the session time zone.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
            return dt if aware_utc else dt.replace(tzinfo=None)
        return dt.replace(tzinfo=timezone.utc) if aware_utc else dt
    if isinstance(value, date) and not isinstance(value, datetime):
        dt = datetime.combine(value, time.min)
        return dt.replace(tzinfo=timezone.utc) if aware_utc else dt
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    # Unix epoch seconds / millis (common in CSV edge fixtures).
    if text.isdigit() or (text[0] in "+-" and text[1:].isdigit()):
        try:
            raw = int(text)
            if abs(raw) >= 10**12:
                raw = raw // 1000
            dt = datetime.fromtimestamp(raw, tz=timezone.utc)
            return dt if aware_utc else dt.replace(tzinfo=None)
        except (OverflowError, OSError, ValueError):
            pass
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    if text.upper().endswith(" UTC"):
        text = text[:-4].strip() + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y/%m/%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
        ):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
        return dt if aware_utc else dt.replace(tzinfo=None)
    return dt.replace(tzinfo=timezone.utc) if aware_utc else dt


def parse_sql_date(value: Any) -> date | None:
    parsed = parse_sql_datetime(value)
    if parsed is not None:
        return parsed.date()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if "T" in text:
            text = text.split("T", 1)[0]
        try:
            return date.fromisoformat(text)
        except ValueError:
            return None
    return None


def coerce_sql_temporal(value: Any, source_type: str) -> Any:
    """Coerce a cell to a Python temporal for the given SQL DDL type, else return value."""
    base = sql_base_type(source_type)
    if base in {
        "TIMESTAMPTZ",
        "TIMESTAMP_TZ",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP WITH LOCAL TIME ZONE",
        "DATETIMEOFFSET",
    }:
        parsed = parse_sql_datetime(value, aware_utc=True)
        return parsed if parsed is not None else value
    if base in {"DATETIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "DATETIME2", "SMALLDATETIME"}:
        parsed = parse_sql_datetime(value)
        return parsed if parsed is not None else value
    if base == "DATE":
        parsed = parse_sql_date(value)
        return parsed if parsed is not None else value
    if base == "TIME":
        if isinstance(value, time):
            return value
        parsed = parse_sql_datetime(value)
        if parsed is not None:
            return parsed.time()
        if isinstance(value, str):
            text = value.strip()
            try:
                return time.fromisoformat(text)
            except ValueError:
                return value
        return value
    return value


_TEMPORAL_BASES = frozenset({
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMP_TZ",
    "TIMESTAMPTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_NTZ",
    "DATE",
    "TIME",
})

# Destinations that bind temporals like MySQL/Postgres (ISO-Z literals unsafe).
# Includes generic_sql dialects, warehouses, and common catalog aliases so Validate
# matches the write path.
_WIRE_DESTS = frozenset({
    "mysql",
    "mariadb",
    "singlestore",
    "postgresql",
    "postgres",
    "redshift",
    "cockroachdb",
    "timescaledb",
    "supabase",
    "oracle",
    "sqlserver",
    "mssql",
    "synapse",
    "generic_sql",
    "clickhouse",
    "trino",
    "presto",
    "questdb",
    "db2",
    "h2",
    "duckdb",
    "sqlite",
    "snowflake",
    "bigquery",
})


def is_temporal_ddl(source_type: str) -> bool:
    return sql_base_type(source_type) in _TEMPORAL_BASES


def logical_to_temporal_ddl(logical: str) -> str | None:
    """Map transform/logical type names to a DDL base for ``coerce_sql_temporal``."""
    t = (logical or "").strip().lower()
    if t in {"date"}:
        return "DATE"
    if t in {"time"}:
        return "TIME"
    # Preserve TZ polarity: aware carriers keep UTC tzinfo on bind.
    if t in {
        "timestamptz",
        "timestamp_tz",
        "timestamp_ltz",
        "timestamp with time zone",
        "timestamp with local time zone",
        "datetimeoffset",
    }:
        return "TIMESTAMPTZ"
    if t in {
        "datetime",
        "timestamp",
        "timestamp_ntz",
        "timestamp without time zone",
        "datetime2",
        "smalldatetime",
    }:
        return "DATETIME"
    if is_temporal_ddl(logical):
        return sql_base_type(logical)
    return None


def format_wire_value(value: Any, source_type: str) -> str | None:
    """Human-readable form that would bind to MySQL/PG after coerce."""
    coerced = coerce_sql_temporal(value, source_type)
    base = sql_base_type(source_type)
    if isinstance(coerced, datetime):
        if base == "DATE":
            return coerced.date().isoformat()
        if coerced.microsecond:
            return coerced.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
        return coerced.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(coerced, date) and not isinstance(coerced, datetime):
        return coerced.isoformat()
    if isinstance(coerced, time):
        return coerced.isoformat()
    return None


def wire_check_temporal(value: Any, ddl_type: str) -> dict[str, Any]:
    """Simulate destination bind for temporal DDL (same helpers writers use).

    Returns ``{ok, wire_value, reason, needs_normalize}``.
    ``needs_normalize`` is True when the engine would emit ISO-Z text that
    MySQL DATETIME rejects as a literal but writers coerce successfully.
    """
    base = sql_base_type(ddl_type)
    if base not in _TEMPORAL_BASES:
        return {"ok": True, "wire_value": None, "reason": "", "needs_normalize": False}

    if value is None:
        return {"ok": True, "wire_value": None, "reason": "", "needs_normalize": False}
    if isinstance(value, str) and not value.strip():
        return {"ok": True, "wire_value": None, "reason": "", "needs_normalize": False}

    coerced = coerce_sql_temporal(value, ddl_type)
    wire = format_wire_value(value, ddl_type)

    # Still a string after coerce → cannot bind as temporal.
    if isinstance(coerced, str):
        text = coerced.strip()
        return {
            "ok": False,
            "wire_value": None,
            "reason": (
                f"Cannot coerce {text[:80]!r} to {base} for destination bind "
                f"(SQL engines reject ISO 'T'/'Z' literals without normalize)."
            ),
            "needs_normalize": False,
        }

    needs_normalize = False
    if isinstance(value, str):
        raw = value.strip()
        if ("T" in raw or raw.endswith(("Z", "z")) or "+" in raw[10:]) and wire:
            # Transform engine often keeps ISO-Z; writers must normalize.
            if raw != wire and ("T" in raw or raw.endswith(("Z", "z"))):
                needs_normalize = True

    return {
        "ok": True,
        "wire_value": wire,
        "reason": (
            f"Will normalize to {wire} for {base} bind"
            if needs_normalize and wire
            else ""
        ),
        "needs_normalize": needs_normalize,
    }


def dest_uses_sql_wire_probe(dest_db_type: str | None) -> bool:
    return (dest_db_type or "").strip().lower() in _WIRE_DESTS


def extract_column_from_sql_error(exc: BaseException | str) -> str | None:
    """Parse ``for column 'column_5'`` / ``column \"foo\"`` from driver errors."""
    import re

    text = str(exc)
    for pattern in (
        r"for column ['`]([^'`]+)['`]",
        r'for column ["“]([^"”]+)["”]',
        r"column ['`]([^'`]+)['`]",
        r'column "([^"]+)"',
    ):
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def is_sql_data_error(exc: BaseException | str) -> bool:
    """True for value/type contract errors that must not be retried as connection drops."""
    text = str(exc).lower()
    name = type(exc).__name__.lower() if isinstance(exc, BaseException) else ""
    if "dataerror" in name or "integrityerror" in name:
        return True
    return any(
        sig in text
        for sig in (
            "incorrect datetime",
            "incorrect date",
            "incorrect time",
            "truncated incorrect",
            "data truncation",
            "out of range value",
            "invalid input syntax",
            "invalid datetime",
            "date/time field value out of range",
            "cannot cast",
            "invalid value",
        )
    )
