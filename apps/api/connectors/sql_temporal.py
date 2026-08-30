"""Shared SQL date/time parsing for MySQL and PostgreSQL writers.

CSV/JSON sources commonly emit ISO-8601 with ``T`` and ``Z``. MySQL DATETIME
rejects that literal; Postgres is more lenient but still benefits from a single
canonical parse path so both destinations behave the same.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from typing import Any


def round_to_smalldatetime(value: datetime) -> datetime:
    """Round to SQL Server ``SMALLDATETIME`` minute accuracy (Microsoft docs).

    Seconds ≤ 29.998 → floor to minute; ≥ 29.999 → ceil to next minute.
    Result is timezone-naive (SMALLDATETIME has no offset polarity).
    """
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    # Compare as whole microseconds past the minute.
    us = value.second * 1_000_000 + value.microsecond
    base = value.replace(second=0, microsecond=0)
    if us >= 29_999_000:  # 29.999 seconds
        return base + timedelta(minutes=1)
    return base


@lru_cache(maxsize=8192)
def sql_base_type(source_type: str) -> str:
    """Strip length/precision suffixes while preserving TZ polarity.

    Airbyte / Postgres class:
    - ``TIMESTAMP(6) WITH TIME ZONE`` → ``TIMESTAMPTZ`` (never bare ``TIMESTAMP``)
    - ``TIMESTAMPTZ(3)`` → ``TIMESTAMPTZ``
    - ``TIMESTAMP WITHOUT TIME ZONE`` → ``TIMESTAMP``
    - ``DATETIME(6)`` → ``DATETIME``
    - ``DECIMAL(10,2)`` → ``DECIMAL``

    ClickHouse / Databricks class:
    - ``Nullable(Int64)`` / ``LowCardinality(Nullable(DateTime64(3)))`` unwrap
      then canonicalize ``Int64``→``BIGINT``, ``DateTime64``→``DATETIME64``.
    """
    upper = re.sub(r"\s+", " ", (source_type or "").upper().strip())
    if not upper:
        return upper
    # Unwrap ClickHouse wrappers before TZ / precision decisions so
    # ``Nullable(DateTime64(3))`` is not mis-read as base ``NULLABLE``.
    while True:
        wrap = re.match(r"^(NULLABLE|LOWCARDINALITY)\((.+)\)$", upper)
        if not wrap:
            break
        upper = re.sub(r"\s+", " ", wrap.group(2).strip())
    # TZ polarity MUST be decided before splitting on '(' — otherwise
    # ``TIMESTAMP(6) WITH TIME ZONE`` collapses to ``TIMESTAMP`` and writers
    # silently strip offsets (enterprise fidelity failure).
    if "WITH LOCAL TIME ZONE" in upper or upper.startswith("TIMESTAMP_LTZ"):
        return "TIMESTAMPTZ"
    # TIMETZ before TIMESTAMPTZ — ``TIMETZ``.startswith("TIME") is true but
    # ``TIME\b`` does not match, so TIMETZ was mis-routed to TIMESTAMPTZ and
    # naive UTC invent ran on time-of-day wires.
    if upper.startswith("TIMETZ") or re.match(r"^TIME\s+WITH\s+TIME\s+ZONE\b", upper):
        return "TIMETZ"
    if (
        re.search(r"\bWITH TIME ZONE\b", upper)
        or upper.startswith("TIMESTAMPTZ")
        or upper == "DATETIMEOFFSET"
        or upper.startswith("DATETIMEOFFSET")
    ):
        if re.match(r"^TIME\b", upper) and not upper.startswith("TIMESTAMP"):
            return "TIMETZ"
        return "TIMESTAMPTZ"
    if re.search(r"\bWITHOUT TIME ZONE\b", upper) or "TIMESTAMP_NTZ" in upper:
        if re.match(r"^TIME\b", upper) and not upper.startswith("TIMESTAMP"):
            return "TIME"
        return "TIMESTAMP"
    if "(" in upper:
        upper = upper.split("(", 1)[0].strip()
    # ClickHouse / Arrow / Spark integer & temporal aliases → canonical bases
    # so overlay_physical_bind_types and coerce_sql_temporal share one map.
    aliases = {
        "INT64": "BIGINT",
        "INT32": "INTEGER",
        "INT16": "SMALLINT",
        "INT8": "TINYINT",
        "UINT64": "BIGINT",
        "UINT32": "INTEGER",
        "UINT16": "SMALLINT",
        "UINT8": "TINYINT",
        "FLOAT32": "FLOAT",
        "FLOAT64": "DOUBLE",
        "DATE32": "DATE",
        "DATETIME64": "DATETIME64",
        "BOOL": "BOOLEAN",
    }
    return aliases.get(upper, upper)


def input_has_timezone(value: Any) -> bool:
    """True when the wire/value carries an explicit offset or Z (not invented)."""
    if isinstance(value, datetime):
        return value.tzinfo is not None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Epoch seconds are instants by definition.
        return True
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    if text.endswith(("Z", "z")) or text.upper().endswith(" UTC"):
        return True
    if text.isdigit() or (text[0] in "+-" and text[1:].isdigit()):
        return True
    # Trailing ±HH:MM / ±HHMM after a datetime body.
    return bool(re.search(r"[+-]\d{2}:?\d{2}$", text))


def parse_sql_datetime(
    value: Any,
    *,
    aware_utc: bool = False,
    wall_clock: bool = False,
) -> datetime | None:
    """Parse ISO-8601 / common CSV timestamps.

    Default returns **naive UTC** (MySQL DATETIME / TIMESTAMP without TZ) —
    offset wires are converted to UTC then stripped.
    When ``aware_utc=True`` (Postgres TIMESTAMPTZ), keep ``tzinfo=UTC``.
    When ``wall_clock=True`` (Snowflake NTZ / BQ DATETIME), keep civil digits
    and strip tzinfo **without** ``astimezone(UTC)`` so Validate/Accept-risk
    owns TZ→NTZ polarity instead of the bind silently rewriting the clock.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is not None:
            if wall_clock:
                return dt.replace(tzinfo=None)
            dt = dt.astimezone(timezone.utc)
            return dt if aware_utc else dt.replace(tzinfo=None)
        if wall_clock:
            return dt
        return dt.replace(tzinfo=timezone.utc) if aware_utc else dt
    if isinstance(value, date) and not isinstance(value, datetime):
        dt = datetime.combine(value, time.min)
        if wall_clock:
            return dt
        return dt.replace(tzinfo=timezone.utc) if aware_utc else dt
    # Unix epoch seconds / millis as int/float (Stripe / HubSpot / SaaS wire).
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            raw = int(value)
            if abs(raw) >= 10**12:
                raw //= 1000  # epoch millis
            dt = datetime.fromtimestamp(raw, tz=timezone.utc)
            if wall_clock:
                return dt.replace(tzinfo=None)
            return dt if aware_utc else dt.replace(tzinfo=None)
        except (OverflowError, OSError, ValueError):
            return None
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
            if wall_clock:
                return dt.replace(tzinfo=None)
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
        # Year-first space forms are unambiguous. Slash/dash day-month pairs
        # go through apply_transform so Auto fails closed on 01/02/2024 00:00:00
        # instead of inventing MDY the way strptime did.
        dt = None
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y/%m/%d %H:%M:%S",
        ):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if dt is None:
            from services.transform_engine import apply_transform

            iso, err = apply_transform(text, "datetime")
            if err or iso is None:
                return None
            try:
                parsed = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
            except ValueError:
                return None
            dt = parsed.replace(tzinfo=None) if parsed.tzinfo is None else parsed
    if dt.tzinfo is not None:
        if wall_clock:
            return dt.replace(tzinfo=None)
        dt = dt.astimezone(timezone.utc)
        return dt if aware_utc else dt.replace(tzinfo=None)
    if wall_clock:
        return dt
    return dt.replace(tzinfo=timezone.utc) if aware_utc else dt


def _integral_digit_token(value: Any) -> str | None:
    """ASCII digits for a whole number, or ``None``.

    Used to send compact ``YYYYMMDD`` / epoch tokens through the write-path
    date parser without ``float`` scientific spelling.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        if value != int(value):
            return None
        return str(int(value))
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit() or (text[:1] in "+-" and text[1:].isdigit()):
            return text
    return None


def parse_sql_date(value: Any) -> date | None:
    """Calendar date only. Epoch instants refuse — they invent a UTC day.

    Matches ``apply_transform(..., "date")``. Compact ``YYYYMMDD`` still
    binds. ``DATETIME`` / ``TIMESTAMP`` still bind epoch via
    ``parse_sql_datetime``. A typed ``datetime`` keeps its calendar day
    (already a date, not an epoch invent).
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        text = _integral_digit_token(value)
        if text is None:
            return None
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
    else:
        return None
    from services.transform_engine import apply_transform

    iso, err = apply_transform(text, "date")
    if err or iso is None:
        return None
    try:
        return date.fromisoformat(str(iso)[:10])
    except ValueError:
        return None


def _is_mysql_engine(engine: str) -> bool:
    """True for the MySQL family (mariadb/tidb/aurora-mysql all normalize here)."""
    eng = (engine or "").strip().lower()
    if not eng:
        return False
    from services.type_system import _normalize_dest_db

    return _normalize_dest_db(eng) == "mysql"


@lru_cache(maxsize=8192)
def sql_type_is_temporal(source_type: str) -> bool:
    """True when ``coerce_sql_temporal`` can act on this DDL type.

    Every branch of the coercion is gated on ``sql_base_type`` landing in
    ``_TEMPORAL_BASES``, so a non-temporal column can skip the call entirely
    instead of re-deriving the base for each of its cells.
    """
    return sql_base_type(source_type) in _TEMPORAL_BASES


def coerce_sql_temporal(value: Any, source_type: str, *, engine: str = "") -> Any:
    """Coerce a cell to a Python temporal for the given SQL DDL type, else return value.

    ``engine`` disambiguates the bare ``TIMESTAMP`` token. On MySQL it is an
    instant carrier (stored UTC, converted with the session ``time_zone``, which
    writers pin to ``+00:00``), so an offset-bearing wire is converted to UTC
    rather than having its offset stripped off the civil digits. Everywhere else
    bare ``TIMESTAMP`` stays wall-clock.
    """
    from services.value_serializer import absent_sql_bind

    handled, bound = absent_sql_bind(value)
    if handled:
        return bound
    base = sql_base_type(source_type)
    # Empty → SQL NULL / MySQL zero-date on upsert wipe. Quarantine owns the cell.
    if base in _TEMPORAL_BASES and isinstance(value, str) and not value.strip():
        raise ValueError(
            f"empty string cannot coerce to {base} — "
            "refuse silent NULL invent (quarantine or remap upstream)"
        )
    if base == "TIMESTAMP" and _is_mysql_engine(engine):
        from services.timezone_policy import (
            MYSQL_TIMESTAMP_MAX,
            MYSQL_TIMESTAMP_MIN,
            mysql_timestamp_out_of_range,
        )

        if mysql_timestamp_out_of_range(value):
            raise ValueError(
                "value is outside the MySQL TIMESTAMP epoch range "
                f"({MYSQL_TIMESTAMP_MIN.date()} .. {MYSQL_TIMESTAMP_MAX.date()}) "
                "— quarantined; map to DATETIME(6) with a UTC-normalize contract "
                "to carry instants beyond 2038"
            )
        # Session time_zone is pinned to UTC, so a naive UTC bind stores the
        # same instant the aware wire carried.
        parsed = parse_sql_datetime(value, aware_utc=True)
        if parsed is None:
            return value
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    if base in {
        "TIMESTAMPTZ",
        "TIMESTAMP_TZ",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP WITH LOCAL TIME ZONE",
        "DATETIMEOFFSET",
        "TIMESTAMP_LTZ",
    }:
        # Refuse naive wall-clock → UTC invent (parity with BQ TIMESTAMP / TIMETZ).
        if not input_has_timezone(value):
            raise ValueError(
                f"{base} refuses naive wall-clock (would invent UTC). "
                "Provide an offset/Z, or map to TIMESTAMP_NTZ / DATETIME."
            )
        parsed = parse_sql_datetime(value, aware_utc=True)
        if parsed is None:
            return value
        from services.offset_label import restore_offset_after_utc

        return restore_offset_after_utc(
            value, parsed, engine=engine, dest_type=source_type
        )
    if base in {
        "DATETIME",
        "DATETIME64",
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
        "DATETIME2",
        "SMALLDATETIME",
        "TIMESTAMP WITHOUT TIME ZONE",
    }:
        # ClickHouse DateTime64(p, 'UTC') / named TZ → aware UTC polarity;
        # bare DateTime64(p) stays wall-clock (no silent offset strip invent).
        raw_u = re.sub(r"\s+", " ", (source_type or "").upper())
        if base == "DATETIME64" and (
            "'" in raw_u or "UTC" in raw_u or "TIME ZONE" in raw_u
        ):
            if not input_has_timezone(value):
                raise ValueError(
                    f"{source_type} refuses naive wall-clock (would invent UTC). "
                    "Provide an offset/Z, or map to DateTime64 without timezone."
                )
            parsed = parse_sql_datetime(value, aware_utc=True)
            return parsed if parsed is not None else value
        parsed = parse_sql_datetime(value, wall_clock=True)
        if parsed is None:
            return value
        if base == "SMALLDATETIME":
            return round_to_smalldatetime(parsed)
        return parsed
    if base == "DATE":
        parsed = parse_sql_date(value)
        if parsed is not None:
            return parsed
        if _integral_digit_token(value) is not None:
            raise ValueError(
                "DATE refuses epoch instants (would invent a UTC calendar day). "
                "Map to TIMESTAMP/DATETIME, or send a calendar date "
                "(ISO, unambiguous slash, or YYYYMMDD)."
            )
        return value
    if base in {"TIME", "TIME WITH TIME ZONE", "TIME WITHOUT TIME ZONE", "TIMETZ"}:
        aware = base in {"TIME WITH TIME ZONE", "TIMETZ"}

        def _timetz_or_refuse(tm: time) -> time:
            if aware and tm.tzinfo is None:
                raise ValueError(
                    "TIMETZ refuses naive wall-clock (would invent UTC). "
                    "Provide an offset/Z, or map to TIME without time zone."
                )
            if not aware and tm.tzinfo is not None:
                # Keep civil clock digits — do not UTC-shift then strip.
                return tm.replace(tzinfo=None)
            return tm

        def _parse_time_wire(raw: Any) -> time | None:
            if isinstance(raw, time):
                return raw
            if isinstance(raw, bool):
                return None
            from services.transform_engine import apply_transform

            text = str(raw).strip() if raw is not None else ""
            if not text:
                return None
            parsed, err = apply_transform(text, "time")
            if err or parsed is None:
                return None
            iso = str(parsed)
            try:
                return time.fromisoformat(iso)
            except ValueError:
                return None

        tm = _parse_time_wire(value)
        if tm is None:
            if _integral_digit_token(value) is not None:
                raise ValueError(
                    "TIME refuses epoch instants (would invent a clock). "
                    "Send a clock (HH:MM[:SS] or AM/PM)."
                )
            return value
        return _timetz_or_refuse(tm)
    return value


def bind_time_clock(value: Any) -> time | None:
    """One TIME clock. Reader-null is None. Unfit cells raise — never str() invent."""
    coerced = coerce_sql_temporal(value, "TIME")
    if coerced is None:
        return None
    if isinstance(coerced, time):
        return coerced
    if isinstance(coerced, datetime):
        return coerced.time()
    raise ValueError(
        f"TIME refused {value!r} (refuse silent str() invent). "
        "Send a clock (HH:MM[:SS] or AM/PM)."
    )


def bind_time_iso(value: Any) -> str | None:
    """Dest-canonical TIME text (ISO clock) or SQL NULL."""
    clock = bind_time_clock(value)
    return None if clock is None else clock.isoformat()


_TEMPORAL_BASES = frozenset({
    "DATETIME",
    "DATETIME64",
    "TIMESTAMP",
    "TIMESTAMP_TZ",
    "TIMESTAMPTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_NTZ",
    "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP WITH LOCAL TIME ZONE",
    "TIMESTAMP WITHOUT TIME ZONE",
    "DATE",
    "TIME",
    "TIME WITH TIME ZONE",
    "TIME WITHOUT TIME ZONE",
    "TIMETZ",
    "DATETIMEOFFSET",
    "DATETIME2",
    "SMALLDATETIME",
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
    "hive",
    "impala",
    "athena",
    "awsathena",
    "amazon_athena",
    "teradata",
    "vertica",
    "hana",
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
    # Offset-storing carriers keep the originating label; instant-only
    # TIMESTAMPTZ stays UTC. Folding DATETIMEOFFSET into TIMESTAMPTZ here
    # is how a SQL Server dest received +00:00.
    if t in {"datetimeoffset"}:
        return "DATETIMEOFFSET"
    if t in {
        "timestamptz",
        "timestamp_tz",
        "timestamp_ltz",
        "timestamp with time zone",
        "timestamp with local time zone",
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


def format_wire_value(value: Any, source_type: str, *, engine: str = "") -> str | None:
    """Human-readable form that would bind to MySQL/PG after coerce."""
    coerced = coerce_sql_temporal(value, source_type, engine=engine)
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


def wire_check_temporal(value: Any, ddl_type: str, *, engine: str = "") -> dict[str, Any]:
    """Simulate destination bind for temporal DDL (same helpers writers use).

    ``engine`` must be the destination engine so Validate simulates the *same*
    bind Execute will run — MySQL ``TIMESTAMP`` is an instant carrier with epoch
    bounds, and a range violation has to surface here, not at write time.

    Returns ``{ok, wire_value, reason, needs_normalize}``.
    ``needs_normalize`` is True when the engine would emit ISO-Z text that
    MySQL DATETIME rejects as a literal but writers coerce successfully.
    """
    base = sql_base_type(ddl_type)
    if base not in _TEMPORAL_BASES:
        return {"ok": True, "wire_value": None, "reason": "", "needs_normalize": False}

    from services.value_serializer import absent_sql_bind

    handled, bound = absent_sql_bind(value)
    if handled:
        return {"ok": True, "wire_value": None, "reason": "", "needs_normalize": False}
    if isinstance(value, str) and not value.strip():
        return {
            "ok": False,
            "wire_value": None,
            "reason": (
                f"empty string cannot coerce to {base} — "
                "refuse silent NULL invent (quarantine or remap upstream)"
            ),
            "needs_normalize": False,
        }

    try:
        coerced = coerce_sql_temporal(value, ddl_type, engine=engine)
        wire = format_wire_value(value, ddl_type, engine=engine)
    except ValueError as exc:
        return {
            "ok": False,
            "wire_value": None,
            "reason": str(exc),
            "needs_normalize": False,
        }

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


# Row-level contract violations: the *value* is unfit for the destination
# column, so the row belongs in quarantine and the rest of the chunk must
# still land. Duplicate/unique-key errors are deliberately absent — those are
# an identity/replay concern, not a row-value defect, and quarantining them
# would let a non-idempotent replay report success.
_ROW_CONTRACT_ERROR_SIGNATURES = (
    # temporal / numeric / cast (MySQL, PostgreSQL, SQL Server)
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
    "numeric value out of range",
    "value too long for type",
    "string or binary data would be truncated",
    # NOT NULL
    "violates not-null constraint",
    "null value in column",
    "cannot be null",
    "cannot insert the value null",
    "does not allow nulls",
    # CHECK / FK
    "violates check constraint",
    "violates foreign key constraint",
    "check constraint",
    "foreign key constraint",
    # Oracle: 01400 NULL insert, 01438/12899 too large, 02290 check, 02291 FK
    "ora-01400",
    "ora-01438",
    "ora-12899",
    "ora-02290",
    "ora-02291",
)

# Unique/PK collisions must keep aborting the chunk (see above).
_IDENTITY_COLLISION_SIGNATURES = (
    "violates unique constraint",
    "duplicate key value",
    "duplicate entry",
    "unique constraint",
    "ora-00001",
    "cannot insert duplicate key",
)


def is_identity_collision_error(exc: BaseException | str) -> bool:
    """True for unique/primary-key collisions (replay identity, not row value)."""
    return any(sig in str(exc).lower() for sig in _IDENTITY_COLLISION_SIGNATURES)


def is_sql_data_error(exc: BaseException | str) -> bool:
    """True for row-level value/contract errors.

    Such an error must never be retried as a connection drop, and under a
    quarantine policy it must be resolved row by row so the fit rows still
    land and the unfit ones are counted — a whole-chunk abort leaves rows
    neither written nor quarantined, which breaks the conservation ledger.

    Driver classification is read from the exception's MRO, not from the
    concrete class name: psycopg2 raises ``NotNullViolation``, pymysql raises
    ``IntegrityError``, and only the base classes are DB-API contract.
    """
    text = str(exc).lower()
    if is_identity_collision_error(text):
        return False
    if isinstance(exc, BaseException):
        for klass in type(exc).__mro__:
            klass_name = klass.__name__.lower()
            if "dataerror" in klass_name or "integrityerror" in klass_name:
                return True
    return any(sig in text for sig in _ROW_CONTRACT_ERROR_SIGNATURES)
