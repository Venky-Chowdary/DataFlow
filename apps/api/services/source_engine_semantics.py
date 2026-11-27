"""What a bare source type token means on the engine that declared it (backlog C3).

Audit 12 left five type pairs graded lossless that lose values, because the
type token alone does not say what the source engine stores:

* SQL Server ``tinyint`` is unsigned (0..255); MySQL ``TINYINT`` is signed
  (-128..127), so 128..255 do not fit.
* Oracle ``DATE`` carries a time of day to the second; a ``DATE`` almost
  everywhere else is a calendar day, so the time is dropped.
* MySQL ``TIMESTAMP`` is an instant (stored in UTC, read in the session zone);
  PostgreSQL ``TIMESTAMP`` is a wall clock with no zone.
* PostgreSQL bare ``numeric`` has no declared precision (up to 131072 integer
  and 16383 fraction digits); any ``DECIMAL(p,s)`` bounds it.
* SQL Server ``datetime2`` keeps 100 ns ticks (7 digits); PostgreSQL keeps
  microseconds. That one is judged on the measured values
  (:func:`services.decision_kernel.type_invent.temporal_precision_would_narrow`),
  because no PostgreSQL carrier can hold the 7th digit.

Two functions share the engine facts:

* :func:`create_new_source_type` restates the source token so the create-new
  mapper picks a carrier that keeps every value (``TINYINT UNSIGNED``,
  ``TIMESTAMP(0)``, ``TIMESTAMPTZ``), so the product's own second run is not
  refused by the predicate below;
* :func:`engine_semantic_loss` is the fidelity predicate's engine-aware check:
  the reason a pair loses values on this source engine, or ``""``.

Both read the source engine bound for the run (``active_source_engine``);
without one the token keeps its generic meaning.
"""

from __future__ import annotations

import re
from typing import Any

_SQLSERVER = frozenset({
    "sqlserver", "mssql", "sql_server", "azure_sql_database", "amazon_rds_sql_server",
    "google_cloud_sql_sql_server", "azure_sql", "synapse_analytics",
})
_ORACLE = frozenset({"oracle", "oracledb", "oracle_db", "oracle_autonomous_warehouse"})
_MYSQL = frozenset({"mysql", "mariadb", "aurora_mysql", "amazon_rds_mysql", "tidb"})
_POSTGRES = frozenset({
    "postgresql", "postgres", "aurora_postgresql", "amazon_rds_postgresql", "alloydb",
    "supabase", "neon", "citus", "timescaledb",
})
#: Destinations whose ``TIMESTAMP`` is a zoneless wall clock with a zoned twin.
_PG_FAMILY_DESTS = frozenset({
    "postgresql", "postgres", "aurora_postgresql", "alloydb", "supabase", "neon",
    "citus", "timescaledb", "cockroachdb", "yugabytedb", "redshift",
})


def _engine(source_db: str | None = None) -> str:
    if source_db:
        return str(source_db).strip().lower()
    from services.source_engine_scope import active_source_engine

    return active_source_engine()


def _base(type_text: str) -> str:
    from services.type_system import strip_identity_qualifier

    text = strip_identity_qualifier(type_text or "").upper().strip()
    return re.sub(r"\s*\(.*$", "", text).strip()


def _fsp(type_text: str) -> str:
    match = re.search(r"\(\s*(\d+)\s*\)", type_text or "")
    return match.group(1) if match else ""


def _dest(dest_db: str) -> str:
    from services.dest_dialect_facts import _normalize_dest_db

    return _normalize_dest_db(dest_db) if dest_db else ""


def create_new_source_type(source_type: str, dest_db: str, source_db: str = "") -> str:
    """The source token as its engine means it, for ``dest_db``.

    The create-new mapper reads it to pick a carrier that keeps every value,
    and the fidelity predicate reads it so that carrier is graded lossless.
    """
    engine = _engine(source_db)
    base = _base(source_type)
    dest = _dest(dest_db)
    if engine in _SQLSERVER and base == "TINYINT" and dest in {"mysql", "mariadb"}:
        # The only destination with a same-width unsigned carrier; elsewhere the
        # mapper already widens (SMALLINT, NUMBER(5,0)) and the bounds check in
        # :func:`engine_semantic_loss` covers an existing narrower column.
        return "TINYINT UNSIGNED"
    if engine in _ORACLE and base == "DATE" and dest and dest not in _ORACLE:
        return "TIMESTAMP(0)"
    if engine in _MYSQL and base == "TIMESTAMP" and dest in _PG_FAMILY_DESTS - {"redshift"}:
        fsp = _fsp(source_type)
        return f"TIMESTAMP({fsp}) WITH TIME ZONE" if fsp else "TIMESTAMP WITH TIME ZONE"
    if engine in _ORACLE and _oracle_decimal_float(source_type) and dest and dest not in _ORACLE:
        # Oracle FLOAT is a decimal NUMBER of up to 38 digits (accuracy review
        # finding 8): carry it as NUMBER, never as a binary64 double.
        return "NUMBER"
    if engine in _POSTGRES and base == "MONEY" and dest and dest not in _PG_ONLY_MONEY:
        # money is an int64 count of the smallest unit: up to 19 digits, with
        # lc_monetary's fraction digits (2 usually, up to 4 here).
        return "NUMERIC(23,4)"
    return source_type


_PG_ONLY_MONEY = frozenset({"postgresql", "postgres"})
_BINARY64 = frozenset({"DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT8", "FLOAT64", "BINARY_DOUBLE", "REAL",
                       "FLOAT4", "BINARY_FLOAT"})


def _bare(type_text: str) -> str:
    """The type name without its ``(n)`` arguments: ``TIMESTAMP WITH TIME ZONE``."""
    from services.type_system import strip_identity_qualifier

    text = re.sub(r"\(\s*[\d,\s]*\)", "", strip_identity_qualifier(type_text or "").upper())
    return " ".join(text.split())


def _oracle_decimal_float(source_type: str) -> bool:
    """Oracle FLOAT / FLOAT(p) with more binary digits than a double's 53."""
    base = _base(source_type)
    if base not in {"FLOAT", "DOUBLE PRECISION"}:
        return False
    p = _fsp(source_type)
    return not p or int(p) > 53


def offset_dropped(source_type: str, target_type: str, *, dest_db: str = "", source_db: str = "") -> str:
    """Why an offset-bearing source loses its UTC offset on this target, or ``""``.

    SQL Server ``datetimeoffset`` and Oracle ``TIMESTAMP WITH TIME ZONE`` store
    each value's own offset (or zone region); PostgreSQL ``timestamptz``, a
    BSON date and most instants keep the instant only. The product loads the
    instant (D136); the classifier discloses the dropped offset instead of
    grading the pair a round trip (accuracy review finding 8).
    """
    from services.decision_kernel.types import normalize_logical_type

    engine = _engine(source_db)
    dest = _dest(dest_db)
    if not (
        (engine in _SQLSERVER and _base(source_type) == "DATETIMEOFFSET")
        or (engine in _ORACLE and _bare(source_type) == "TIMESTAMP WITH TIME ZONE")
    ):
        return ""
    if normalize_logical_type(target_type) not in {"datetime", "date"} or _keeps_offset(target_type, dest):
        return ""
    return (
        f"{source_type} stores each value's own UTC offset (or zone region); "
        f"{target_type} keeps the instant but not the offset"
    )


def oracle_zone_bind_shifts_instant(
    source_type: str, target_type: str, *, dest_db: str = "", source_db: str = ""
) -> str:
    """Why an instant written into an Oracle zone-bearing column moves, or ``""``.

    The row writer hands python-oracledb an aware ``datetime`` (the originating
    offset for ``WITH TIME ZONE``, UTC for ``WITH LOCAL TIME ZONE``). The driver
    encodes only the civil digits: ``encode_date``/``encode_timestamp`` never
    read ``tzinfo`` and ``encode_timestamp_tz`` writes a fixed ``+00:00``
    (python-oracledb ``impl/base/encoders.pyx``). Oracle then reads those
    digits in the session time zone, which the thin driver sets from the
    client process's local offset; DataFlow pins neither. So the wall clock is
    kept and the offset dropped: the instant moves by the source offset (or by
    the process offset for ``LOCAL``). That route needs a Risk Contract
    (accuracy review finding 8). Same-instance Oracle to Oracle copies run
    server side (``INSERT ... SELECT``) and are not judged here.
    """
    from connectors.oracle_tstz import oracle_tz_kind
    from services.type_system import datetime_timezone_polarity

    if _dest(dest_db) not in _ORACLE or (source_db or "") in _ORACLE:
        return ""
    if oracle_tz_kind(target_type) is None:
        return ""
    if datetime_timezone_polarity(source_type) not in {"tz", "ltz"}:
        return ""
    return (
        f"{target_type} is written through python-oracledb, which sends a value's "
        "clock digits without its offset; Oracle reads them in the session time "
        f"zone, so a {source_type} instant moves by its offset"
    )


def _offsets_all_utc(population: Any) -> bool:
    if population is None:
        return False
    measured = False
    for value in population:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        measured = True
        if not text.endswith(("+00:00", "+0000", "Z", "+00", "-00:00")):
            return False
    return measured


def _keeps_offset(target_type: str, dest: str) -> bool:
    text = " ".join((target_type or "").upper().split())
    if "DATETIMEOFFSET" in text or "TIMESTAMP_TZ" in text:
        return True
    return dest in _ORACLE and "WITH TIME ZONE" in text and "LOCAL" not in text


def _zone_aware(target_type: str) -> bool:
    text = " ".join((target_type or "").upper().split())
    return "TIMESTAMPTZ" in text or ("WITH TIME ZONE" in text and "WITHOUT" not in text)


def engine_semantic_loss(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
    population: Any = None,
    source_db: str = "",
) -> str:
    """Why this pair loses values on the bound source engine, or ``""``."""
    from services.decision_kernel.types import normalize_logical_type

    engine = _engine(source_db)
    shifted = oracle_zone_bind_shifts_instant(source_type, target_type, dest_db=dest_db, source_db=engine)
    if shifted:
        return shifted
    if not engine:
        return ""
    base = _base(source_type)
    tgt_l = normalize_logical_type(target_type)
    dest = _dest(dest_db)
    if (
        engine in _SQLSERVER
        and base == "TINYINT"
        and dest not in _SQLSERVER | {"mssql"}
        and "UNSIGNED" not in (target_type or "").upper()
        and tgt_l in {"integer", "bigint", "smallint", "tinyint"}
    ):
        from services.type_system import integer_storage_bounds

        bounds = integer_storage_bounds(target_type, dest_db=dest)
        if bounds and bounds[1] < 255:
            return (
                f"SQL Server tinyint holds 0..255; {target_type} holds "
                f"{bounds[0]}..{bounds[1]}, so values above {bounds[1]} do not fit"
            )
    if engine in _ORACLE and base == "DATE" and tgt_l == "date" and dest not in _ORACLE:
        return (
            f"Oracle DATE keeps a time of day to the second; {target_type} keeps the "
            "calendar day only, so the time is dropped"
        )
    if (
        engine in _MYSQL
        and base == "TIMESTAMP"
        and dest in _PG_FAMILY_DESTS
        and tgt_l == "datetime"
        and not _zone_aware(target_type)
    ):
        return (
            f"MySQL TIMESTAMP is an instant (stored in UTC); {target_type} is a wall "
            "clock with no zone, so the instant's zone is dropped"
        )
    if engine in _ORACLE and _oracle_decimal_float(source_type) and dest not in _ORACLE:
        if _base(target_type) in _BINARY64 or tgt_l == "float":
            return (
                f"Oracle {source_type} keeps up to 38 significant decimal digits (binary "
                f"precision 126); {target_type} is a binary double of 15 to 17 digits"
            )
        if tgt_l == "decimal":
            from services.type_system import parse_numeric_precision_scale

            precision, scale = parse_numeric_precision_scale(target_type)
            if precision is not None and not _decimals_fit(population, precision, scale or 0):
                return (
                    f"Oracle {source_type} ranges to 1E+125 with 38 significant digits; "
                    f"{target_type} keeps {precision - (scale or 0)} integer and {scale or 0} "
                    "fraction digits"
                )
    if engine in _POSTGRES and base == "MONEY" and dest not in _PG_ONLY_MONEY:
        from services.type_system import parse_numeric_precision_scale

        tgt_base = _base(target_type)
        if tgt_base in {"MONEY", "SMALLMONEY"}:
            return (
                f"PostgreSQL money reaches 92233720368547758.07; {target_type} stops at "
                f"{'922337203685477.5807' if tgt_base == 'MONEY' else '214748.3647'}"
            )
        if tgt_l == "decimal":
            precision, scale = parse_numeric_precision_scale(target_type)
            if precision is not None and precision - (scale or 0) < 17:
                return (
                    "PostgreSQL money reaches 92233720368547758.07 (17 integer digits); "
                    f"{target_type} keeps {precision - (scale or 0)}"
                )
    if engine in _POSTGRES and base in {"NUMERIC", "DECIMAL"} and "(" not in (source_type or ""):
        from services.type_system import parse_numeric_precision_scale

        if tgt_l == "decimal":
            precision, scale = parse_numeric_precision_scale(target_type)
            if precision is not None and not _decimals_fit(population, precision, scale or 0):
                return (
                    f"PostgreSQL numeric has no declared precision; {target_type} keeps "
                    f"{precision - (scale or 0)} integer and {scale or 0} fraction digits"
                )
    return ""


def _decimals_fit(population: Any, precision: int, scale: int) -> bool:
    """True only when measured cells all fit ``DECIMAL(precision, scale)``."""
    from decimal import Decimal, InvalidOperation

    if population is None:
        return False
    try:
        values = list(population)
    except TypeError:
        return False
    measured = False
    for value in values:
        if value is None or (isinstance(value, str) and not value.strip()):
            continue
        try:
            number = Decimal(str(value).strip())
        except (InvalidOperation, ValueError):
            return False
        if not number.is_finite():
            return False
        measured = True
        sign, digits, exponent = number.as_tuple()
        frac = max(0, -int(exponent))
        whole = max(0, len(digits) + int(exponent))
        if frac > scale or whole > precision - scale:
            return False
    return measured
