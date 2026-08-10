"""Runtime numeric-fit SSOT: what a physical column can actually hold.

Split out of ``services.type_system`` (a god module already over its size
budget) because these are *write-time fit* questions, not catalog questions:

* ``integer_storage_bounds`` — the inclusive range a value must satisfy before
  a writer binds it, resolving the ambiguous ``INT``/``INTEGER`` keyword per
  destination engine.
* ``dest_lacks_fixed_point_decimal`` / ``decimal_fixed_point_would_collapse_to_text``
  — whether a DECIMAL→TEXT stamp is a fidelity collapse or simply the most
  faithful carrier the destination engine offers.

Type-system imports are deferred inside the functions: ``type_system`` re-exports
these names, so a module-level import would be circular.
"""

from __future__ import annotations

import re

# Storage width of the ambiguous ``INT``/``INTEGER`` keyword per engine.
# ``None`` means the engine backs the keyword with a big-decimal carrier
# (Snowflake NUMBER(38,0), Oracle NUMBER) — no integer bounds to enforce.
_BARE_INT_STORAGE_WIDTH: dict[str, int | None] = {
    "sqlite": 64,      # INTEGER affinity holds up to 8 bytes
    "bigquery": 64,    # INT64
    "databricks": 32,
    "snowflake": None,
    "oracle": None,
    # Schemaless / document / object sinks have no fixed integer column, so a
    # bare ``INTEGER`` Map stamp must not invent a 32-bit bound and quarantine
    # values the sink stores natively (BSON int64, JSON number, Avro long).
    "mongodb": None,
    "dynamodb": None,
    "elasticsearch": None,
    "redis": None,
    "kafka": None,
    "s3": None,
    "gcs": None,
    "adls": None,
    "salesforce": None,
    "hubspot": None,
    "airtable": None,
    "notion": None,
    "": 32,            # SQL standard INT — Postgres/MySQL/SQL Server/DuckDB/…
}


def integer_storage_bounds(
    inferred: str | None, *, dest_db: str = ""
) -> tuple[int, int] | None:
    """Inclusive (lo, hi) a value must satisfy for an integer carrier, else None.

    Unlike ``integer_bit_width`` — which reports ``None`` for the ambiguous
    ``INT``/``INTEGER`` keyword so create-new invent never narrows — this is the
    *runtime fit* SSOT: a write must know what the physical column actually
    holds. The ambiguous keyword resolves per engine (SQL standard 32-bit,
    SQLite/BigQuery 64-bit, Snowflake/Oracle decimal carrier → unbounded).

    ``None`` means "no integer bound enforceable" and callers must not
    quarantine on width (e.g. ``BIGINT UNSIGNED``, which writers carry as
    DECIMAL, or an unknown carrier). Never invent a bound — a wrong bound
    quarantines good rows.
    """
    from services.type_system import (
        LOGICAL_INTEGER,
        _normalize_dest_db,
        integer_bit_width,
        normalize_logical_type,
    )

    if normalize_logical_type(inferred) != LOGICAL_INTEGER:
        return None
    raw = (inferred or "").strip()
    if raw in {LOGICAL_INTEGER, "int", "integer"}:
        # Bare *logical* stamp (not a physical carrier) — width unknown.
        return None
    upper = raw.upper()
    unsigned = "UNSIGNED" in upper or bool(re.search(r"\bUINT\d*\b", upper))
    width = integer_bit_width(inferred)
    if width is None:
        if not re.search(r"\b(INT|INTEGER)\b", upper):
            # Unknown integer carrier — refuse to invent a fit bound.
            return None
        db = _normalize_dest_db(dest_db) if dest_db else ""
        bare = _BARE_INT_STORAGE_WIDTH.get(db, _BARE_INT_STORAGE_WIDTH[""])
        if bare is None:
            return None
        # ``integer_bit_width`` convention: UNSIGNED carries nominal width + 1.
        width = bare + 1 if unsigned else bare
    if unsigned:
        nominal = max(1, width - 1)
        if nominal >= 64:
            # UINT64 exceeds every signed integer bind — DECIMAL carrier owns it.
            return None
        return (0, (1 << nominal) - 1)
    return (-(1 << (width - 1)), (1 << (width - 1)) - 1)


def unsigned_bare_int_fits_signed_target(source_type: str, target_type: str) -> bool:
    """True when a bare ``INT UNSIGNED`` provably fits the signed target.

    ``integer_bit_width`` reports ``None`` for the ambiguous ``INT``/``INTEGER``
    keyword, so overflow checks fail closed on width-unknown carriers. The
    ``UNSIGNED`` qualifier removes the ambiguity in practice: it is MySQL /
    MariaDB syntax (ClickHouse spells it ``UInt32``), and there ``INT`` is
    exactly 32 bits, so the domain caps at 2^32-1. Any signed 64-bit sink holds
    that range, which makes ``INT UNSIGNED → BIGINT`` — the most common
    MySQL→warehouse widening — a false positive when it is blocked.

    Only *bare* ``INT``/``INTEGER UNSIGNED`` qualifies; ``BIGINT UNSIGNED`` and
    unknown carriers keep failing closed.
    """
    from services.type_system import integer_bit_width, strip_identity_qualifier

    upper = (strip_identity_qualifier(source_type) or "").upper()
    if "UNSIGNED" not in upper:
        return False
    compact = re.sub(r"\s+", " ", re.sub(r"\bUNSIGNED\b", "", upper)).strip()
    if compact not in {"INT", "INTEGER"}:
        return False
    tgt_w = integer_bit_width(target_type)
    return tgt_w is not None and tgt_w >= 64


def dest_lacks_fixed_point_decimal(dest_db: str) -> bool:
    """True when the destination engine has no fixed-point DECIMAL carrier.

    SQLite (and file sinks) genuinely cannot declare DECIMAL(p,s): the catalog
    itself maps logical ``decimal`` to TEXT there. Exact digits survive as text;
    only the arithmetic domain is unavailable — and no alternative exists on
    that engine, so this is the *most* faithful carrier, not an avoidable
    collapse. Engines that do have DECIMAL keep the collapse verdict.
    """
    from services.type_system import (
        DDL_TYPES,
        DEFAULT_DDL,
        LOGICAL_DECIMAL,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        _DECIMAL_PARAM_TEMPLATES,
        _normalize_dest_db,
        normalize_logical_type,
    )

    db = _normalize_dest_db(dest_db) if dest_db else ""
    if not db:
        return False
    if db in _DECIMAL_PARAM_TEMPLATES:
        return False
    carrier = DDL_TYPES.get(db, {}).get(LOGICAL_DECIMAL) or DEFAULT_DDL.get(db)
    if not carrier:
        # Untyped sink (CSV/JSON/Excel) — no declared column type to collapse.
        return True
    return normalize_logical_type(carrier) in {LOGICAL_STRING, LOGICAL_TEXT}


def decimal_fixed_point_would_collapse_to_text(
    source_type: str, target_type: str, *, dest_db: str = ""
) -> bool:
    """True when fixed-point DECIMAL collapses to open TEXT/STRING.

    Create-new may stamp TEXT when (p,s) exceeds the destination DECIMAL cap
    (e.g. ClickHouse Decimal256→MySQL). That preserves digits as strings but
    drops fixed-point polarity — Accept risk, never silent green.

    Not a collapse when the destination engine has no fixed-point carrier at
    all (SQLite, untyped file sinks): TEXT is then the exact-digit carrier our
    own DDL catalog picks, and REAL would be the lossy alternative.
    """
    from services.type_system import (
        LOGICAL_DECIMAL,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        is_decfloat_carrier,
        normalize_logical_type,
    )

    if normalize_logical_type(source_type) != LOGICAL_DECIMAL:
        return False
    if is_decfloat_carrier(source_type):
        return False
    if normalize_logical_type(target_type) not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    return not dest_lacks_fixed_point_decimal(dest_db)


def float_mantissa_bits(inferred: str | None, *, dest_db: str = "") -> int | None:
    """IEEE significand bits for float carriers (53=double, 24=single, 11=half).

    Property 1 — referential transparency: bare ``FLOAT`` / logical ``float``
    (any case) returns ``None`` (width unknown) so create-new invents via
    ``DDL_TYPES`` / ``_FLOAT_DDL`` (IEEE-64 default). Unambiguous single-
    precision carriers are ``REAL`` / ``FLOAT4`` / ``FLOAT32`` / ``FLOAT(p≤24)``.
    Introspect must emit those for true IEEE-32 sources (e.g. MySQL FLOAT).

    ``REAL`` is the one carrier whose width is genuinely dialect-defined:
    SQLite ``REAL`` is an 8-byte IEEE-754 double and MySQL ``REAL`` is a
    synonym for ``DOUBLE`` (outside ``REAL_AS_FLOAT``), while PostgreSQL /
    SQL Server ``REAL`` is ``float4``. ``dest_db`` selects that width so a
    ``DOUBLE PRECISION → SQLite REAL`` create-new wire is not reported as an
    IEEE narrowing it never performs. Bare FLOAT stays width-unknown.
    """
    from services.type_system import (
        LOGICAL_FLOAT,
        REAL_IS_DOUBLE_DIALECTS,
        _normalize_dest_db,
        normalize_logical_type,
        strip_identity_qualifier,
    )

    dialect = _normalize_dest_db(dest_db) if dest_db else ""
    if normalize_logical_type(inferred) != LOGICAL_FLOAT:
        return None
    raw = strip_identity_qualifier(inferred)
    # Bare logical family token (any case of ``float``) — width unknown.
    if raw.strip().lower() == LOGICAL_FLOAT:
        return None
    # Strip UNSIGNED so REAL UNSIGNED / FLOAT UNSIGNED keep single-width tokens.
    upper = re.sub(
        r"\bUNSIGNED\b",
        "",
        raw.upper(),
    ).strip().replace(" ", "")
    # IEEE half / float16 (~10 explicit + 1 implicit significand bits).
    if upper in {"HALF", "HALFFLOAT", "FLOAT16"} or upper.startswith("HALFFLOAT"):
        return 11
    # ``REAL`` is IEEE-64 on SQLite and MySQL, IEEE-32 everywhere else.
    if (upper == "REAL" or upper.startswith("REAL(")) and dialect in REAL_IS_DOUBLE_DIALECTS:
        return 53
    # Single-precision tokens (unambiguous).
    if upper in {
        "REAL",
        "FLOAT4",
        "FLOAT32",
        "BINARY_FLOAT",
    } or upper.startswith("REAL("):
        return 24
    # SQL FLOAT(p): p≤24 → single; p>24 → double (SQL Server / ANSI).
    m = re.match(r"^FLOAT\((\d+)\)$", upper)
    if m:
        return 24 if int(m.group(1)) <= 24 else 53
    if upper in {
        "DOUBLE",
        "DOUBLEPRECISION",
        "FLOAT8",
        "FLOAT64",
        "BINARY_DOUBLE",
    } or upper.startswith("DOUBLE"):
        return 53
    # Bare FLOAT (any case) — ambiguous; invent IEEE-64 via DDL_TYPES.
    if upper == "FLOAT" or upper.startswith("FLOAT"):
        return None
    return 53


def float_mantissa_would_narrow(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
    source_db: str = "",
) -> bool:
    """True when DOUBLE/FLOAT64 lands on REAL/FLOAT32/HALF (silent IEEE drop).

    Widths are resolved per side: the source carrier under ``source_db`` and the
    target carrier under ``dest_db``. Borrowing the sink dialect for the source
    would read a PostgreSQL ``REAL`` (float4) as a SQLite double.
    """
    src_b = float_mantissa_bits(source_type, dest_db=source_db)
    tgt_b = float_mantissa_bits(target_type, dest_db=dest_db)
    if src_b is None or tgt_b is None:
        return False
    return src_b > tgt_b
