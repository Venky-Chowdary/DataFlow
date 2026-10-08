"""Document-store ``date``: an instant carrier that shares SQL DATE's spelling.

Split out of ``services.type_system`` (a god module at its size budget), beside
``services.identity_fit``. Type-system imports are deferred inside the functions:
``type_system`` re-exports these names, so a module-level import would be
circular.

MongoDB, DocumentDB, CosmosDB and the Elasticsearch date type all store a single
temporal value — a 64-bit count of milliseconds since the epoch. They have no
date-only type, so the DDL table stamps the same ``date`` token for a logical
date and a logical datetime alike. Reading that token as SQL ``DATE`` says the
time of day is dropped, which is not what happens and is why the reconcile
fingerprint has always resolved it as an instant instead.

What the carrier genuinely cannot do is hold sub-millisecond precision, or hold
an instant for a source that never had a zone.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Final

# Engines whose bare ``date`` token stores an instant (BSON date / Elasticsearch
# date), not a calendar date.
INSTANT_DATE_TOKEN_ENGINES: Final[frozenset[str]] = frozenset(
    {"mongodb", "documentdb", "cosmosdb", "elasticsearch", "opensearch"}
)

#: Anything finer than 3 fractional digits is truncated on write. Proven by
#: round-trip: a datetime carrying microseconds comes back without them.
DOCUMENT_INSTANT_FRACTIONAL_DIGITS: Final[int] = 3


def is_document_instant_token(engine: str | None, ddl_type_token: str | None) -> bool:
    """True for a document store's temporal token — an instant, not a calendar day.

    The store has one temporal carrier, so ``date``, ``timestamp`` and whatever
    spelling a sampler stamped all name it. Matching only ``date`` left a
    collection introspected as ``TIMESTAMP`` outside every document-instant
    rule, including the one that asks a zoneless source for its zone before the
    writer refuses the rows.
    """
    from services.dest_dialect_facts import _normalize_dest_db
    from services.type_system import (
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        normalize_logical_type,
        strip_identity_qualifier,
    )

    raw_engine = (engine or "").strip().lower()
    # Catalog aliases (``mongo``, ``cosmos``, ``amazon_elasticsearch``) are the
    # same carrier. Normalization is the SSOT for those names. Firestore shares
    # the mongodb DDL bucket and is not a BSON date, so it stays out.
    if raw_engine == "firestore":
        return False
    canonical = _normalize_dest_db(raw_engine) if raw_engine else ""
    if (
        raw_engine not in INSTANT_DATE_TOKEN_ENGINES
        and canonical not in INSTANT_DATE_TOKEN_ENGINES
    ):
        return False
    token = strip_identity_qualifier(ddl_type_token).upper().strip()
    return normalize_logical_type(token) in {LOGICAL_DATE, LOGICAL_DATETIME}


def promote_document_instant_create_target(
    source_type: str,
    target_type: str,
    *,
    dest_db: str,
    source_db: str,
) -> str:
    """Create-new stamp that keeps a BSON datetime's milliseconds.

    An echoed ``TIMESTAMP`` on MySQL is fractional-digits 0. The source token
    has no typmod and used to look identical, so preflight called the pair a
    fidelity collapse — or, when it did not, the write dropped every
    millisecond. Live destination columns are not rewritten here.
    """
    from services.source_engine_scope import bind_source_engine
    from services.type_system import temporal_precision_would_narrow

    src = (source_type or "").strip()
    tgt = (target_type or "").strip()
    if not src or not tgt or not dest_db:
        return tgt
    with bind_source_engine(source_db or ""):
        if not temporal_precision_would_narrow(src, tgt, dest_db=dest_db):
            return tgt
        from services.decision_kernel.type_invent import create_new_mapping_target_type

        upgraded = create_new_mapping_target_type(src, dest_db, source_db=source_db or "")
    return (upgraded or tgt).strip() or tgt


def transform_narrows_to_calendar_day(transform: str | None) -> bool:
    """True only when the mapping explicitly asked for a calendar-day narrow.

    On a carrier that holds an instant, midnight-truncation is a decision, never
    a consequence of the token's spelling. Only the ``date`` transform states it;
    every other transform (identity, a declared zone, a parse) leaves the time of
    day to be written, so testing for one named transform instead of listing the
    instant-bearing ones is what keeps a new transform from silently truncating.
    """
    return (transform or "").strip().lower() == "date"


def source_fractional_digits(source_type: str) -> int | None:
    """Fractional-second digits the source column holds, engine defaults included.

    A bare spelling is not "no precision": PostgreSQL ``timestamptz`` keeps
    microseconds and SQL Server ``datetime2`` keeps seven digits. Reading only
    an explicit typmod passed every PostgreSQL instant onto a millisecond
    carrier while the write dropped its microseconds. An unnamed engine leaves a
    bare spelling unknown (``None``); the writer still refuses any cell it would
    truncate.
    """
    from services.source_engine_scope import active_source_engine
    from services.type_system import (
        destination_temporal_fractional_digits,
        parse_temporal_fractional_precision,
    )

    explicit = parse_temporal_fractional_precision(source_type)
    if explicit is not None:
        return int(explicit)
    engine = (active_source_engine() or "").strip()
    if not engine:
        return None
    return destination_temporal_fractional_digits(source_type, dest_db=engine)


def cell_exceeds_document_instant(value: object) -> bool:
    """True when a cell carries a non-zero digit below the millisecond.

    The per-cell rule the writer enforces and the population check measures.
    Trailing zeros are not precision: ``10:00:00.120000`` lands intact.
    """
    from services.type_system import value_fractional_second_digits

    return value_fractional_second_digits(value) > DOCUMENT_INSTANT_FRACTIONAL_DIGITS


def population_fits_document_instant(population: object) -> bool | None:
    """True when every measured cell fits milliseconds, False when one does not.

    ``None`` means nothing was measured (no population, or only SQL NULL): a
    sample that was never taken is not proof the column holds whole
    milliseconds.
    """
    if population is None:
        return None
    try:
        values = list(population)  # type: ignore[call-overload]
    except TypeError:
        return None
    measured = False
    for value in values:
        if value is None or (isinstance(value, str) and not value.strip()):
            continue
        measured = True
        if cell_exceeds_document_instant(value):
            return False
    return True if measured else None


def document_instant_wire_preserved(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
    population: object = None,
) -> bool:
    """True when a date/datetime source lands intact on a document instant.

    The carrier holds an instant, so an offset-bearing source keeps it and the
    time of day survives — the collapse this used to report, from the token
    sharing a name with SQL ``DATE``, does not happen.

    Two things are genuinely not preserved and are excluded here so the rules
    that name them precisely still fire: sub-millisecond precision, which the
    64-bit millisecond carrier truncates, and a zoneless source, which has no
    instant to preserve. Stamping one is the UTC invent the MongoDB writer
    refuses, and ``resolve_timezone_policy`` calls it POLICY_UTC_INVENT and
    requires an operator contract.

    A source declaring more than three digits is preserved only when the
    measured ``population`` holds whole milliseconds — the carrier then loses
    nothing those rows contain, and the writer refuses any later cell that
    does not. Unmeasured, the declaration stands and the pair is a truncation.
    """
    from services.type_system import (
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        datetime_timezone_polarity,
        normalize_logical_type,
    )

    if not is_document_instant_token(dest_db, target_type):
        return False
    if normalize_logical_type(source_type) not in {LOGICAL_DATE, LOGICAL_DATETIME}:
        return False
    if datetime_timezone_polarity(source_type) == "ntz":
        return False
    src_p = source_fractional_digits(source_type)
    if src_p is None or src_p <= DOCUMENT_INSTANT_FRACTIONAL_DIGITS:
        return True
    return population_fits_document_instant(population) is True


def document_instant_utc_invent(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when landing this column has to stamp a zone the source never proved.

    The single predicate the writer enforces, so Validate can demand the same
    contract instead of letting the run discover it: a zoneless *datetime* on an
    instant-only carrier. A calendar day is excluded — it carries no time of day,
    so UTC midnight invents nothing an operator has to accept.
    """
    from services.type_system import (
        LOGICAL_DATETIME,
        datetime_timezone_polarity,
        normalize_logical_type,
    )

    if not is_document_instant_token(dest_db, target_type):
        return False
    if normalize_logical_type(source_type) != LOGICAL_DATETIME:
        return False
    return datetime_timezone_polarity(source_type) == "ntz"


@lru_cache(maxsize=8192)
def instant_date_carrier(engine: str | None, ddl_type_token: str | None) -> str:
    """Return the carrier to bind/fingerprint ``ddl_type_token`` against.

    Identity for SQL engines. A document store has exactly one temporal carrier
    and it is an instant, so *every* temporal spelling there — ``date``,
    ``timestamp``, ``timestamp_ntz`` as a sampler may have stamped it — resolves
    to ``TIMESTAMPTZ``. Resolving only the bare ``date`` token left an
    introspected ``TIMESTAMP`` reading as zoneless, so the timezone policy saw
    naive→naive and asked for no contract while the writer refused every naive
    row: Validate green, Run quarantining the whole batch.
    """
    from services.type_system import (
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        normalize_logical_type,
    )

    token = (ddl_type_token or "").strip()
    if (engine or "").strip().lower() not in INSTANT_DATE_TOKEN_ENGINES:
        return token
    if normalize_logical_type(token) in {LOGICAL_DATE, LOGICAL_DATETIME}:
        return "TIMESTAMPTZ"
    return token
