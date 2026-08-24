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
    from services.type_system import (
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        normalize_logical_type,
        strip_identity_qualifier,
    )

    if (engine or "").strip().lower() not in INSTANT_DATE_TOKEN_ENGINES:
        return False
    token = strip_identity_qualifier(ddl_type_token).upper().strip()
    return normalize_logical_type(token) in {LOGICAL_DATE, LOGICAL_DATETIME}


def transform_narrows_to_calendar_day(transform: str | None) -> bool:
    """True only when the mapping explicitly asked for a calendar-day narrow.

    On a carrier that holds an instant, midnight-truncation is a decision, never
    a consequence of the token's spelling. Only the ``date`` transform states it;
    every other transform (identity, a declared zone, a parse) leaves the time of
    day to be written, so testing for one named transform instead of listing the
    instant-bearing ones is what keeps a new transform from silently truncating.
    """
    return (transform or "").strip().lower() == "date"


def document_instant_wire_preserved(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
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
    """
    from services.type_system import (
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        datetime_timezone_polarity,
        normalize_logical_type,
        parse_temporal_fractional_precision,
    )

    if not is_document_instant_token(dest_db, target_type):
        return False
    if normalize_logical_type(source_type) not in {LOGICAL_DATE, LOGICAL_DATETIME}:
        return False
    if datetime_timezone_polarity(source_type) == "ntz":
        return False
    src_p = parse_temporal_fractional_precision(source_type)
    if src_p is None:
        return True
    return int(src_p) <= DOCUMENT_INSTANT_FRACTIONAL_DIGITS


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
