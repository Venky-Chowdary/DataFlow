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
    """True for a document store's ``date`` token — an instant, not a calendar day."""
    from services.type_system import strip_identity_qualifier

    if (engine or "").strip().lower() not in INSTANT_DATE_TOKEN_ENGINES:
        return False
    return strip_identity_qualifier(ddl_type_token).upper().strip() == "DATE"


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


@lru_cache(maxsize=8192)
def instant_date_carrier(engine: str | None, ddl_type_token: str | None) -> str:
    """Return the carrier to bind/fingerprint ``ddl_type_token`` against.

    Identity for SQL engines. On document stores the bare ``date`` token stores
    an offset-normalized instant, so it resolves to ``TIMESTAMPTZ`` — an
    offset-bearing wire keeps its instant instead of the wall clock a bare
    ``TIMESTAMP`` bind would preserve.
    """
    token = (ddl_type_token or "").strip()
    if (engine or "").strip().lower() not in INSTANT_DATE_TOKEN_ENGINES:
        return token
    return "TIMESTAMPTZ" if token.upper() == "DATE" else token
