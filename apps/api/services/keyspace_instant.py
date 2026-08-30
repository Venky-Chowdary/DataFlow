"""Keyspace stores: an aware datetime lands as RFC 3339 text, offset intact.

Split out of ``services.type_system`` (a god module at its size budget), beside
``services.document_instant``. Type-system imports are deferred inside the
functions: ``type_system`` re-exports these names, so a module-level import
would be circular.

Redis has no type system at all — a row lands as one JSON document written
through ``services.value_serializer``, which renders an aware ``datetime`` as
``2024-12-31T23:59:59.123456+05:30``. The instant survives, and so does the
original offset and the microseconds, which is more than a typed millisecond
carrier keeps. Reading the untyped ``string`` carrier as an open-text collapse
made every timestamp column into this destination demand a signed Risk Contract
for a loss the wire does not have.

Only engines whose write path is proven to serialize through that helper belong
here: the exemption is about the wire, not about the engine being schemaless.
"""

from __future__ import annotations

from typing import Final

#: Engines whose rows are written as one JSON document per key through
#: ``services.value_serializer`` (``connectors.redis_writer.write_mapped_rows``).
INSTANT_TEXT_WIRE_ENGINES: Final[frozenset[str]] = frozenset({"redis"})


def keyspace_instant_text_wire_preserved(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when an offset-bearing source lands intact as RFC 3339 text.

    The destination must be one of ``INSTANT_TEXT_WIRE_ENGINES`` and the carrier
    an open text/string one — the only carrier those engines have. A zoneless
    source is excluded: it has no offset to keep, so the rules that name UTC
    invent still fire.
    """
    from services.type_system import (
        LOGICAL_DATETIME,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_TIME,
        datetime_timezone_polarity,
        normalize_logical_type,
        time_timezone_polarity,
    )

    if (dest_db or "").strip().lower() not in INSTANT_TEXT_WIRE_ENGINES:
        return False
    if normalize_logical_type(target_type) not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    if normalize_logical_type(source_type) not in {LOGICAL_DATETIME, LOGICAL_TIME}:
        return False
    return datetime_timezone_polarity(source_type) in {"tz", "ltz"} or (
        time_timezone_polarity(source_type) == "tz"
    )
