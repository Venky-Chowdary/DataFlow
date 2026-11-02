"""Least-upper-bound resolution for columns whose type is observed, not declared.

Schemaless sources — DynamoDB, MongoDB, Redis, object payloads — have no catalog
to declare a column's type, so the type is whatever the values happen to be.
Every one of those connectors had its own answer to "given these observations,
what is the column?", and each answered by **majority vote**: a column of 999
integers and one ``2000.50`` resolved to INTEGER, and the write then failed that
row with "Invalid integer". The larger the table, the more certain it was that
the minority value would be mistyped — a defect that scales the wrong way.

A column type must therefore be a **join**, never a vote: the narrowest carrier
that holds *every* value observed. This module owns that lattice so the answer
is one answer, and so a new source only has to describe what it saw.

The lattice
-----------
``BOOLEAN < INTEGER < DECIMAL < FLOAT``
    Booleans are 0/1; integers are exact decimals. FLOAT sits above DECIMAL
    rather than beside it because an observed IEEE value means the field's
    domain is already approximate — declaring DECIMAL there would promise an
    exactness the data does not have. (Above 2^53 an integer no longer round
    trips through a float; that is a genuine conflict, not something a wider
    carrier can fix, and it is left to the write-time quarantine to catch.)

``DATE < TIMESTAMP < TIMESTAMPTZ``
    A date is a timestamp at midnight. Naive and aware timestamps do not
    strictly order, but the aware carrier is chosen because stamping a zone on
    a naive value is recoverable under a documented convention while dropping a
    zone is not — ``services.timezone_policy`` names that policy and requires a
    contract for it.

``ARRAY, OBJECT < JSON``
    A document carrier holds either shape. ``ARRAY`` and ``OBJECT`` are
    siblings rather than ordered, so their join is the carrier above both.

``TEXT`` is the top
    Two carriers from different families (a number and a timestamp, say) have
    no common type but text, which holds every serialization without loss. That
    is a widening, never a narrowing. JSON is deliberately *not* treated as an
    upper bound of scalars even though a number is valid JSON: landing a
    numeric field in a JSON column changes how it is queried, while text does
    not pretend to be structured.

Why the shape matters
---------------------
Families are disjoint chains under one top, which makes the join commutative,
idempotent **and associative**. Associativity is not decoration here: a source
is read in pages, so the type is folded across batches, and an operation that
depended on fold order would give one answer for a table read in one page and
another for the same table read in three.

What this module deliberately does not decide
---------------------------------------------
Whether a textual value among typed ones is a *sentinel* (``"N/A"`` in a numeric
field, which should be quarantined rather than widen the whole column to text)
is a per-source policy question about data quality, not a question about how
carriers combine. Callers apply that policy first and hand the typed remainder
here — see ``schema_introspect._finalize_mongodb_type_with_note``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final

#: Carriers that hold any value once serialized — the top of the lattice.
TEXTUAL_TYPES: Final[frozenset[str]] = frozenset({"TEXT", "VARCHAR", "STRING"})

#: Document carriers. ``JSON`` is their join and also absorbs scalars.
STRUCTURAL_TYPES: Final[frozenset[str]] = frozenset({"OBJECT", "ARRAY", "JSON", "MAP", "STRUCT"})

#: ``carrier -> (family, rank)``. Within a family a higher rank holds every
#: value of a lower one. Two distinct carriers at the *same* rank are siblings
#: (``ARRAY`` and ``OBJECT``), and join to their family's top.
_FAMILIES: Final[dict[str, tuple[str, int]]] = {
    "BOOLEAN": ("numeric", 0),
    "INTEGER": ("numeric", 1),
    "DECIMAL": ("numeric", 2),
    "FLOAT": ("numeric", 3),
    "DATE": ("temporal", 0),
    "TIMESTAMP": ("temporal", 1),
    "TIMESTAMPTZ": ("temporal", 2),
    "ARRAY": ("structural", 0),
    "OBJECT": ("structural", 0),
    "MAP": ("structural", 0),
    "STRUCT": ("structural", 0),
    "JSON": ("structural", 1),
    "VARCHAR": ("textual", 0),
    "STRING": ("textual", 0),
    "TEXT": ("textual", 1),
}

#: The carrier that sits above every member of a family. Numeric and temporal
#: families are total chains, so they never need one.
_FAMILY_TOP: Final[dict[str, str]] = {"structural": "JSON", "textual": "TEXT"}

#: Spellings that mean the same carrier to this lattice. Keeping them here means
#: a connector can report its own dialect's word without every caller
#: normalizing first.
_ALIASES: Final[dict[str, str]] = {
    "BOOL": "BOOLEAN",
    "INT": "INTEGER",
    "BIGINT": "INTEGER",
    "SMALLINT": "INTEGER",
    "LONG": "INTEGER",
    "NUMERIC": "DECIMAL",
    "DOUBLE": "FLOAT",
    "REAL": "FLOAT",
    "TIMESTAMP_NTZ": "TIMESTAMP",
    "DATETIME": "TIMESTAMP",
    "TIMESTAMP_TZ": "TIMESTAMPTZ",
    "TIMESTAMP_LTZ": "TIMESTAMPTZ",
    "BYTES": "BINARY",
    "BLOB": "BINARY",
}

_TOP: Final[str] = "TEXT"


def canonical_logical(carrier: str | None) -> str:
    """Normalize a carrier spelling to the name this lattice reasons about."""
    token = str(carrier or "").strip().upper()
    if not token:
        return ""
    return _ALIASES.get(token, token)


def join_logical_types(left: str | None, right: str | None) -> str:
    """Narrowest carrier that holds every value of both — never a narrowing.

    Unknown carriers are not guessed at: an unrecognised name joined with
    anything but itself lands on text rather than being assumed compatible.
    """
    a = canonical_logical(left)
    b = canonical_logical(right)
    if not a:
        return b or ""
    if not b:
        return a
    if a == b:
        return a

    left_family = _FAMILIES.get(a)
    right_family = _FAMILIES.get(b)
    if left_family is None or right_family is None:
        # At least one carrier this lattice does not model (BINARY, UUID, a
        # dialect type nobody has mapped yet). Only text is safe.
        return _TOP
    family, left_rank = left_family
    if family != right_family[0]:
        return _TOP

    right_rank = right_family[1]
    if left_rank == right_rank:
        # Siblings: ARRAY and OBJECT are both documents, neither holds the
        # other, so the answer is the carrier above both.
        return _FAMILY_TOP.get(family, _TOP)
    return a if left_rank > right_rank else b


def resolve_observed_types(observed: Mapping[str, int] | Iterable[str]) -> str:
    """Join every carrier observed for one column into a single type.

    Accepts either the vote counts a connector accumulated or a bare iterable of
    carriers; the counts are ignored on purpose, because *how often* a value
    appeared has no bearing on whether the column must hold it.

    Returns ``""`` when nothing was observed, so a null-only column stays
    unknown instead of inventing a carrier.
    """
    if isinstance(observed, Mapping):
        names = [str(k) for k, count in observed.items() if k and count]
    else:
        names = [str(k) for k in observed if k]
    resolved = ""
    for name in names:
        resolved = join_logical_types(resolved, name)
    return resolved
