"""Full-population digests computed by the engine instead of by Python.

Gate-8 compares a source digest against a destination digest. It never compares
either one against a constant, so the two sides only have to agree with *each
other* — which means the digest does not have to be computed in Python at all.
Profiling a PostgreSQL-to-PostgreSQL transfer put 59 seconds of an 87 second run
inside per-cell fingerprinting, so moving that work into the engines removes the
single largest cost in a run while keeping the population coverage that
distinguishes this product from row-count validation.

The digest is order independent by construction: each row is hashed on its own
and the row hashes are summed, so neither side has to sort and the comparison
holds whatever order the scan returns.

This path is only correct when both sides render a value to text identically,
which is why :func:`engine_checksum_comparable` demands the same engine family
*and* the same declared type per column. An unconstrained ``numeric`` holding
``150.250`` and a ``numeric(12,2)`` holding ``150.25`` are the same number and
different text, and a text digest would call that a mismatch. Where the
precondition does not hold the caller keeps the Python fingerprint path, which
knows how to make those two equal.
"""

from __future__ import annotations

import logging
from typing import Any, Final, NamedTuple

logger = logging.getLogger(__name__)

#: Engines whose bare ``::text`` rendering this module knows how to reproduce on
#: both sides. PostgreSQL-family only for now — every other engine needs its own
#: proof that the rendering is stable before it may claim this path.
_SUPPORTED_ENGINES: Final[frozenset[str]] = frozenset(
    {"postgresql", "postgres", "pg", "timescaledb", "alloydb", "supabase"}
)

#: Hex width of an md5 digest. Every field is reduced to exactly this many
#: characters before the row is assembled, which is what makes the row rendering
#: injective without needing a separator: a value cannot forge a field boundary
#: because there are no boundaries to forge, only fixed-width blocks.
#:
#: A separator would have been simpler and wrong. Any byte chosen as a separator
#: can legitimately appear inside a text column, and the moment it does,
#: ``('a|b', 'c')`` and ``('a', 'b|c')`` digest the same. PostgreSQL text cannot
#: even hold the NUL the Python path uses, so this side needs its own answer.
_FIELD_WIDTH: Final[int] = 32


class EngineChecksum(NamedTuple):
    """A population digest and the cardinality it covers."""

    row_count: int
    checksum: str
    scope: str


def normalize_engine(engine: str | None) -> str:
    return (engine or "").strip().lower()


def engine_supports_checksum(engine: str | None) -> bool:
    return normalize_engine(engine) in _SUPPORTED_ENGINES


def engine_checksum_comparable(
    source_engine: str | None,
    dest_engine: str | None,
    source_types: dict[str, str] | None,
    dest_types: dict[str, str] | None,
    columns: list[str] | None,
) -> bool:
    """True when both sides will render every mapped column to the same text.

    Requires the same supported engine on both ends and a declared type on both
    ends that matches per column. Anything else — a widened carrier, a differing
    precision, a column whose type is unknown — is left to the Python path,
    which compares values rather than their spelling.
    """
    src = normalize_engine(source_engine)
    if src != normalize_engine(dest_engine) or not engine_supports_checksum(src):
        return False
    if not columns:
        return False
    src_types = {str(k).lower(): str(v or "").strip().lower() for k, v in (source_types or {}).items()}
    dst_types = {str(k).lower(): str(v or "").strip().lower() for k, v in (dest_types or {}).items()}
    for col in columns:
        key = str(col).lower()
        left = src_types.get(key)
        right = dst_types.get(key)
        if not left or not right or left != right:
            return False
    return True


def postgresql_checksum_sql(table_ref: str, columns: list[str]) -> str:
    """``SELECT count, digest`` over every row of ``table_ref``.

    Each row renders to one text value, is hashed with md5, and the leading 64
    bits are summed as ``numeric``. Summing rather than aggregating in order is
    what makes the result independent of scan order; ``numeric`` rather than
    ``bigint`` is what keeps a wide table from overflowing the accumulator.
    """
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    fields = []
    for col in columns:
        quoted = quote_sql_identifier(require_safe_identifier(col, preserve_case=True))
        # The IS NULL flag rides inside the hashed text so that NULL and the
        # empty string cannot digest alike — the same distinction the Python
        # fingerprint path draws, and one that has already been a real bug here.
        fields.append(
            f"md5(coalesce({quoted}::text, '') || ({quoted} IS NULL)::text)"
        )
    row_text = " || ".join(fields)
    return (
        "SELECT count(*)::bigint AS n, "
        f"coalesce(sum(('x' || substr(md5({row_text}), 1, 15))::bit(60)::bigint::numeric), 0) AS digest "
        f"FROM {table_ref}"  # nosec B608 — identifiers quoted above
    )


def postgresql_engine_checksum(
    cur: Any, table_ref: str, columns: list[str]
) -> EngineChecksum | None:
    """Run the digest, or return ``None`` so the caller keeps the Python path."""
    if not columns:
        return None
    try:
        cur.execute(postgresql_checksum_sql(table_ref, columns))
        row = cur.fetchone()
    except Exception as exc:
        logger.info("Engine checksum unavailable for %s: %s", table_ref, exc)
        return None
    if not row:
        return None
    return EngineChecksum(
        row_count=int(row[0] or 0),
        # Rendered as text so the value survives transport and comparison the
        # same way the Python digest does.
        checksum=str(row[1]),
        scope="engine_population",
    )
