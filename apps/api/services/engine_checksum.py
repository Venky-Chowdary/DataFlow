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


def engines_comparable(source_engine: str | None, dest_engine: str | None) -> bool:
    """True when both ends are the same engine this module knows how to render."""
    src = normalize_engine(source_engine)
    return src == normalize_engine(dest_engine) and engine_supports_checksum(src)


#: Transforms that coerce a value *into* a type rather than changing it. Paired
#: with the identical-type precondition below they are no-ops: the mapper stamps
#: ``decimal`` on a column that is already ``numeric(12,2)``, and coercing a
#: Decimal to a Decimal cannot move it. The mapper assigns these by default, so
#: refusing them would have left this path unreachable on ordinary routes.
#:
#: Deliberately absent: ``currency`` and ``percentage`` parse ``$1,234.56`` and
#: do change the value; ``trim``/``upper``/``lower``/``hash_pii``/``mask_pii``
#: and the other normalizers exist precisely to change it; ``json`` may be
#: re-serialized with different whitespace or key order, which is the same
#: number and different text, and text is what this digest compares.
_NO_OP_TYPE_TRANSFORMS: Final[frozenset[str]] = frozenset({
    "",
    "none",
    "identity",
    "passthrough",
    "string",
    "varchar",
    "text",
    "decimal",
    "integer",
    "boolean",
    "date",
    "datetime",
    "time",
    "uuid",
})


def _canonical_type(engine: str, declared: str) -> str:
    """Canonical spelling, so two resolvers naming one column agree.

    The source schema comes from introspection and the destination types from
    the writer or the live catalog, so the same physical column arrives as
    ``DECIMAL(12,2)`` from one and ``NUMERIC(12,2)`` from the other. Comparing
    the raw spellings declined every real route.
    """
    from services.reconciliation import _canonical_fingerprint_ddl

    return _canonical_fingerprint_ddl(engine, str(declared or "").strip())


def comparable_column_pairs(
    mappings: list[dict] | None,
    source_schema: dict[str, str] | None,
    dest_types: dict[str, str] | None,
    *,
    engine: str = "postgresql",
) -> list[tuple[str, str]] | None:
    """Ordered ``(source_column, target_column)`` when a digest may compare them.

    The digest hashes values positionally and never sees a column name, so a
    rename is free: the two sides simply project their own names in the same
    order. What it cannot absorb is a value that is *meant* to differ, so every
    mapping must be a pure carry — no transform, no omission — onto a type
    declared identically on both sides.

    Returns ``None`` rather than a partial list, because a digest over a subset
    of columns would prove a subset of the migration while reading as a
    full-population pass.
    """
    if not mappings:
        return None
    from services.mapping_constraints import is_intentional_omit

    eng = normalize_engine(engine)
    src_types = {
        str(k).lower(): str(v or "").strip() for k, v in (source_schema or {}).items()
    }
    dst_types = {
        str(k).lower(): str(v or "").strip() for k, v in (dest_types or {}).items()
    }
    pairs: list[tuple[str, str]] = []
    for mapping in mappings:
        if is_intentional_omit(mapping):
            # A declared omission means the destination is knowingly missing a
            # source column, so the two populations are not the same shape.
            return None
        source = str(mapping.get("source") or "").strip()
        target = str(mapping.get("target") or source).strip()
        if not source or not target:
            return None
        transform = str(mapping.get("transform") or "").strip().lower()
        if transform not in _NO_OP_TYPE_TRANSFORMS:
            return None
        left = src_types.get(source.lower())
        right = dst_types.get(target.lower())
        if not left or not right:
            return None
        if _canonical_type(eng, left) != _canonical_type(eng, right):
            return None
        pairs.append((source, target))
    return pairs or None


def postgresql_checksum_sql(
    table_ref: str, columns: list[str], *, where: str = ""
) -> str:
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
    where_sql = f" WHERE {where}" if (where or "").strip() else ""
    return (
        "SELECT count(*)::bigint AS n, "
        f"coalesce(sum(('x' || substr(md5({row_text}), 1, 15))::bit(60)::bigint::numeric), 0) AS digest "
        f"FROM {table_ref}{where_sql}"  # nosec B608 — identifiers quoted above
    )


def postgresql_engine_checksum(
    cur: Any, table_ref: str, columns: list[str], *, where: str = ""
) -> EngineChecksum | None:
    """Run the digest, or return ``None`` so the caller keeps the Python path."""
    if not columns:
        return None
    try:
        cur.execute(postgresql_checksum_sql(table_ref, columns, where=where))
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
