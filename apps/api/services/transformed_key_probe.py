"""Uniqueness of an upsert key *after* its mapping transform (D39).

The source uniqueness probe groups the key as the source stores it. A
transform on the key column (``lower``, ``trim``, ...) can make distinct
source keys identical on the destination: ``A1`` and ``a1`` are two rows to
the source and one key to the merge. Nothing noticed until the write, where
the load died on a unique violation or - worse for an upsert - silently kept
one of the rows and failed the row-count check afterwards.

This walks the same projected population Validate already reads and applies
the key columns' own transforms, counting groups whose *source* keys differ
but whose *written* key is the same. Plain duplicates (identical source keys)
are the source probe's finding and are not repeated here.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

#: Distinct keys held at once. A bigger population is reported as partial
#: coverage rather than silently claimed.
_MAX_TRACKED_KEYS = 2_000_000

_IDENTITY_TRANSFORMS = frozenset({"", "none", "identity", "passthrough"})


def key_transforms(
    mappings: Iterable[Mapping[str, Any]] | None, key_sources: Iterable[str]
) -> dict[str, str]:
    """``{source key column: transform}`` for key columns that rewrite their value."""
    from services.mapping_constraints import write_mappings

    wanted = {str(c).casefold() for c in key_sources}
    out: dict[str, str] = {}
    for mapping in write_mappings([dict(m) for m in (mappings or []) if isinstance(m, Mapping)]):
        source = str(mapping.get("source") or "").strip()
        if source.casefold() not in wanted:
            continue
        transform = str(mapping.get("transform") or "").strip()
        if transform.casefold() not in _IDENTITY_TRANSFORMS:
            out[source] = transform
    return out


def probe_transformed_key_collisions(
    rows: Iterable[Mapping[str, Any]] | None,
    *,
    key_sources: list[str],
    transforms: Mapping[str, str],
    limit: int = 5,
    equalizers: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Count written keys that two different source keys collapse into.

    ``equalizers`` (D205) maps a key column to ``(normalize, ddl, rules)`` from
    :func:`services.dest_key_equality.key_equalizers`: the destination's own
    comparison (PAD SPACE, case / accent folding) applied to the written key.
    """
    from services.transform_engine import apply_transform

    equalizers = dict(equalizers or {})

    seen: dict[tuple[str, ...], tuple[str, ...]] = {}
    collisions: list[dict[str, Any]] = []
    collision_count = 0
    scanned = 0
    complete = True
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        scanned += 1
        raw: list[str] = []
        written: list[str] = []
        usable = True
        for col in key_sources:
            cell = row.get(col)
            if cell is None:
                usable = False  # a NULL key is the null-key check's finding
                break
            text = str(cell)
            transform = transforms.get(col, "")
            if transform:
                value, error = apply_transform(text, transform)
                if error or value is None:
                    usable = False  # a transform error is G5's finding
                    break
                written.append(str(value))
            else:
                written.append(text)
            if col in equalizers:
                written[-1] = equalizers[col][0](written[-1])
            raw.append(text)
        if not usable:
            continue
        key = tuple(written)
        first = seen.get(key)
        if first is None:
            if len(seen) >= _MAX_TRACKED_KEYS:
                complete = False
                continue
            seen[key] = tuple(raw)
        elif first != tuple(raw):
            collision_count += 1
            if len(collisions) < limit:
                collisions.append({
                    "written_key": ", ".join(key),
                    "source_keys": [", ".join(first), ", ".join(raw)],
                    "source_key_parts": [list(first), list(raw)],
                })
    return {
        "ran": True,
        "complete": complete,
        "rows_scanned": scanned,
        "collision_count": collision_count,
        "collisions": collisions,
        "key_columns": list(key_sources),
        "transforms": dict(transforms),
        "destination_equality": {
            col: {"ddl": eq[1], "rules": list(eq[2])} for col, eq in equalizers.items()
        },
    }
