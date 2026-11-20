"""Which destination columns are keys, for the mapper's rule-4 guard (D369).

The mapper refuses to propose an unrelated source column onto a destination
primary key, identity column, DynamoDB key attribute or document ``_id``
(``semantic_mapper.mapping_violates_key_rule``). That guard reads per-column
flags. Introspection reports most keys at table level
(``primary_key_columns``, ``identity_columns``, ``dynamo_key_schema``), and
every caller rebuilt its target rows as ``{name, inferred_type, samples}``, so
the flags never reached the mapper and only ``_id`` was protected. Each caller
now passes its introspection result (or the probe stamps on
``destination.extra``) through :func:`with_key_flags`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

_COLUMN_FLAGS = ("primary_key", "is_primary_key", "identity", "is_identity")


def _names(raw: Any) -> list[str]:
    if isinstance(raw, str):
        return [p.strip() for p in raw.split(",") if p.strip()]
    if not isinstance(raw, Iterable):
        return []
    out: list[str] = []
    for item in raw:
        if isinstance(item, Mapping):
            item = item.get("name") or item.get("column") or item.get("AttributeName")
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def destination_key_names(meta: Mapping[str, Any] | None) -> set[str]:
    """Key, identity and partition/sort key column names, lower-cased."""
    if not isinstance(meta, Mapping):
        return set()
    # A Pilot introspection keeps the catalog result under ``raw``.
    names: list[str] = sorted(destination_key_names(meta.get("raw"))) if meta.get("raw") is not meta else []
    for field in ("primary_key_columns", "identity_columns", "key_columns"):
        names.extend(_names(meta.get(field)))
    names.extend(_names(meta.get("dynamo_key_schema")))
    for col in meta.get("columns") or []:
        if isinstance(col, Mapping) and any(col.get(flag) for flag in _COLUMN_FLAGS):
            names.extend(_names([col]))
    return {n.lower() for n in names}


def with_key_flags(
    rows: list[dict[str, Any]] | None,
    meta: Mapping[str, Any] | None,
) -> list[dict[str, Any]] | None:
    """Copy ``rows`` with ``primary_key: True`` on every key column of ``meta``."""
    if not rows:
        return rows
    keys = destination_key_names(meta)
    if not keys:
        return rows
    return [
        {**row, "primary_key": True}
        if str(row.get("name") or "").strip().lower() in keys
        else row
        for row in rows
    ]
