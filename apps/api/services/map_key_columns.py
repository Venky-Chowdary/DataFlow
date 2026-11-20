"""The destination key columns Map protects, decided on the server (rule 4, D369).

Map's key guard (``semantic_mapper.mapping_violates_key_rule``) refuses to
propose an unrelated source column onto a destination primary key or identity
column. The Studio used to send those columns itself (``target_key_columns``)
and the route used them as the only key source. A stale list (the previous
table's key after the operator typed another table) or a crafted one then
protected the wrong column and left the real key open.

When the request names the destination, the keys come from the destination's
own catalog read here, under the caller's connector authorization, and the
client's list is ignored. Without a destination (older clients, file
targets), or when the catalog cannot be read, the client's names are kept only
where they are columns of the target the request maps onto; a name that is
not a column of that table is dropped. Validate and Execute read the catalog
keys themselves either way, so Map can never widen what they enforce.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

#: Bound on the Map-time catalog read; the Studio has just probed this table.
MAP_KEY_PROBE_TIMEOUT_S = 20.0

SOURCE_CATALOG = "catalog"
SOURCE_CLIENT_CHECKED = "client_checked"
SOURCE_NONE = "none"


def _names(raw: Iterable[Any] | None) -> list[str]:
    return [str(n).strip() for n in (raw or []) if str(n or "").strip()]


def catalog_key_columns(catalog: Mapping[str, Any]) -> list[str]:
    """Primary key, identity and key attributes the destination reports, in that order."""
    from services.key_column_guard import destination_key_names

    lowered = destination_key_names(catalog)
    spelled = {c.lower(): c for c in _names(catalog.get("columns"))}
    out: list[str] = []
    seen: set[str] = set()
    for name in (
        *_names(catalog.get("primary_key_columns")),
        *_names(catalog.get("identity_columns")),
        *sorted(lowered),
    ):
        if name.lower() in lowered and name.lower() not in seen:
            seen.add(name.lower())
            out.append(spelled.get(name.lower(), name))
    return out


def resolve_map_key_columns(
    client_keys: Iterable[Any] | None,
    *,
    target_columns: Iterable[Any] | None,
    catalog: Mapping[str, Any] | None,
) -> tuple[list[str], str]:
    """``(key_columns, source)`` for Map's key guard.

    ``catalog`` is the destination introspection for the mapped table, or
    ``None`` when the request named no destination or it could not be read.
    """
    if isinstance(catalog, Mapping) and (catalog.get("columns") or catalog.get("table_exists") is False):
        # The table was read (or is confirmed missing: create-new has no key).
        return catalog_key_columns(catalog), SOURCE_CATALOG
    targets = {c.lower(): c for c in _names(target_columns)}
    kept: list[str] = []
    for name in _names(client_keys):
        spelled = targets.get(name.lower())
        if spelled and spelled not in kept:
            kept.append(spelled)
    return kept, (SOURCE_CLIENT_CHECKED if kept else SOURCE_NONE)
