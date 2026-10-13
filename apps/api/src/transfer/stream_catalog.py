"""Catalog facts the stream writer reads once: destination keys and CREATE payload.

Kept beside ``stream.py`` so the page loop does not also own introspection.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def declared_destination_key_columns(
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    mappings: list[dict],
) -> tuple[list[str], list[str]]:
    """Identity key the write can use, from the destination catalog.

    Returns ``(source_columns, target_columns)``, empty when the destination
    declares no primary key or the key is not covered by the mapping — an
    unmapped key column cannot be an ON CONFLICT target, and guessing one would
    resolve rows on the wrong identity.
    """
    from services.sync_cursor import map_source_to_target

    from .adapters import _introspect_table_schema_rich

    if not dest_type or not dest_table:
        return [], []
    try:
        _types, _nulls, keys = _introspect_table_schema_rich(
            dest_type, dest_cfg, dest_table, [], strict_namespace=True
        )
    except Exception as exc:
        logger.debug("resume destination key introspection failed: %s", exc, exc_info=exc)
        return [], []
    pk_targets = [str(c) for c in (keys.get("primary_key_columns") or []) if str(c or "")]
    if not pk_targets:
        return [], []
    wanted = {c.lower() for c in pk_targets}
    src_cols: list[str] = []
    for item in mappings or []:
        src = str(item.get("source") or "")
        if not src:
            continue
        tgt = str(map_source_to_target(src, mappings) or "")
        if tgt.lower() in wanted:
            src_cols.append(src)
    if len(src_cols) != len(pk_targets):
        return [], []
    return src_cols, pk_targets


def fast_path_source_catalog(
    src_type: str,
    mappings: list[dict],
    schema: dict[str, str],
    rich: tuple[dict[str, str], dict[str, bool], dict[str, Any]],
) -> dict[str, Any] | None:
    """Catalog payload for a fast-path CREATE, or ``None`` when nothing was read."""
    types, nulls, keys = rich
    if not (types or nulls or keys):
        return None
    from services.schema_fidelity import build_catalog_from_introspect, catalog_to_payload

    try:
        return catalog_to_payload(
            build_catalog_from_introspect(
                dialect=src_type,
                columns=[str(item.get("source") or "") for item in mappings if item.get("source")],
                column_types=types or dict(schema or {}),
                nullable=nulls,
                keys=keys,
            )
        )
    except Exception as exc:
        logger.debug("source schema catalog build failed: %s", exc, exc_info=exc)
        return None
