"""Live source schema authority for Validate.

Validate used to score mappings against the ``column_types`` the browser
posted, while Execute re-derived the source schema from the live connector and
re-decided. When the two disagreed the operator got 13/13 green at Validate and
a hard fidelity failure at write — the Mongo ``_id`` case (``VARCHAR`` in the
Map payload, ``OBJECTID`` from live BSON introspection) is one instance, but the
divergence is connector-agnostic: any source whose declared types are refined
server-side (BSON, Parquet logical types, SaaS describes, PG domains) can drift.

This module gives Validate the same source facts Execute will use. Live
introspection wins over the posted declaration; whatever it cannot answer keeps
the declared type. Columns that changed are returned as drift so the response
can show *why* a gate now blocks instead of silently rescoring.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("datawrap.preflight")


def endpoint_source_column_types(endpoint: Any) -> dict[str, str]:
    """Declared column types for an already-built source endpoint.

    The reader reports the shape it decoded, which for a SQL source is the
    Python value space (``uid`` arrives as ``str`` and is reported ``VARCHAR``)
    rather than the column's declaration (``CHAR(36)``). Mapping off the decoded
    shape while the coercion validator later sees the declaration is the same
    Validate/Execute split as the Mongo ``_id`` case, so both must read this.
    """
    if endpoint is None or getattr(endpoint, "kind", "") != "database":
        return {}
    try:
        from src.transfer.endpoint_intelligence import introspect_endpoint

        endpoint.extra = {**(endpoint.extra or {}), "introspect_purpose": "source"}
        info = introspect_endpoint(endpoint)
    except Exception:
        logger.debug("live source introspect failed for endpoint", exc_info=True)
        return {}
    schema = info.get("schema") or {}
    if not isinstance(schema, dict):
        return {}
    return {str(k): str(v) for k, v in schema.items() if str(v or "").strip()}


def live_source_column_types(
    *,
    source_connector_id: str = "",
    source_table: str = "",
    source_collection: str = "",
    source_schema: str = "",
    source_database: str = "",
) -> dict[str, str]:
    """Introspect the saved source connector for its declared column types.

    Returns ``{}`` when there is no saved connector or introspection fails —
    Validate then keeps the posted declaration rather than inventing types.
    """
    connector_id = (source_connector_id or "").strip()
    stream = (source_table or source_collection or "").strip()
    if not connector_id or not stream:
        return {}
    try:
        from services.connector_probe import endpoint_from_saved_connector

        endpoint = endpoint_from_saved_connector(
            connector_id,
            table=source_table or stream,
            collection=source_collection or stream,
            schema=source_schema or "",
            database=source_database or "",
        )
        if endpoint is None:
            return {}
    except Exception:
        logger.debug("live source introspect failed for %s", connector_id, exc_info=True)
        return {}
    return endpoint_source_column_types(endpoint)


def reconcile_source_types(
    declared: dict[str, str] | None,
    live: dict[str, str] | None,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    """Return ``(types, drift)`` where live introspection wins over ``declared``.

    ``drift`` lists only columns whose *logical* type changed — a declared
    ``VARCHAR(64)`` refined to ``VARCHAR(24)`` is the same decision and is not
    worth an operator note, but ``VARCHAR`` → ``OBJECTID`` changes the create-new
    invent and the fidelity verdict, so it must be visible.
    """
    from services.type_system import normalize_logical_type

    merged = dict(declared or {})
    drift: list[dict[str, str]] = []
    for col, live_type in (live or {}).items():
        prior = str(merged.get(col) or "").strip()
        merged[col] = live_type
        if not prior:
            continue
        try:
            changed = normalize_logical_type(prior) != normalize_logical_type(live_type)
        except (ValueError, TypeError, KeyError, AttributeError):
            changed = prior.upper() != str(live_type).upper()
        if changed:
            drift.append({"column": col, "declared": prior, "live": str(live_type)})
    return merged, drift


def restamp_mapping_source_types(
    mappings: list[dict[str, Any]] | None,
    source_types: dict[str, str] | None,
) -> list[dict[str, Any]]:
    """Re-stamp ``source_type`` on Map rows from the authoritative schema.

    The Decision Artifact hashes each mapping's ``source_type``; leaving the
    browser's stale stamp there would sign an artifact Execute cannot reproduce.
    """
    types = source_types or {}
    out: list[dict[str, Any]] = []
    for m in mappings or []:
        row = dict(m)
        live = str(types.get(str(row.get("source") or "")) or "").strip()
        if live:
            row["source_type"] = live
        out.append(row)
    return out
