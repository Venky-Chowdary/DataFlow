"""Searchable connector catalog — certified drivers plus roadmap integrations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

CATALOG_PATH = Path(__file__).resolve().parents[1] / "data" / "connector_catalog.json"

_cache: dict[str, Any] | None = None


def _load() -> dict[str, Any]:
    global _cache
    if _cache is not None:
        return _cache
    if not CATALOG_PATH.exists():
        _cache = {"version": 1, "total": 0, "connectors": []}
        return _cache
    _cache = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    return _cache


def list_catalog(
    *,
    q: str = "",
    category: str | None = None,
    status: str | None = None,
    offset: int = 0,
    limit: int = 48,
) -> dict[str, Any]:
    """Catalog page whose statuses are the enriched, capability-derived ones.

    This endpoint served the raw catalog file, so a roadmap tile carrying
    ``status: live`` was published to a client as live and ``live_count``
    counted those tiles — an overclaim of hundreds of connectors against a
    handful of transfer-live drivers. Enrichment and the driver count both come
    from the canonical catalog service.
    """
    from services.catalog_service import catalog_summary, enriched_connectors

    data = _load()
    items: list[dict] = enriched_connectors()
    query = q.strip().lower()

    if query:
        items = [
            c
            for c in items
            if query in c.get("name", "").lower()
            or query in c.get("description", "").lower()
            or query in c.get("id", "").lower()
        ]
    if category:
        items = [c for c in items if c.get("category") == category]
    if status:
        items = [c for c in items if c.get("status") == status]

    total = len(items)
    page = items[offset : offset + limit]
    categories = sorted({c.get("category", "other") for c in data.get("connectors", [])})

    summary = catalog_summary()
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "categories": categories,
        # Unique transfer-live drivers, never the number of tiles.
        "live_count": int(summary.get("unique_drivers") or 0),
        "live_tiles": int(summary.get("transfer_live_tiles") or 0),
        "catalog_total": len(enriched_connectors()),
        "connectors": page,
    }


def get_connector_meta(connector_id: str) -> dict | None:
    from services.catalog_service import get_connector_by_id

    return get_connector_by_id(connector_id)
