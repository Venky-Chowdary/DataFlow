"""Searchable connector catalog — 600+ data product integrations."""

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
    data = _load()
    items: list[dict] = data.get("connectors", [])
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

    live_count = sum(1 for c in data.get("connectors", []) if c.get("status") == "live")
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "categories": categories,
        "live_count": live_count,
        "catalog_total": data.get("total", len(data.get("connectors", []))),
        "connectors": page,
    }


def get_connector_meta(connector_id: str) -> dict | None:
    for c in _load().get("connectors", []):
        if c.get("id") == connector_id:
            return c
    return None
