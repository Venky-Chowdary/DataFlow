"""Connector catalog — searchable marketplace with honest capability enrichment."""

from __future__ import annotations
import json
import os
from functools import lru_cache

try:
    from ..transfer.connector_capabilities import (
        SUGGESTED_DESTINATIONS,
        SUGGESTED_SOURCES,
        enrich_catalog_entry,
    )
except ImportError:  # pragma: no cover - compatibility for direct module loading in tests
    from transfer.connector_capabilities import (
        SUGGESTED_DESTINATIONS,
        SUGGESTED_SOURCES,
        enrich_catalog_entry,
    )

_CATALOG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "connector_catalog.json"
)


@lru_cache(maxsize=1)
def load_catalog() -> dict:
    with open(_CATALOG_PATH, encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def _enriched_connectors() -> list[dict]:
    enriched = []
    seen: set[str] = set()
    for c in load_catalog().get("connectors", []):
        cid = c.get("id", "")
        if cid in seen:
            continue
        seen.add(cid)
        try:
            row = enrich_catalog_entry(c)
            row["status"] = row.get("effective_status", row.get("status"))
        except Exception:
            # If a single connector entry fails capability discovery, keep the
            # original catalog row and mark it as planned so the catalog still
            # loads and the rest of the connectors are usable.
            row = dict(c)
            row["status"] = row.get("status", "planned")
            row["effective_status"] = "planned"
            row["driver_type"] = "unknown"
            row["capabilities"] = {
                "test": False, "read": False, "write": False,
                "introspect": False, "preflight": False,
            }
            row["transfer_ready"] = False
            row["connect_only"] = False
            row["capability_label"] = "Roadmap"
        enriched.append(row)
    return enriched


def search_catalog(
    query: str = "",
    role: str = "all",
    category: str = "",
    status: str = "",
    limit: int = 60,
    transfer_only: bool = False,
) -> dict:
    data = load_catalog()
    connectors = _enriched_connectors()

    if transfer_only:
        connectors = [c for c in connectors if c.get("transfer_ready")]

    if role == "source":
        suggested_ids = set(SUGGESTED_SOURCES)
        connectors = sorted(
            connectors,
            key=lambda c: (c["id"] not in suggested_ids, not c.get("transfer_ready"), c["name"]),
        )
    elif role == "destination":
        suggested_ids = set(SUGGESTED_DESTINATIONS)
        connectors = sorted(
            connectors,
            key=lambda c: (c["id"] not in suggested_ids, not c.get("transfer_ready"), c["name"]),
        )

    q = query.lower().strip()
    if q:
        connectors = [
            c for c in connectors
            if q in c["name"].lower() or q in c["id"].lower()
            or q in c.get("category", "").lower()
            or q in c.get("description", "").lower()
            or q in c.get("driver_type", "").lower()
        ]

    if category:
        connectors = [c for c in connectors if c.get("category") == category]

    if status:
        if status == "live":
            connectors = [c for c in connectors if c.get("effective_status") == "live"]
        elif status == "connect_only":
            connectors = [c for c in connectors if c.get("effective_status") == "connect_only"]
        elif status == "beta":
            connectors = [c for c in connectors if c.get("status") == "beta" or c.get("effective_status") == "connect_only"]
        else:
            connectors = [c for c in connectors if c.get("status") == status or c.get("effective_status") == status]

    total = len(connectors)
    page = connectors[:limit]

    suggested = []
    ids = SUGGESTED_SOURCES if role == "source" else SUGGESTED_DESTINATIONS if role == "destination" else []
    if ids and not q:
        by_id = {c["id"]: c for c in _enriched_connectors()}
        suggested = [by_id[i] for i in ids if i in by_id][:16]
        if transfer_only:
            suggested = [s for s in suggested if s.get("transfer_ready")]

    categories = sorted({c.get("category", "other") for c in data.get("connectors", [])})
    enriched_all = _enriched_connectors()

    return {
        "total": total,
        "filtered": total,
        "connectors": page,
        "suggested": suggested if not q else page[:16],
        "categories": categories,
        "transfer_live": sum(1 for c in enriched_all if c.get("transfer_ready")),
        "connect_only": sum(1 for c in enriched_all if c.get("connect_only")),
        "roadmap": sum(1 for c in enriched_all if c.get("effective_status") == "planned"),
    }


def catalog_summary() -> dict:
    data = load_catalog()
    enriched = _enriched_connectors()
    by_status: dict[str, int] = {}
    for c in data.get("connectors", []):
        st = c.get("status", "planned")
        by_status[st] = by_status.get(st, 0) + 1

    transfer_live = sum(1 for c in enriched if c.get("transfer_ready"))
    connect_only = sum(1 for c in enriched if c.get("connect_only"))

    return {
        "total": data.get("total", len(data.get("connectors", []))),
        "live": transfer_live,
        "beta": by_status.get("beta", 0),
        "planned": by_status.get("planned", 0),
        "categories": len({c.get("category", "other") for c in data.get("connectors", [])}),
        "transfer_live": transfer_live,
        "connect_only": connect_only,
        "roadmap": len(enriched) - transfer_live - connect_only,
    }


def get_connector_by_id(connector_id: str) -> dict | None:
    for c in load_catalog().get("connectors", []):
        if c["id"] == connector_id:
            return enrich_catalog_entry(c)
    return None


def catalog_training_docs(limit: int | None = None) -> list[dict]:
    """RAG documents — accurate readiness per connector."""
    connectors = _enriched_connectors()
    if limit is not None:
        connectors = connectors[:limit]

    docs = []
    for c in connectors:
        label = c.get("capability_label", "Roadmap")
        readiness = (
            f"{label}. Supports production transfer via Transfer Studio."
            if c.get("transfer_ready")
            else (
                "Connection test only — save credentials but transfer routes are not implemented yet."
                if c.get("connect_only")
                else "Catalog roadmap entry — catalog discovery only, no driver registered yet. route live transfers only when the connector is marked transfer-ready."
            )
        )
        docs.append({
            "id": f"catalog_{c['id']}",
            "text": (
                f"Connector: {c['name']} ({c['id']})\n"
                f"Driver type: {c.get('driver_type', 'unknown')}\n"
                f"Category: {c.get('category', 'unknown')}\n"
                f"Catalog status: {c.get('status', 'planned')}\n"
                f"Effective status: {c.get('effective_status', 'planned')}\n"
                f"Description: {c.get('description', '')}\n"
                f"Readiness: {readiness}\n"
                f"Capabilities: test={c.get('capabilities', {}).get('test')}, "
                f"read={c.get('capabilities', {}).get('read')}, "
                f"write={c.get('capabilities', {}).get('write')}"
            ),
            "metadata": {
                "type": "copilot_knowledge",
                "connector_id": c["id"],
                "driver_type": c.get("driver_type"),
                "category": c.get("category", ""),
                "status": c.get("effective_status"),
                "transfer_ready": c.get("transfer_ready"),
            },
        })
    return docs
