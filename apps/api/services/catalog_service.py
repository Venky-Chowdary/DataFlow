"""Connector catalog — searchable marketplace with honest capability enrichment."""

from __future__ import annotations

import json
import os
from functools import lru_cache

try:
    from src.transfer.connector_capabilities import (
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
    os.path.dirname(__file__), "..", "data", "connector_catalog.json"
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
            row["certification_tier"] = "planned"
        enriched.append(row)
    return enriched


def _with_host_destination_policy(row: dict) -> dict:
    """D276: a destination this host refuses keeps its tile, flagged with why.

    SQLite on the hosted cloud is still a source; as a destination it reads
    ``dest_ready: false`` plus ``dest_unavailable_reason`` ("Not available on
    cloud") so a picker can show it disabled instead of offering it.
    """
    from services.local_file_policy import (
        SQLITE_DESTINATION_UNAVAILABLE_LABEL,
        sqlite_destination_refusal,
    )

    refusal = row.get("dest_ready") and sqlite_destination_refusal(
        str(row.get("driver_type") or row.get("id") or "")
    )
    if not refusal:
        return row
    return {
        **row,
        "dest_ready": False,
        "dest_unavailable": True,
        "dest_unavailable_reason": SQLITE_DESTINATION_UNAVAILABLE_LABEL,
        "dest_unavailable_message": refusal,
    }


def _host_rows() -> list[dict]:
    """The cached enrichment as this host offers it (env reads only; cache untouched)."""
    from services.local_file_policy import sqlite_destination_allowed

    rows = _enriched_connectors()
    if sqlite_destination_allowed():
        return rows
    return [_with_host_destination_policy(row) for row in rows]


def enriched_connectors() -> list[dict]:
    """Every catalog row with canonical capability enrichment applied.

    One enrichment owner: any surface that lists connectors reads this, so a
    raw ``status: live`` tile cannot reach a client through a second path.
    """
    return [dict(row) for row in _host_rows()]


def search_catalog(
    query: str = "",
    role: str = "all",
    category: str = "",
    status: str = "",
    limit: int = 60,
    transfer_only: bool = False,
    offset: int = 0,
) -> dict:
    data = load_catalog()
    connectors = _host_rows()

    # ``transfer_only`` means duplex — read *and* write. That is the right
    # question only when no side is chosen. Applying it to a role-scoped picker
    # dropped exactly the connectors that can serve that one side: a source-only
    # feed could never be picked as a source, which is the only thing it can be.
    # For a role, the role-readiness filter below *is* the transfer-ready test.
    if transfer_only and role not in {"source", "destination"}:
        connectors = [c for c in connectors if c.get("transfer_ready")]

    # ``role`` used only to re-sort, so both pickers were handed the same list:
    # the source picker offered Pinecone, Weaviate, Milvus, Qdrant and pgvector,
    # none of which can be read, and picking one produced a route that failed at
    # Execute. A picker must only offer the side a connector can actually take.
    if role == "source":
        suggested_ids = set(SUGGESTED_SOURCES)
        connectors = [c for c in connectors if c.get("source_ready")]
        connectors = sorted(
            connectors,
            key=lambda c: (c["id"] not in suggested_ids, not c.get("transfer_ready"), c["name"]),
        )
    elif role == "destination":
        suggested_ids = set(SUGGESTED_DESTINATIONS)
        connectors = [c for c in connectors if c.get("dest_ready")]
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
            or q in c.get("driver_type", "").lower()
        ]
        # Description text is not an identity. SFTP, Excel and CrateDB mention
        # Postgres in their blurbs and were returned for query=postgres
        # (QA RT5-13). A product name matches the name, id, or driver only.

    if category:
        connectors = [c for c in connectors if c.get("category") == category]

    if status:
        if status == "live":
            # "Transfer ready" means duplex — full read *and* write. Asked
            # without a side that is the right question. Asked *with* one it is
            # not: a source-only connector is transfer-ready as a source, and
            # filtering it out left an operator who ticked "Transfer ready" on
            # the source list unable to find Couchbase or Neo4j at all.
            if role == "source":
                connectors = [c for c in connectors if c.get("source_ready")]
            elif role == "destination":
                connectors = [c for c in connectors if c.get("dest_ready")]
            else:
                connectors = [c for c in connectors if c.get("transfer_ready")]
        elif status == "source_only":
            connectors = [c for c in connectors if c.get("certification_tier") == "source_only"]
        elif status == "connect_only":
            connectors = [
                c for c in connectors
                if c.get("effective_status") == "connect_only"
                or c.get("certification_tier") == "connect_only"
            ]
        elif status == "beta":
            connectors = [c for c in connectors if c.get("status") == "beta" or c.get("effective_status") == "connect_only"]
        else:
            connectors = [c for c in connectors if c.get("status") == status or c.get("effective_status") == status]

    total = len(connectors)
    # ``offset`` pages the filtered list. It used to be ignored, so with 700+
    # tiles and ``limit`` capped at 200 everything past the first page was
    # unreachable (QA round 11). ``next_offset`` is None on the last page.
    start = max(0, int(offset or 0))
    size = max(0, int(limit))
    page = connectors[start:start + size]
    next_offset = start + size if start + size < total else None

    suggested = []
    ids = SUGGESTED_SOURCES if role == "source" else SUGGESTED_DESTINATIONS if role == "destination" else []
    if ids and not q:
        by_id = {c["id"]: c for c in _host_rows()}
        suggested = [by_id[i] for i in ids if i in by_id][:16]
        # A suggestion is still an offer: the same role rule applies to it.
        role_key = "source_ready" if role == "source" else "dest_ready"
        suggested = [s for s in suggested if s.get(role_key)]

    categories = sorted({c.get("category", "other") for c in data.get("connectors", [])})
    enriched_all = _host_rows()
    certified_tiles = sum(
        1 for c in enriched_all if c.get("certification_tier") == "certified" or c.get("transfer_ready")
    )
    source_only = sum(1 for c in enriched_all if c.get("certification_tier") == "source_only")
    planned = sum(1 for c in enriched_all if c.get("certification_tier") == "planned" or c.get("effective_status") == "planned")
    try:
        from src.transfer.connector_capabilities import transfer_live_driver_types

        unique_drivers = len(transfer_live_driver_types())
    except Exception:
        unique_drivers = sum(1 for c in enriched_all if c.get("transfer_ready"))

    # How many engines the operator can actually pick on each side. A single
    # "43 drivers" answered both questions and neither: it counts a connector
    # usable in *a* transfer, which includes write-only stores when asked about
    # sources and read-only feeds when asked about destinations.
    sides = _side_driver_counts(enriched_all)
    source_drivers = sides["source_live"]
    dest_drivers = sides["dest_live"]

    return {
        "total": total,
        "filtered": total,
        "offset": start,
        "limit": size,
        "next_offset": next_offset,
        "connectors": page,
        "suggested": suggested if not q else page[:16],
        "categories": categories,
        # Primary honesty metrics — unique engines, not alias tiles.
        "transfer_live": unique_drivers,
        "transfer_live_tiles": sum(1 for c in enriched_all if c.get("transfer_ready")),
        "connect_only": sum(1 for c in enriched_all if c.get("connect_only")),
        "roadmap": sum(1 for c in enriched_all if c.get("effective_status") == "planned"),
        "certified": unique_drivers,
        "certified_tiles": certified_tiles,
        "source_only": source_only,
        "planned_count": planned,
        # Per-side counts, derived the same way the list above is filtered, so
        # the number always describes the list the operator is looking at.
        **sides,
        **_capability_driver_counts(),
        "role_live": (
            source_drivers if role == "source"
            else dest_drivers if role == "destination"
            else unique_drivers
        ),
    }


def _side_driver_counts(enriched: list[dict]) -> dict[str, int]:
    """Distinct engines pickable on each side, split live vs fixture-proven beta.

    Betas are offered so operators can try them, but their only evidence is a
    synthetic fixture — the rule transfer_live_driver_types uses to exclude
    them — so counting them as live would push a side past the transfer union.
    """
    try:
        from src.transfer.connector_capabilities import get_capabilities
    except ImportError:
        from transfer.connector_capabilities import get_capabilities
    source = {c.get("driver_type") for c in enriched if c.get("source_ready") and c.get("driver_type")}
    dest = {c.get("driver_type") for c in enriched if c.get("dest_ready") and c.get("driver_type")}
    beta = {t for t in source | dest if get_capabilities(t).get("evidence") == "synthetic-fixture"}
    return {
        "source_live": len(source - beta),
        "dest_live": len(dest - beta),
        "source_beta": len(source & beta),
        "dest_beta": len(dest & beta),
    }


def _capability_driver_counts() -> dict[str, object]:
    """Driver counts from the same functions get_transfer_capabilities uses.

    ``source_live`` / ``dest_live`` stay the catalog-tile counts. These fields
    are the transfer-ready driver sets, which are a different population.
    """
    try:
        from src.transfer.connector_capabilities import (
            dest_live_driver_types,
            source_live_driver_types,
            transfer_live_driver_types,
        )
    except ImportError:
        from transfer.connector_capabilities import (
            dest_live_driver_types,
            source_live_driver_types,
            transfer_live_driver_types,
        )
    return {
        "capabilities_source_live": len(source_live_driver_types()),
        "capabilities_dest_live": len(dest_live_driver_types()),
        "capabilities_transfer_live": len(transfer_live_driver_types()),
        "count_note": (
            "source_live and dest_live count live catalog engines you can pick; "
            "source_beta and dest_beta count fixture-proven beta engines. "
            "capabilities_source_live, capabilities_dest_live, and "
            "capabilities_transfer_live are the transfer-ready driver sets "
            "get_transfer_capabilities reports. Catalog tiles are not transfer-live drivers."
        ),
    }


def catalog_summary() -> dict:
    data = load_catalog()
    enriched = _host_rows()
    by_status: dict[str, int] = {}
    for c in data.get("connectors", []):
        st = c.get("status", "planned")
        by_status[st] = by_status.get(st, 0) + 1

    catalog_tiles = sum(1 for c in enriched if c.get("transfer_ready"))
    connect_only = sum(1 for c in enriched if c.get("connect_only"))
    certified_tiles = sum(
        1 for c in enriched if c.get("certification_tier") == "certified" or c.get("transfer_ready")
    )
    source_only = sum(1 for c in enriched if c.get("certification_tier") == "source_only")
    planned_tier = sum(1 for c in enriched if c.get("certification_tier") == "planned")
    alias_tiles = sum(1 for c in enriched if c.get("is_hosted_alias"))
    roadmap_tiles = sum(
        1
        for c in enriched
        if c.get("certification_tier") == "planned" or c.get("effective_status") == "planned"
    )

    try:
        from src.transfer.connector_capabilities import transfer_live_driver_types

        unique_types = transfer_live_driver_types()
    except Exception:
        unique_types = []

    return {
        # Marketplace tile count (includes Planned roadmap + hosted aliases).
        "total": data.get("total", len(data.get("connectors", []))),
        "catalog_tile_total": len(enriched),
        # Primary honesty metric: unique package-available drivers (not alias tiles).
        "live": len(unique_types),
        "beta": by_status.get("beta", 0),
        "planned": by_status.get("planned", 0),
        "categories": len({c.get("category", "other") for c in data.get("connectors", [])}),
        "unique_drivers": len(unique_types),
        "unique_driver_types": unique_types,
        "catalog_tiles": catalog_tiles,
        # Phase E1 — never market alias tiles as distinct engines.
        "alias_tiles": alias_tiles,
        "roadmap_tiles": roadmap_tiles,
        # Backward-compatible alias — prefer unique_drivers in UI.
        "transfer_live": len(unique_types),
        "transfer_live_tiles": catalog_tiles,
        "connect_only": connect_only,
        "roadmap": len(enriched) - catalog_tiles - connect_only,
        # "certified" = unique duplex/live engines (never alias inflation).
        "certified": len(unique_types),
        "certified_tiles": certified_tiles,
        "source_only": source_only,
        "planned_count": planned_tier,
        # Per-side engine counts. "Usable in a transfer" is not the same question
        # as "usable as a source", and answering both with one number counted
        # write-only stores among the sources.
        **_side_driver_counts(enriched),
        "catalog_tiles_are_not_transfer_live": True,
        "customer_tenant_warehouse_sku_claimed": False,
        "honesty_note": (
            "Catalog tiles are not transfer-live. Public live/certified counts "
            "are unique engines (unique_drivers). source_live / dest_live are "
            "the per-side live counts, which differ: a vector store is a destination "
            "only and a REST feed a source only. source_beta / dest_beta count "
            "fixture-proven beta engines, never live. catalog_tile_total includes "
            "Planned roadmap and hosted aliases."
        ),
    }


def get_connector_by_id(connector_id: str) -> dict | None:
    for c in load_catalog().get("connectors", []):
        if c["id"] == connector_id:
            return _with_host_destination_policy(enrich_catalog_entry(c))
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
