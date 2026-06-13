"""620+ connector catalog — searchable marketplace like Airbyte sources/destinations."""

from __future__ import annotations
import json
import os
from functools import lru_cache

_CATALOG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "connector_catalog.json"
)

SUGGESTED_SOURCES = [
    "postgresql", "mongodb", "mysql", "csv___tsv", "json", "excel",
    "salesforce", "shopify", "stripe", "github", "kafka", "s3",
]
SUGGESTED_DESTINATIONS = [
    "snowflake", "postgresql", "mongodb", "bigquery", "redshift",
    "pinecone", "clickhouse", "databricks", "s3", "hubspot",
]


@lru_cache(maxsize=1)
def load_catalog() -> dict:
    with open(_CATALOG_PATH, encoding="utf-8") as f:
        return json.load(f)


def search_catalog(
    query: str = "",
    role: str = "all",
    category: str = "",
    status: str = "",
    limit: int = 60,
) -> dict:
    data = load_catalog()
    connectors = data.get("connectors", [])

    if role == "source":
        suggested_ids = set(SUGGESTED_SOURCES)
        connectors = sorted(connectors, key=lambda c: (c["id"] not in suggested_ids, c["name"]))
    elif role == "destination":
        suggested_ids = set(SUGGESTED_DESTINATIONS)
        connectors = sorted(connectors, key=lambda c: (c["id"] not in suggested_ids, c["name"]))

    q = query.lower().strip()
    if q:
        connectors = [
            c for c in connectors
            if q in c["name"].lower() or q in c["id"].lower()
            or q in c.get("category", "").lower()
            or q in c.get("description", "").lower()
        ]

    if category:
        connectors = [c for c in connectors if c.get("category") == category]

    if status:
        connectors = [c for c in connectors if c.get("status") == status]

    total = len(connectors)
    page = connectors[:limit]

    suggested = []
    ids = SUGGESTED_SOURCES if role == "source" else SUGGESTED_DESTINATIONS if role == "destination" else []
    if ids and not q:
        by_id = {c["id"]: c for c in data.get("connectors", [])}
        suggested = [by_id[i] for i in ids if i in by_id][:16]

    categories = sorted({c.get("category", "other") for c in data.get("connectors", [])})

    return {
        "total": data.get("total", total),
        "filtered": total,
        "connectors": page,
        "suggested": suggested if not q else page[:16],
        "categories": categories,
    }


def get_connector_by_id(connector_id: str) -> dict | None:
    for c in load_catalog().get("connectors", []):
        if c["id"] == connector_id:
            return c
    return None


def catalog_training_docs(limit: int | None = None) -> list[dict]:
    """RAG documents so Data Pilot knows every connector in the catalog."""
    connectors = load_catalog().get("connectors", [])
    if limit is not None:
        connectors = connectors[:limit]

    docs = []
    for c in connectors:
        docs.append({
            "id": f"catalog_{c['id']}",
            "text": (
                f"Connector: {c['name']} ({c['id']})\n"
                f"Category: {c.get('category', 'unknown')}\n"
                f"Status: {c.get('status', 'planned')}\n"
                f"Description: {c.get('description', '')}\n"
                f"Use as source or destination in DataTransfer.space universal transfer.\n"
                f"Data Pilot can help set up {c['name']} as a source or destination, "
                f"map schemas, and run any→any transfers."
            ),
            "metadata": {
                "type": "copilot_knowledge",
                "connector_id": c["id"],
                "category": c.get("category", ""),
                "status": c.get("status", ""),
            },
        })
    return docs
