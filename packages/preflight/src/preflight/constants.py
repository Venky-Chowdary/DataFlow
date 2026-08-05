"""Shared preflight constants."""

from __future__ import annotations

# Canonical schemaless engines (after alias normalize).
SCHEMALESS_DESTS: frozenset[str] = frozenset({"mongodb", "dynamodb", "redis"})

# Product catalog / vendor aliases that must resolve to SCHEMALESS_DESTS.
# Kept local so package-only preflight does not soft-miss documentdb/firestore.
_SCHEMALESS_ALIAS_TO_CANON: dict[str, str] = {
    "mongo": "mongodb",
    "mongodb": "mongodb",
    "documentdb": "mongodb",
    "document_db": "mongodb",
    "cosmos": "mongodb",
    "cosmos-mongodb": "mongodb",
    "cosmos_mongodb": "mongodb",
    "cosmosdb": "mongodb",
    "firestore": "mongodb",
    "dynamodb": "dynamodb",
    "amazon_dynamodb": "dynamodb",
    "redis": "redis",
    "redis-kv": "redis",
    "redis_kv": "redis",
}


def schemaless_dest_canonical(db_type: str | None) -> str:
    """Normalize destination kind for schemaless membership checks."""
    db = (db_type or "").strip().lower()
    if not db:
        return ""
    if db in _SCHEMALESS_ALIAS_TO_CANON:
        return _SCHEMALESS_ALIAS_TO_CANON[db]
    # Prefer type_system SSOT when hosted (covers future aliases).
    try:
        from services.type_system import _normalize_dest_db

        return _normalize_dest_db(db)
    except ImportError:
        return db


def is_schemaless_dest(db_type: str | None) -> bool:
    """True when destination is a document/KV store without relational DDL contract."""
    return schemaless_dest_canonical(db_type) in SCHEMALESS_DESTS
