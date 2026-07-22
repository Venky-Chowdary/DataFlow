"""Stable attribute/header union for schemaless document & KV sources.

Used by DynamoDB, MongoDB, Elasticsearch, Redis flatten, and stream absorb so
sparse fields discovered mid-transfer are never silently dropped.
"""

from __future__ import annotations


def union_attribute_keys(
    *sources: list[str] | set[str] | tuple[str, ...] | None,
) -> list[str]:
    """Stable union of attribute names — earlier sources win order."""
    seen: dict[str, None] = {}
    for src in sources:
        if not src:
            continue
        for name in src:
            n = str(name).strip()
            if n and n not in seen:
                seen[n] = None
    return list(seen.keys())


# Sources whose documents/keys can grow mid-transfer.
SCHEMALESS_SOURCE_TYPES = frozenset({
    "dynamodb",
    "mongodb",
    "elasticsearch",
    "redis",
    "kafka",
})
