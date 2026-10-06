"""Policy gates that are a note, not a probe.

Kept out of ``preflight_service`` so that module stays inside its line freeze.
"""

from __future__ import annotations

_REDIS = {
    "redis",
    "redis_enterprise",
    "amazon_elasticache_redis",
    "azure_cache_redis",
    "google_memorystore_redis",
}


def redis_ttl_policy_gate(dest_type: str, source_type: str) -> dict | None:
    """Redis TTL/EXPIRE is not a migration guarantee. Warn, do not block."""
    dest = (dest_type or "").strip().lower()
    src = (source_type or "").strip().lower()
    if dest not in _REDIS and src not in _REDIS:
        return None
    return {
        "id": "redis_ttl_semantics",
        "name": "Redis TTL / EXPIRE",
        "status": "pass",
        "severity": "warn",
        "message": (
            "Redis TTL/EXPIRE is not preserved as a migration guarantee — "
            "values transfer; set EXPIRE in a post-load job if needed. "
            "See docs/REDIS_TTL_SEMANTICS.md."
        ),
        "blocks_transfer": False,
        "details": {"honesty": "ttl_not_productized"},
    }
