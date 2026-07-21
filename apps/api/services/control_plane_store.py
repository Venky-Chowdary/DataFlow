"""Shared Mongo collections for multi-replica Railway / HA control plane.

When Mongo is available, control-plane state must live here — not under
``apps/api/data/*.json`` — so API replicas and worker processes see the same
repair proposals, CDC snapshot signals, and job queue.
"""

from __future__ import annotations

import logging
from typing import Any

_logger = logging.getLogger(__name__)


def mongo_collection(name: str) -> Any | None:
    """Return a real Mongo collection, or None when only memory/file backends exist."""
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
            return None
        if not getattr(mongo, "client", None):
            return None
        return mongo.get_database()[name]
    except Exception:
        _logger.debug("mongo_collection(%s) unavailable", name, exc_info=True)
        return None


def mongo_available() -> bool:
    return mongo_collection("transfer_jobs") is not None
