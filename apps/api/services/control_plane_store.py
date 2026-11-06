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


class ControlPlaneUnavailable(RuntimeError):
    """A control-plane database is configured but cannot be reached right now.

    State that must be the same on every replica (deployment settings, safety
    flags, capture ownership) must then fail closed — never fall back to this
    node's local file, which may hold another, stale value.
    """


def _explicit_mongo_uri() -> bool:
    import os

    for key in ("MONGODB_URI", "MONGO_URL", "MONGO_PRIVATE_URL", "MONGODB_URL", "MONGO_PUBLIC_URL"):
        val = os.getenv(key, "").strip()
        if val and "mongo" in val.lower():
            return True
    return False


def control_plane_configured() -> bool:
    """A real (non-memory) control-plane store is part of this deployment.

    True when the Mongo service is connected, or a Mongo URI is configured
    even if the client is down right now. A dev/single-node deployment with
    neither (or the in-memory job store) keeps its node-local files.
    """
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
    except Exception:  # noqa: BLE001
        return _explicit_mongo_uri()
    if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
        return False
    return bool(getattr(mongo, "client", None)) or _explicit_mongo_uri()


def shared_collection(name: str) -> Any | None:
    """The shared collection; None on a deployment with no control plane.

    Raises :class:`ControlPlaneUnavailable` when one is configured but the
    client is down — the caller must fail closed, not read a local file.
    """
    coll = mongo_collection(name)
    if coll is not None:
        return coll
    if control_plane_configured():
        raise ControlPlaneUnavailable(
            f"the control-plane database is configured but unreachable ({name})"
        )
    return None
