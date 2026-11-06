"""Shared Mongo collections for multi-replica Railway / HA control plane.

When Mongo is available, control-plane state must live here — not under
``apps/api/data/*.json`` — so API replicas and worker processes see the same
repair proposals, CDC snapshot signals, and job queue.

Fail-closed marker (``<data dir>/.control_plane_seen``): the first time this
node reaches the shared store it writes this file. From then on
:func:`control_plane_configured` is true on this node even when a later boot
cannot connect and no Mongo URI is in its environment, so shared settings,
safety flags and capture ownership fail closed (``ControlPlaneUnavailable``,
503 on the settings API) instead of reading the node's stale local files.
Deleting the marker on a node that is meant to run without a shared store
(dev / single node) returns it to its local files; on a deployment with a
shared store, never delete it to "fix" an outage — restore the store.
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


def _seen_marker():
    from services.platform_config import data_dir

    return data_dir() / ".control_plane_seen"


def _remember_seen() -> None:
    try:
        marker = _seen_marker()
        if not marker.exists():
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("1", encoding="utf-8")
    except OSError:
        _logger.debug("control-plane marker not written", exc_info=True)


def control_plane_configured() -> bool:
    """A real (non-memory) control-plane store is part of this deployment.

    True when the Mongo service is connected, a Mongo URI is configured, or
    this node has reached the shared store before (a marker in its data
    directory) — so a later boot-time outage fails closed instead of reading
    the node's files (third review). A dev / single-node deployment that never
    had one (or the in-memory job store) keeps its node-local files.
    """
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
    except Exception:  # noqa: BLE001
        return _explicit_mongo_uri() or _seen_marker().exists()
    if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
        return False
    return bool(getattr(mongo, "client", None)) or _explicit_mongo_uri() or _seen_marker().exists()


_last_reconnect = [0.0]
RECONNECT_INTERVAL_S = 30.0


def _try_reconnect() -> None:
    """A client that never connected (boot-time outage) retries, at most every 30 s."""
    import time

    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if type(mongo).__name__ == "MemoryMongoDBService" or getattr(mongo, "client", None):
            return
        now = time.monotonic()
        if now - _last_reconnect[0] < RECONNECT_INTERVAL_S:
            return
        _last_reconnect[0] = now
        mongo.connect()
    except Exception:  # noqa: BLE001 - still down: the caller fails closed
        _logger.debug("control-plane reconnect failed", exc_info=True)


def shared_collection(name: str) -> Any | None:
    """The shared collection; None on a deployment with no control plane.

    Raises :class:`ControlPlaneUnavailable` when one is configured but the
    client is down — the caller must fail closed, not read a local file.
    """
    coll = mongo_collection(name)
    if coll is None:
        _try_reconnect()
        coll = mongo_collection(name)
    if coll is not None:
        _remember_seen()
        return coll
    if control_plane_configured():
        raise ControlPlaneUnavailable("the control-plane database is configured but unreachable")
    return None
