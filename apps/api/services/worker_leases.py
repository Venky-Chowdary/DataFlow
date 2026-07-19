"""Distributed worker lease store for transfer jobs.

Prevents multiple API replicas from running the same job simultaneously.
Uses MongoDB when a shared backend is required; in-memory only for explicit
single-instance / test mode (DATAFLOW_JOB_STORE=memory).

Leases carry a monotonic fencing token. Stale owners that lose the lease must
not continue writing job progress.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

_logger = logging.getLogger(__name__)

# job_id -> fence held by THIS process after a successful acquire
_active_fences: dict[str, int] = {}
_active_lock = threading.Lock()


def worker_id() -> str:
    host = os.environ.get("HOSTNAME") or socket.gethostname() or "local"
    pid = os.getpid()
    token = uuid.uuid4().hex[:8]
    return f"{host}:{pid}:{token}"


def requires_distributed_backend() -> bool:
    """True when memory lease fallback is forbidden (multi-replica / shared Mongo)."""
    store = os.getenv("DATAFLOW_JOB_STORE", "").lower().strip()
    if store == "memory":
        return False
    if os.getenv("DATAFLOW_MULTI_REPLICA", "").lower() in ("1", "true", "yes"):
        return True
    if store in ("mongodb", "mongo"):
        return True
    try:
        from services.platform_config import is_production, is_railway

        if is_railway() and is_production():
            return True
    except Exception:
        pass
    return False


def active_fence(job_id: str) -> int | None:
    with _active_lock:
        return _active_fences.get(job_id)


def clear_active_fence(job_id: str) -> None:
    with _active_lock:
        _active_fences.pop(job_id, None)


class WorkerLeaseStore:
    """Acquire, extend, and release short-lived leases for job execution."""

    _memory: dict[str, dict[str, Any]] = {}
    _lock = threading.Lock()

    def __init__(self, worker_id_str: str = "") -> None:
        self.worker_id = worker_id_str or worker_id()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def _mongo_collection(self):  # type: ignore[no-untyped-def]
        try:
            from services.mongodb_service import get_mongodb_service

            mongo = get_mongodb_service()
            if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
                return None
            if getattr(mongo, "client", None):
                db = mongo.get_database()
                if db is not None:
                    return db["worker_leases"]
        except Exception:
            pass
        return None

    def _remember_fence(self, job_id: str, fence: int) -> None:
        with _active_lock:
            _active_fences[job_id] = fence

    def acquire(self, job_id: str, ttl_seconds: int = 60) -> bool:
        """Try to acquire a lease for ``job_id`` for this worker.

        Returns True on success and records a fencing token for this process.
        When a distributed backend is required and Mongo is unavailable, returns
        False (fail closed) instead of falling back to process-local memory.
        """
        now = self._now()
        expires = now + timedelta(seconds=ttl_seconds)
        coll = self._mongo_collection()
        if coll is not None:
            try:
                existing = coll.find_one({"_id": job_id})
                next_fence = int((existing or {}).get("fence") or 0) + 1
                # CAS: only steal when expired / unowned / already ours. No upsert
                # against a live foreign lease (avoids DuplicateKey → false own).
                result = coll.find_one_and_update(
                    {
                        "_id": job_id,
                        "$or": [
                            {"worker_id": self.worker_id},
                            {"expires_at": {"$lt": now}},
                            {"worker_id": {"$exists": False}},
                            {"worker_id": None},
                        ],
                    },
                    {
                        "$set": {
                            "worker_id": self.worker_id,
                            "expires_at": expires,
                            "updated_at": now,
                            "fence": next_fence,
                        },
                        "$setOnInsert": {"_id": job_id},
                    },
                    upsert=True,
                    return_document=True,
                )
                if result is not None and result.get("worker_id") == self.worker_id:
                    fence = int(result.get("fence") or next_fence)
                    self._remember_fence(job_id, fence)
                    return True
                return False
            except Exception as exc:
                # DuplicateKey on concurrent upsert = lost race, not ownership.
                name = type(exc).__name__
                if "DuplicateKey" in name or "duplicate key" in str(exc).lower():
                    _logger.info("Lease race for job %s (DuplicateKey); not acquired", job_id)
                    return False
                if requires_distributed_backend():
                    _logger.error(
                        "Mongo lease acquire failed for job %s; fail-closed (no memory fallback): %s",
                        job_id,
                        exc,
                    )
                    return False
                _logger.warning(
                    "Mongo lease acquire failed for job %s; single-instance memory fallback: %s",
                    job_id,
                    exc,
                )

        if requires_distributed_backend():
            _logger.error(
                "Distributed coordination required but Mongo lease store unavailable; refuse job %s",
                job_id,
            )
            return False

        with self._lock:
            existing = self._memory.get(job_id)
            if existing is None or existing.get("expires_at", now) < now:
                fence = int((existing or {}).get("fence") or 0) + 1
                self._memory[job_id] = {
                    "worker_id": self.worker_id,
                    "expires_at": expires,
                    "fence": fence,
                }
                self._remember_fence(job_id, fence)
                return True
            if existing.get("worker_id") == self.worker_id:
                self._remember_fence(job_id, int(existing.get("fence") or 1))
                return True
            return False

    def release(self, job_id: str) -> None:
        """Release the lease if this worker owns it."""
        fence = active_fence(job_id)
        coll = self._mongo_collection()
        if coll is not None:
            try:
                filt: dict[str, Any] = {"_id": job_id, "worker_id": self.worker_id}
                if fence is not None:
                    filt["fence"] = fence
                coll.delete_one(filt)
                clear_active_fence(job_id)
                return
            except Exception:
                if requires_distributed_backend():
                    clear_active_fence(job_id)
                    return

        with self._lock:
            existing = self._memory.get(job_id)
            if existing and existing.get("worker_id") == self.worker_id:
                del self._memory[job_id]
        clear_active_fence(job_id)

    def is_held(self, job_id: str) -> bool:
        """Return True if ``job_id`` is currently leased by any active worker."""
        now = self._now()
        coll = self._mongo_collection()
        if coll is not None:
            try:
                return coll.find_one({"_id": job_id, "expires_at": {"$gte": now}}) is not None
            except Exception:
                if requires_distributed_backend():
                    # Fail closed: assume held so we do not start a duplicate.
                    return True

        with self._lock:
            existing = self._memory.get(job_id)
            return existing is not None and existing.get("expires_at", now) >= now

    def heartbeat(self, job_id: str, ttl_seconds: int = 60) -> bool:
        """Extend the lease if this worker still owns it (same fence)."""
        now = self._now()
        expires = now + timedelta(seconds=ttl_seconds)
        fence = active_fence(job_id)
        coll = self._mongo_collection()
        if coll is not None:
            try:
                filt: dict[str, Any] = {"_id": job_id, "worker_id": self.worker_id}
                if fence is not None:
                    filt["fence"] = fence
                result = coll.find_one_and_update(
                    filt,
                    {"$set": {"expires_at": expires, "updated_at": now}},
                    return_document=True,
                )
                return result is not None
            except Exception:
                if requires_distributed_backend():
                    return False

        with self._lock:
            existing = self._memory.get(job_id)
            if (
                existing
                and existing.get("worker_id") == self.worker_id
                and existing.get("expires_at", now) >= now
                and (fence is None or int(existing.get("fence") or 0) == fence)
            ):
                existing["expires_at"] = expires
                return True
            return False

    def get_fence(self, job_id: str) -> int | None:
        """Return the fence for a held lease (Mongo or memory), if any."""
        local = active_fence(job_id)
        if local is not None:
            return local
        coll = self._mongo_collection()
        if coll is not None:
            try:
                doc = coll.find_one({"_id": job_id, "worker_id": self.worker_id})
                if doc:
                    return int(doc.get("fence") or 0) or None
            except Exception:
                pass
        with self._lock:
            existing = self._memory.get(job_id)
            if existing and existing.get("worker_id") == self.worker_id:
                return int(existing.get("fence") or 0) or None
        return None


def get_worker_lease_store(worker_id: str = "") -> WorkerLeaseStore:
    return WorkerLeaseStore(worker_id)
