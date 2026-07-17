"""Distributed worker lease store for transfer jobs.

Prevents multiple Railway replicas from running the same job simultaneously.
Uses MongoDB when connected, otherwise falls back to an in-process dict for
single-instance tests.
"""

from __future__ import annotations

import os
import socket
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any


def worker_id() -> str:
    host = os.environ.get("HOSTNAME") or socket.gethostname() or "local"
    pid = os.getpid()
    token = uuid.uuid4().hex[:8]
    return f"{host}:{pid}:{token}"


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
            if mongo and getattr(mongo, "client", None):
                db = mongo.get_database()
                if db:
                    return db["worker_leases"]
        except Exception:
            pass
        return None

    def acquire(self, job_id: str, ttl_seconds: int = 60) -> bool:
        """Try to acquire a lease for ``job_id`` for this worker."""
        now = self._now()
        expires = now + timedelta(seconds=ttl_seconds)
        coll = self._mongo_collection()
        if coll is not None:
            try:
                result = coll.find_one_and_update(
                    {
                        "_id": job_id,
                        "$or": [
                            {"worker_id": self.worker_id},
                            {"expires_at": {"$lt": now}},
                            {"worker_id": {"$exists": False}},
                        ],
                    },
                    {
                        "$set": {
                            "worker_id": self.worker_id,
                            "expires_at": expires,
                            "updated_at": now,
                        }
                    },
                    upsert=True,
                    return_document=True,
                )
                return result is not None and result.get("worker_id") == self.worker_id
            except Exception:
                # Fall through to in-memory on any MongoDB error.
                pass

        with self._lock:
            existing = self._memory.get(job_id)
            if existing is None or existing.get("expires_at", now) < now:
                self._memory[job_id] = {
                    "worker_id": self.worker_id,
                    "expires_at": expires,
                }
                return True
            return existing.get("worker_id") == self.worker_id

    def release(self, job_id: str) -> None:
        """Release the lease if this worker owns it."""
        coll = self._mongo_collection()
        if coll is not None:
            try:
                coll.delete_one({"_id": job_id, "worker_id": self.worker_id})
                return
            except Exception:
                pass

        with self._lock:
            existing = self._memory.get(job_id)
            if existing and existing.get("worker_id") == self.worker_id:
                del self._memory[job_id]

    def is_held(self, job_id: str) -> bool:
        """Return True if ``job_id`` is currently leased by any active worker."""
        now = self._now()
        coll = self._mongo_collection()
        if coll is not None:
            try:
                return coll.find_one({"_id": job_id, "expires_at": {"$gte": now}}) is not None
            except Exception:
                pass

        with self._lock:
            existing = self._memory.get(job_id)
            return existing is not None and existing.get("expires_at", now) >= now

    def heartbeat(self, job_id: str, ttl_seconds: int = 60) -> bool:
        """Extend the lease if this worker still owns it."""
        now = self._now()
        expires = now + timedelta(seconds=ttl_seconds)
        coll = self._mongo_collection()
        if coll is not None:
            try:
                result = coll.find_one_and_update(
                    {"_id": job_id, "worker_id": self.worker_id},
                    {"$set": {"expires_at": expires, "updated_at": now}},
                    return_document=True,
                )
                return result is not None
            except Exception:
                pass

        with self._lock:
            existing = self._memory.get(job_id)
            if existing and existing.get("worker_id") == self.worker_id and existing.get("expires_at", now) >= now:
                existing["expires_at"] = expires
                return True
            return False


def get_worker_lease_store(worker_id: str = "") -> WorkerLeaseStore:
    return WorkerLeaseStore(worker_id)
