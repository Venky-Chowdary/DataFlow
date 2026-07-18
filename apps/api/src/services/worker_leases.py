"""Compatibility shim: canonical implementation now lives in services.worker_leases."""
from __future__ import annotations

from services.worker_leases import WorkerLeaseStore, get_worker_lease_store

__all__ = ["WorkerLeaseStore", "get_worker_lease_store"]
