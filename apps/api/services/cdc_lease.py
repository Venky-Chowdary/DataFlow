"""Durable CDC resource leases (multi-worker fail-fast).

Under user load, two jobs must not share the same CDC resource (PostgreSQL
logical slot, MySQL binlog ``server_id``, SQL Server capture instance / CT
table, or Oracle LogMiner / flashback stream).

Backends (``DATAFLOW_CDC_LEASE_BACKEND``)
----------------------------------------
- ``auto`` (default): Redis when ``DATAFLOW_CDC_LEASE_REDIS_URL`` /
  ``DATAFLOW_REDIS_URL`` is set, otherwise file.
- ``redis``: multi-node authority; **fail-closed** if Redis is unreachable
  (never silent split-brain to a local file).
- ``file``: single-host JSON + ``fcntl`` flock.
- ``memory``: process-local (tests).

Semantics
---------
- ``acquire`` succeeds if free, held by the same ``holder_id``, or stale
  (heartbeat older than TTL). Steal increments a fencing ``generation``.
- ``renew`` / ``release`` require matching holder **and** generation so a
  zombie after steal cannot keep the lease alive.
- Conflict raises ``CdcLeaseConflict`` — callers fail-fast (no silent double-read).
- Delivery remains **at-least-once**; leases only prevent concurrent consumers.
"""

from __future__ import annotations

import os
import socket
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from services.cdc_lease_store import (
    LeaseStoreError,
    configure_store,
    get_store,
    reset_store,
    resolve_backend_name,
)

_DEFAULT_TTL = float(os.getenv("DATAFLOW_CDC_LEASE_TTL_SEC", "120"))

# Re-export for callers / tests.
__all__ = [
    "CdcLease",
    "CdcLeaseConflict",
    "CdcLeaseGuard",
    "LeaseStoreError",
    "acquire_lease",
    "configure_store",
    "get_lease",
    "get_store",
    "lease_backend_name",
    "lease_view",
    "mssql_cdc_resource",
    "mssql_cdc_shared_resource",
    "new_holder_id",
    "oracle_cdc_resource",
    "oracle_cdc_shared_resource",
    "force_release_lease",
    "parse_holder_job_id",
    "release_lease",
    "renew_lease",
    "reset_store",
]


class CdcLeaseConflict(RuntimeError):
    """Another worker holds this CDC resource."""

    def __init__(
        self,
        message: str,
        *,
        holder_id: str = "",
        resource: str = "",
        cursor_key: str = "",
    ) -> None:
        super().__init__(message)
        self.holder_id = holder_id
        self.resource = resource
        self.cursor_key = cursor_key

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": "cdc_lease_conflict",
            "message": str(self),
            "holder_id": self.holder_id,
            "resource": self.resource,
            "cursor_key": self.cursor_key,
        }


@dataclass
class CdcLease:
    cursor_key: str
    resource: str
    holder_id: str
    acquired_at: float = field(default_factory=time.time)
    heartbeat_at: float = field(default_factory=time.time)
    ttl_sec: float = _DEFAULT_TTL
    generation: int = 1
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "CdcLease":
        def _ts(key: str) -> float:
            if key in d and d[key] is not None:
                return float(d[key])
            return time.time()

        return cls(
            cursor_key=str(d.get("cursor_key") or ""),
            resource=str(d.get("resource") or ""),
            holder_id=str(d.get("holder_id") or ""),
            acquired_at=_ts("acquired_at"),
            heartbeat_at=_ts("heartbeat_at"),
            ttl_sec=float(d["ttl_sec"]) if d.get("ttl_sec") is not None else _DEFAULT_TTL,
            generation=int(d.get("generation") or 1),
            meta=dict(d.get("meta") or {}),
        )

    def is_stale(self, *, now: float | None = None) -> bool:
        ts = now if now is not None else time.time()
        # Floor of 1s avoids flapping; TTL itself is the authority.
        return (ts - float(self.heartbeat_at)) > max(1.0, float(self.ttl_sec))


def new_holder_id(job_id: str = "") -> str:
    host = socket.gethostname()[:32]
    jid = (job_id or "job")[:24]
    return f"{host}:{jid}:{uuid.uuid4().hex[:10]}"


def parse_holder_job_id(holder_id: str) -> str | None:
    """Best-effort job id from ``host:job_id:nonce`` holder format."""
    parts = [p for p in str(holder_id or "").split(":") if p]
    if len(parts) < 3:
        return None
    # host may itself contain colons (IPv6) — job id is second-to-last.
    jid = parts[-2].strip()
    if not jid or jid == "job":
        return None
    return jid


def lease_backend_name() -> str:
    try:
        return get_store().name
    except Exception:
        return resolve_backend_name()


def acquire_lease(
    cursor_key: str,
    *,
    resource: str,
    holder_id: str,
    ttl_sec: float | None = None,
    meta: dict[str, Any] | None = None,
) -> CdcLease:
    """Acquire or renew a lease for ``cursor_key`` / ``resource``.

    Raises ``CdcLeaseConflict`` when another live holder owns the key or the
    same resource under a different cursor key. Raises ``LeaseStoreError`` when
    a configured Redis backend is unreachable (fail-closed).
    """
    key = (cursor_key or "").strip() or f"resource:{resource}"
    res = (resource or "").strip() or key
    holder = (holder_id or "").strip() or new_holder_id()
    ttl = float(ttl_sec if ttl_sec is not None else _DEFAULT_TTL)
    raw = get_store().acquire(
        cursor_key=key,
        resource=res,
        holder_id=holder,
        ttl_sec=ttl,
        meta=dict(meta or {}),
    )
    return CdcLease.from_dict(raw)


def renew_lease(
    cursor_key: str,
    *,
    holder_id: str,
    generation: int | None = None,
) -> CdcLease | None:
    key = (cursor_key or "").strip()
    if not key:
        return None
    store = get_store()
    gen = generation
    if gen is None:
        existing = store.get(key)
        if not existing or existing.get("holder_id") != holder_id:
            return None
        gen = int(existing.get("generation") or 1)
    raw = store.renew(cursor_key=key, holder_id=holder_id, generation=int(gen))
    return CdcLease.from_dict(raw) if raw else None


def release_lease(
    cursor_key: str,
    *,
    holder_id: str,
    generation: int | None = None,
) -> bool:
    key = (cursor_key or "").strip()
    if not key:
        return False
    store = get_store()
    gen = generation
    if gen is None:
        existing = store.get(key)
        if not existing or existing.get("holder_id") != holder_id:
            return False
        gen = int(existing.get("generation") or 1)
    return store.release(cursor_key=key, holder_id=holder_id, generation=int(gen))


def get_lease(cursor_key: str) -> CdcLease | None:
    key = (cursor_key or "").strip()
    if not key:
        return None
    raw = get_store().get(key)
    return CdcLease.from_dict(raw) if raw else None


def force_release_lease(
    cursor_key: str,
    *,
    expected_generation: int | None = None,
    reason: str = "",
    actor: str = "",
) -> dict[str, Any]:
    """Operator break of a live CDC lease (fencing-aware).

    Releases using the current holder + generation so zombies cannot renew.
    When ``expected_generation`` is set and does not match, refuses to break
    (another steal already advanced the fence).
    """
    key = (cursor_key or "").strip()
    if not key:
        return {"released": False, "reason": "missing_cursor_key", "cursor_key": ""}
    view = lease_view(key)
    if view is None:
        return {
            "released": False,
            "reason": "not_found",
            "cursor_key": key,
            "backend": lease_backend_name(),
        }
    gen = int(view.get("generation") or 1)
    if expected_generation is not None and int(expected_generation) != gen:
        return {
            "released": False,
            "reason": "generation_mismatch",
            "cursor_key": key,
            "lease": view,
            "backend": view.get("backend") or lease_backend_name(),
        }
    holder = str(view.get("holder_id") or "")
    ok = release_lease(key, holder_id=holder, generation=gen)
    result = {
        "released": bool(ok),
        "reason": "ok" if ok else "release_failed",
        "cursor_key": key,
        "prior": view,
        "backend": view.get("backend") or lease_backend_name(),
        "note": (reason or "")[:300],
        "actor": (actor or "")[:120],
        "holder_job_id": parse_holder_job_id(holder)
        or (view.get("meta") or {}).get("job_id"),
    }
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action="cdc_lease.force_release",
            resource=key,
            actor=actor or "operator",
            details={
                "cursor_key": key,
                "released": bool(ok),
                "reason": result["reason"],
                "generation": gen,
                "holder_id": holder,
                "resource": view.get("resource"),
                "note": (reason or "")[:300],
            },
        )
    except Exception:
        pass
    return result


def lease_view(cursor_key: str) -> dict[str, Any] | None:
    """Operator-facing lease snapshot for Job Theater / lag fields."""
    lease = get_lease(cursor_key)
    if lease is None:
        return None
    now = time.time()
    return {
        "cursor_key": lease.cursor_key,
        "resource": lease.resource,
        "holder_id": lease.holder_id,
        "ttl_sec": lease.ttl_sec,
        "heartbeat_at": lease.heartbeat_at,
        "acquired_at": lease.acquired_at,
        "generation": lease.generation,
        "stale": lease.is_stale(now=now),
        "age_sec": max(0.0, now - float(lease.heartbeat_at)),
        "meta": dict(lease.meta or {}),
        "backend": lease_backend_name(),
    }


def mssql_cdc_resource(
    database: str,
    schema: str,
    table: str,
    *,
    mode: str = "cdc",
) -> str:
    """Stable lease resource for SQL Server native CDC or Change Tracking."""
    db = (database or "master").strip().lower()
    sch = (schema or "dbo").strip().lower()
    tbl = (table or "").strip().lower()
    kind = "ct" if mode in {"ct", "change_tracking"} else "cdc"
    return f"mssql_{kind}:{db}:{sch}.{tbl}"


def mssql_cdc_shared_resource(
    database: str,
    schema: str,
    tables: list[str],
    *,
    mode: str = "cdc",
) -> str:
    """Lease for a shared multi-table SQL Server native CDC consumer."""
    from services.cdc_multi_table import tables_digest

    db = (database or "master").strip().lower()
    sch = (schema or "dbo").strip().lower()
    kind = "ct" if mode in {"ct", "change_tracking"} else "cdc"
    digest = tables_digest(tables)
    return f"mssql_{kind}_shared:{db}:{sch}:{digest}"


def oracle_cdc_resource(
    schema: str,
    table: str,
    *,
    mode: str = "logminer",
    host: str = "",
) -> str:
    """Stable lease resource for Oracle LogMiner or flashback CDC."""
    sch = (schema or "").strip().upper()
    tbl = (table or "").strip().upper()
    kind = "flashback" if mode in {"flashback", "versions"} else "logminer"
    host_part = (host or "").strip().lower()
    if host_part:
        return f"oracle_{kind}:{host_part}:{sch}.{tbl}"
    return f"oracle_{kind}:{sch}.{tbl}"


def oracle_cdc_shared_resource(
    schema: str,
    tables: list[str],
    *,
    mode: str = "logminer",
    host: str = "",
) -> str:
    """Lease for a shared multi-table Oracle LogMiner consumer."""
    from services.cdc_multi_table import tables_digest

    sch = (schema or "").strip().upper()
    kind = "flashback" if mode in {"flashback", "versions"} else "logminer"
    digest = tables_digest([str(t).upper() for t in tables])
    host_part = (host or "").strip().lower()
    if host_part:
        return f"oracle_{kind}_shared:{host_part}:{sch}:{digest}"
    return f"oracle_{kind}_shared:{sch}:{digest}"


@dataclass
class CdcLeaseGuard:
    """Shared acquire/renew/release for CDC connectors (no duplicated lease logic)."""

    cursor_key: str
    resource: str
    holder_id: str = ""
    job_id: str = ""
    meta: dict[str, Any] = field(default_factory=dict)
    ttl_sec: float | None = None
    _acquired: bool = field(default=False, init=False, repr=False)
    _generation: int = field(default=0, init=False, repr=False)

    @property
    def acquired(self) -> bool:
        return self._acquired

    @property
    def generation(self) -> int:
        return self._generation

    def ensure(self) -> CdcLease:
        """Acquire on first call; renew heartbeat on subsequent calls."""
        if self._acquired:
            if self.holder_id and self._generation:
                renewed = renew_lease(
                    self.cursor_key,
                    holder_id=self.holder_id,
                    generation=self._generation,
                )
                if renewed is not None:
                    self._generation = int(renewed.generation)
                    return renewed
            # Lost our lease (stolen / expired / fenced) — re-acquire fail-fast.
            self._acquired = False
            self._generation = 0
        if not self.holder_id:
            self.holder_id = new_holder_id(self.job_id)
        meta = dict(self.meta or {})
        if self.job_id and "job_id" not in meta:
            meta["job_id"] = self.job_id
        lease = acquire_lease(
            self.cursor_key,
            resource=self.resource,
            holder_id=self.holder_id,
            ttl_sec=self.ttl_sec,
            meta=meta,
        )
        self._acquired = True
        self._generation = int(lease.generation)
        return lease

    def renew(self) -> CdcLease | None:
        if not self._acquired or not self.holder_id or not self._generation:
            return None
        renewed = renew_lease(
            self.cursor_key,
            holder_id=self.holder_id,
            generation=self._generation,
        )
        if renewed is None:
            self._acquired = False
            self._generation = 0
            return None
        self._generation = int(renewed.generation)
        return renewed

    def release(self) -> bool:
        if not self._acquired or not self.holder_id:
            return False
        ok = False
        try:
            ok = release_lease(
                self.cursor_key,
                holder_id=self.holder_id,
                generation=self._generation or None,
            )
        finally:
            self._acquired = False
            self._generation = 0
        return ok

    def theater_fields(self) -> dict[str, Any]:
        """Fields safe to promote onto the job document for UI tiles."""
        view = lease_view(self.cursor_key) if self.cursor_key else None
        backend = lease_backend_name()
        if not view and self._acquired:
            return {
                "cdc_lease_holder": self.holder_id,
                "cdc_lease_resource": self.resource,
                "cdc_lease_stale": False,
                "cdc_lease_backend": backend,
                "cdc_lease_generation": self._generation or None,
            }
        if not view:
            return {"cdc_lease_backend": backend} if backend else {}
        return {
            "cdc_lease_holder": view.get("holder_id"),
            "cdc_lease_resource": view.get("resource"),
            "cdc_lease_stale": bool(view.get("stale")),
            "cdc_lease_heartbeat_age_sec": view.get("age_sec"),
            "cdc_lease_backend": view.get("backend") or backend,
            "cdc_lease_generation": view.get("generation"),
        }
