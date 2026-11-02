"""CDC lease persistence backends (file / memory / Redis).

Multi-node workers must share one lease authority. Redis is the distributed
backend; file remains single-host. When Redis is explicitly selected, acquire
is **fail-closed** on connectivity errors (never silent split-brain to file).

Generation counters survive releases. A live or stale legacy lease without
counters seeds them from its generation; a lease released before counters
existed has no recoverable generation and starts again at 1.
"""

from __future__ import annotations

import json
import logging
import os
from services.brand_env import getenv_brand
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

LEASE_KEY_PREFIX = "df:cdc:lease:"
RESOURCE_KEY_PREFIX = "df:cdc:res:"
GENERATION_KEY_PREFIX = "df:cdc:gen:"


def _warn_generation_seed(
    *,
    cursor_key: str,
    resource: str,
    store_generation: int,
    min_generation: int,
    backend: str,
) -> None:
    if int(min_generation) <= int(store_generation):
        return
    logger.warning(
        "CDC lease generation seeded cursor_key=%s resource=%s "
        "store_generation=%s dest_fence=%s backend=%s",
        cursor_key,
        resource,
        int(store_generation),
        int(min_generation),
        backend,
    )

# Atomic acquire: resource cross-index + fencing generation + stale steal.
_REDIS_ACQUIRE_SCRIPT = """
local lease_key = KEYS[1]
local res_key = KEYS[2]
local cursor_generation_key = KEYS[3]
local resource_generation_key = KEYS[4]
local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local holder = ARGV[3]
local cursor_key = ARGV[4]
local resource = ARGV[5]
local meta_json = ARGV[6]
local stale_floor = tonumber(ARGV[7])
local gc_ttl = tonumber(ARGV[8])
local min_generation = tonumber(ARGV[9]) or 0

local function is_stale(lease)
  if not lease then return true end
  local hb = tonumber(lease["heartbeat_at"] or 0)
  local lease_ttl = tonumber(lease["ttl_sec"] or ttl)
  if lease_ttl < stale_floor then lease_ttl = stale_floor end
  return (now - hb) > lease_ttl
end

local owner_ck = redis.call("GET", res_key)
local cursor_generation = tonumber(redis.call("GET", cursor_generation_key) or 0)
local resource_generation = tonumber(redis.call("GET", resource_generation_key) or 0)
local prior_generation = 0
-- lease_key = <prefix>df:cdc:lease:<cursor_key>; derive the prefix so we can look up
-- other cursor keys under the same resource without hard-coding the key namespace.
local base = string.sub(lease_key, 1, -(#cursor_key + 1))
if owner_ck and owner_ck ~= cursor_key then
  local other_key = base .. owner_ck
  local other_raw = redis.call("GET", other_key)
  if other_raw then
    local other = cjson.decode(other_raw)
    if not is_stale(other) and other["holder_id"] ~= holder then
      return {"err", "conflict", "holder", other["holder_id"], "cursor", other["cursor_key"] or owner_ck, "resource", other["resource"] or resource}
    end
    prior_generation = tonumber(other["generation"] or 0)
    local other_generation_key = string.sub(cursor_generation_key, 1, -(#cursor_key + 1)) .. owner_ck
    local other_generation = tonumber(redis.call("GET", other_generation_key) or 0)
    if prior_generation > other_generation then
      redis.call("SET", other_generation_key, prior_generation)
    end
    redis.call("DEL", other_key)
  end
  redis.call("DEL", res_key)
end

local existing_raw = redis.call("GET", lease_key)
local generation = 1
local acquired_at = now
local existing_generation = 0
if existing_raw then
  local existing = cjson.decode(existing_raw)
  if existing["holder_id"] ~= holder and not is_stale(existing) then
    return {"err", "conflict", "holder", existing["holder_id"], "cursor", existing["cursor_key"] or cursor_key, "resource", existing["resource"] or resource}
  end
  if existing["holder_id"] == holder then
    existing_generation = tonumber(existing["generation"] or 1)
    generation = existing_generation
    acquired_at = tonumber(existing["acquired_at"] or now)
  else
    existing_generation = tonumber(existing["generation"] or 0)
    generation = math.max(cursor_generation, resource_generation, prior_generation, existing_generation, min_generation) + 1
  end
else
  generation = math.max(cursor_generation, resource_generation, prior_generation, min_generation) + 1
end
local pre_seed_floor = math.max(cursor_generation, resource_generation, prior_generation, existing_generation)
if existing_raw and existing_generation <= min_generation then
  generation = math.max(pre_seed_floor, min_generation) + 1
end

local lease = {
  cursor_key = cursor_key,
  resource = resource,
  holder_id = holder,
  acquired_at = acquired_at,
  heartbeat_at = now,
  ttl_sec = ttl,
  generation = generation,
  meta = cjson.decode(meta_json)
}
local payload = cjson.encode(lease)
redis.call("SET", cursor_generation_key, math.max(cursor_generation, generation))
redis.call("SET", resource_generation_key, math.max(resource_generation, generation))
redis.call("SET", lease_key, payload, "EX", gc_ttl)
redis.call("SET", res_key, cursor_key, "EX", gc_ttl)
return {"status", "ok", "payload", payload, "floor", tostring(pre_seed_floor)}
"""

_REDIS_RENEW_SCRIPT = """
local lease_key = KEYS[1]
local res_key = KEYS[2]
local now = tonumber(ARGV[1])
local holder = ARGV[2]
local generation = tonumber(ARGV[3])
local gc_ttl = tonumber(ARGV[4])

local raw = redis.call("GET", lease_key)
if not raw then
  return {"err", "missing"}
end
local lease = cjson.decode(raw)
if lease["holder_id"] ~= holder then
  return {"err", "holder"}
end
if tonumber(lease["generation"] or 0) ~= generation then
  return {"err", "fence"}
end
lease["heartbeat_at"] = now
local payload = cjson.encode(lease)
redis.call("SET", lease_key, payload, "EX", gc_ttl)
if res_key and res_key ~= "" then
  redis.call("SET", res_key, lease["cursor_key"], "EX", gc_ttl)
end
return {"status", "ok", "payload", payload}
"""

_REDIS_RELEASE_SCRIPT = """
local lease_key = KEYS[1]
local res_key = KEYS[2]
local cursor_generation_key = KEYS[3]
local resource_generation_key = KEYS[4]
local holder = ARGV[1]
local generation = tonumber(ARGV[2])

local raw = redis.call("GET", lease_key)
if not raw then
  return {"err", "missing"}
end
local lease = cjson.decode(raw)
if lease["holder_id"] ~= holder then
  return {"err", "holder"}
end
if tonumber(lease["generation"] or 0) ~= generation then
  return {"err", "fence"}
end
local lease_generation = tonumber(lease["generation"] or 0)
local cursor_generation = tonumber(redis.call("GET", cursor_generation_key) or 0)
local resource_generation = tonumber(redis.call("GET", resource_generation_key) or 0)
if lease_generation > cursor_generation then
  redis.call("SET", cursor_generation_key, lease_generation)
end
if lease_generation > resource_generation then
  redis.call("SET", resource_generation_key, lease_generation)
end
redis.call("DEL", lease_key)
local owner = redis.call("GET", res_key)
if owner == lease["cursor_key"] then
  redis.call("DEL", res_key)
end
return {"status", "ok"}
"""


def _sorted_leases(raws: Any) -> list[dict[str, Any]]:
    """Copy lease dicts, most recently heartbeated first."""
    out = [dict(r) for r in raws or [] if isinstance(r, dict)]
    out.sort(key=lambda r: float(r.get("heartbeat_at") or 0.0), reverse=True)
    return out


class LeaseStoreError(RuntimeError):
    """Backend unavailable or misconfigured (fail-closed)."""


class LeaseStore(ABC):
    name: str = "abstract"

    @abstractmethod
    def acquire(
        self,
        *,
        cursor_key: str,
        resource: str,
        holder_id: str,
        ttl_sec: float,
        meta: dict[str, Any],
        now: float | None = None,
        min_generation: int = 0,
    ) -> dict[str, Any]:
        """Return lease dict or raise CdcLeaseConflict / LeaseStoreError."""

    @abstractmethod
    def renew(
        self,
        *,
        cursor_key: str,
        holder_id: str,
        generation: int,
        now: float | None = None,
    ) -> dict[str, Any] | None:
        ...

    @abstractmethod
    def release(
        self,
        *,
        cursor_key: str,
        holder_id: str,
        generation: int,
    ) -> bool:
        ...

    @abstractmethod
    def get(self, cursor_key: str) -> dict[str, Any] | None:
        ...

    @abstractmethod
    def list_leases(self) -> list[dict[str, Any]]:
        """Every lease currently held, newest heartbeat first.

        A conflict names the *resource* it refused on (``mysql_server_id:90269``),
        but breaking a lease needs its ``cursor_key``. Without enumeration an
        operator whose worker died cannot find the key that is blocking them.
        """

    def debug_set_heartbeat(self, cursor_key: str, heartbeat_at: float) -> None:
        """Test helper — not for production paths."""
        raise NotImplementedError


class MemoryLeaseStore(LeaseStore):
    """Process-local store for unit tests."""

    name = "memory"

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._leases: dict[str, dict[str, Any]] = {}
        self._by_resource: dict[str, str] = {}
        self._last_generation_by_cursor: dict[str, int] = {}
        self._last_generation_by_resource: dict[str, int] = {}

    def clear(self) -> None:
        with self._lock:
            self._leases.clear()
            self._by_resource.clear()
            self._last_generation_by_cursor.clear()
            self._last_generation_by_resource.clear()

    def acquire(
        self,
        *,
        cursor_key: str,
        resource: str,
        holder_id: str,
        ttl_sec: float,
        meta: dict[str, Any],
        now: float | None = None,
        min_generation: int = 0,
    ) -> dict[str, Any]:
        from services.cdc_lease import CdcLease, CdcLeaseConflict

        ts = now if now is not None else time.time()
        with self._lock:
            prior_generation = 0
            owner_ck = self._by_resource.get(resource)
            if owner_ck and owner_ck != cursor_key:
                other = self._leases.get(owner_ck)
                if other:
                    other_lease = CdcLease.from_dict(other)
                    if other_lease.holder_id != holder_id and not other_lease.is_stale(now=ts):
                        raise CdcLeaseConflict(
                            f"CDC resource {resource!r} held by {other_lease.holder_id!r} "
                            f"(cursor_key={other_lease.cursor_key!r}); refuse concurrent consumer",
                            holder_id=other_lease.holder_id,
                            resource=resource,
                            cursor_key=other_lease.cursor_key,
                        )
                    prior_generation = max(prior_generation, int(other_lease.generation or 0))
                    self._last_generation_by_cursor[owner_ck] = max(
                        self._last_generation_by_cursor.get(owner_ck, 0),
                        int(other_lease.generation or 0),
                    )
                    self._last_generation_by_resource[resource] = max(
                        self._last_generation_by_resource.get(resource, 0),
                        int(other_lease.generation or 0),
                    )
                    self._leases.pop(owner_ck, None)
                self._by_resource.pop(resource, None)

            existing = self._leases.get(cursor_key)
            generation_floor = max(
                self._last_generation_by_cursor.get(cursor_key, 0),
                self._last_generation_by_resource.get(resource, 0),
                prior_generation,
                int(existing.get("generation") or 0) if existing else 0,
            )
            acquired_at = ts
            if existing:
                ex = CdcLease.from_dict(existing)
                if ex.holder_id != holder_id and not ex.is_stale(now=ts):
                    raise CdcLeaseConflict(
                        f"CDC cursor {cursor_key!r} held by {ex.holder_id!r}; "
                        f"refuse concurrent consumer",
                        holder_id=ex.holder_id,
                        resource=ex.resource or resource,
                        cursor_key=cursor_key,
                    )
                if ex.holder_id == holder_id and int(ex.generation or 1) > int(min_generation):
                    generation = int(ex.generation or 1)
                    acquired_at = float(ex.acquired_at or ts)
                else:
                    generation = max(generation_floor, int(min_generation)) + 1
            else:
                generation = max(generation_floor, int(min_generation)) + 1
            _warn_generation_seed(
                cursor_key=cursor_key,
                resource=resource,
                store_generation=generation_floor,
                min_generation=int(min_generation),
                backend=self.name,
            )

            lease = {
                "cursor_key": cursor_key,
                "resource": resource,
                "holder_id": holder_id,
                "acquired_at": acquired_at,
                "heartbeat_at": ts,
                "ttl_sec": float(ttl_sec),
                "generation": generation,
                "meta": dict(meta or {}),
            }
            self._leases[cursor_key] = lease
            self._by_resource[resource] = cursor_key
            self._last_generation_by_cursor[cursor_key] = max(
                self._last_generation_by_cursor.get(cursor_key, 0), generation
            )
            self._last_generation_by_resource[resource] = max(
                self._last_generation_by_resource.get(resource, 0), generation
            )
            # Opportunistic GC
            for k, raw in list(self._leases.items()):
                if k == cursor_key:
                    continue
                try:
                    if CdcLease.from_dict(raw).is_stale(now=ts):
                        res = str(raw.get("resource") or "")
                        generation = int(raw.get("generation") or 0)
                        self._last_generation_by_cursor[k] = max(
                            self._last_generation_by_cursor.get(k, 0), generation
                        )
                        self._last_generation_by_resource[res] = max(
                            self._last_generation_by_resource.get(res, 0), generation
                        )
                        self._leases.pop(k, None)
                        if self._by_resource.get(res) == k:
                            self._by_resource.pop(res, None)
                except Exception:
                    self._leases.pop(k, None)
            return dict(lease)

    def renew(
        self,
        *,
        cursor_key: str,
        holder_id: str,
        generation: int,
        now: float | None = None,
    ) -> dict[str, Any] | None:
        ts = now if now is not None else time.time()
        with self._lock:
            raw = self._leases.get(cursor_key)
            if not raw:
                return None
            if raw.get("holder_id") != holder_id:
                return None
            if int(raw.get("generation") or 0) != int(generation):
                return None
            raw = dict(raw)
            raw["heartbeat_at"] = ts
            self._leases[cursor_key] = raw
            return dict(raw)

    def release(
        self,
        *,
        cursor_key: str,
        holder_id: str,
        generation: int,
    ) -> bool:
        with self._lock:
            raw = self._leases.get(cursor_key)
            if not raw:
                return False
            if raw.get("holder_id") != holder_id:
                return False
            if int(raw.get("generation") or 0) != int(generation):
                return False
            res = str(raw.get("resource") or "")
            generation = int(raw.get("generation") or 0)
            self._last_generation_by_cursor[cursor_key] = max(
                self._last_generation_by_cursor.get(cursor_key, 0), generation
            )
            self._last_generation_by_resource[res] = max(
                self._last_generation_by_resource.get(res, 0), generation
            )
            self._leases.pop(cursor_key, None)
            if self._by_resource.get(res) == cursor_key:
                self._by_resource.pop(res, None)
            return True

    def get(self, cursor_key: str) -> dict[str, Any] | None:
        with self._lock:
            raw = self._leases.get(cursor_key)
            return dict(raw) if raw else None

    def list_leases(self) -> list[dict[str, Any]]:
        with self._lock:
            return _sorted_leases(self._leases.values())

    def debug_set_heartbeat(self, cursor_key: str, heartbeat_at: float) -> None:
        with self._lock:
            raw = self._leases.get(cursor_key)
            if raw:
                raw["heartbeat_at"] = float(heartbeat_at)


class FileLeaseStore(LeaseStore):
    """Single-host durable store with fcntl flock (legacy / fallback)."""

    name = "file"

    def __init__(self, path: str | None = None, data_dir: str | None = None) -> None:
        default_dir = os.path.join(os.path.dirname(__file__), "..", "data")
        if path is None and data_dir is None:
            from services.platform_config import data_dir as platform_data_dir

            default_dir = str(platform_data_dir())
        self.data_dir = data_dir or default_dir
        self.path = path or os.path.join(self.data_dir, "cdc_leases.json")
        self._thread = threading.RLock()

    def _load(self) -> dict[str, Any]:
        if not os.path.isfile(self.path):
            return {"leases": {}, "generations": {"cursor": {}, "resource": {}}}
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return {"leases": {}}
            leases = data.get("leases")
            if not isinstance(leases, dict):
                leases = {}
            generations = data.get("generations")
            if not isinstance(generations, dict):
                generations = {}
            cursor_generations = generations.get("cursor")
            resource_generations = generations.get("resource")
            return {
                "leases": leases,
                "generations": {
                    "cursor": cursor_generations
                    if isinstance(cursor_generations, dict)
                    else {},
                    "resource": resource_generations
                    if isinstance(resource_generations, dict)
                    else {},
                },
            }
        except Exception:
            return {"leases": {}, "generations": {"cursor": {}, "resource": {}}}

    def _save(self, data: dict[str, Any]) -> None:
        from services.atomic_file import write_json_atomic

        os.makedirs(self.data_dir, exist_ok=True)
        write_json_atomic(Path(self.path), data)

    def _lock_cm(self):
        from contextlib import contextmanager

        @contextmanager
        def _cm():
            os.makedirs(self.data_dir, exist_ok=True)
            lock_path = self.path + ".lock"
            lock_f = open(lock_path, "a+", encoding="utf-8")
            locked = False
            try:
                try:
                    import fcntl

                    fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
                    locked = True
                except Exception:
                    locked = False
                with self._thread:
                    yield
            finally:
                if locked:
                    try:
                        import fcntl

                        fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
                    except Exception as exc:
                        logging.getLogger(__name__).debug("Exception suppressed: %s", exc, exc_info=exc)
                try:
                    lock_f.close()
                except Exception as exc:
                    logging.getLogger(__name__).debug("Exception suppressed: %s", exc, exc_info=exc)

        return _cm()

    def acquire(
        self,
        *,
        cursor_key: str,
        resource: str,
        holder_id: str,
        ttl_sec: float,
        meta: dict[str, Any],
        now: float | None = None,
        min_generation: int = 0,
    ) -> dict[str, Any]:
        from services.cdc_lease import CdcLease, CdcLeaseConflict

        ts = now if now is not None else time.time()
        with self._lock_cm():
            data = self._load()
            leases: dict[str, Any] = dict(data.get("leases") or {})
            generations = data.get("generations") or {}
            cursor_generations = dict(generations.get("cursor") or {})
            resource_generations = dict(generations.get("resource") or {})
            prior_generation = 0

            for other_key, raw in list(leases.items()):
                other = CdcLease.from_dict(raw if isinstance(raw, dict) else {})
                if other.resource != resource:
                    continue
                if other.holder_id == holder_id:
                    continue
                if other.is_stale(now=ts):
                    prior_generation = max(prior_generation, int(other.generation or 0))
                    cursor_generations[other_key] = max(
                        int(cursor_generations.get(other_key) or 0),
                        int(other.generation or 0),
                    )
                    resource_generations[resource] = max(
                        int(resource_generations.get(resource) or 0),
                        int(other.generation or 0),
                    )
                    leases.pop(other_key, None)
                    continue
                raise CdcLeaseConflict(
                    f"CDC resource {resource!r} held by {other.holder_id!r} "
                    f"(cursor_key={other.cursor_key!r}); refuse concurrent consumer",
                    holder_id=other.holder_id,
                    resource=resource,
                    cursor_key=other.cursor_key,
                )

            existing_raw = leases.get(cursor_key)
            generation_floor = max(
                int(cursor_generations.get(cursor_key) or 0),
                int(resource_generations.get(resource) or 0),
                prior_generation,
                int(existing_raw.get("generation") or 0)
                if isinstance(existing_raw, dict)
                else 0,
            )
            acquired_at = ts
            if existing_raw:
                existing = CdcLease.from_dict(
                    existing_raw if isinstance(existing_raw, dict) else {}
                )
                if existing.holder_id != holder_id and not existing.is_stale(now=ts):
                    raise CdcLeaseConflict(
                        f"CDC cursor {cursor_key!r} held by {existing.holder_id!r}; "
                        f"refuse concurrent consumer",
                        holder_id=existing.holder_id,
                        resource=existing.resource or resource,
                        cursor_key=cursor_key,
                    )
                if existing.holder_id == holder_id and int(existing.generation or 1) > int(min_generation):
                    generation = int(existing.generation or 1)
                    acquired_at = float(existing.acquired_at or ts)
                else:
                    generation = max(generation_floor, int(min_generation)) + 1
            else:
                generation = max(generation_floor, int(min_generation)) + 1
            _warn_generation_seed(
                cursor_key=cursor_key,
                resource=resource,
                store_generation=generation_floor,
                min_generation=int(min_generation),
                backend=self.name,
            )

            lease = {
                "cursor_key": cursor_key,
                "resource": resource,
                "holder_id": holder_id,
                "acquired_at": acquired_at,
                "heartbeat_at": ts,
                "ttl_sec": float(ttl_sec),
                "generation": generation,
                "meta": dict(meta or {}),
            }
            leases[cursor_key] = lease
            cursor_generations[cursor_key] = max(
                int(cursor_generations.get(cursor_key) or 0), generation
            )
            resource_generations[resource] = max(
                int(resource_generations.get(resource) or 0), generation
            )
            for k, raw in list(leases.items()):
                if k == cursor_key:
                    continue
                try:
                    if CdcLease.from_dict(raw).is_stale(now=ts):
                        stale = CdcLease.from_dict(raw)
                        cursor_generations[k] = max(
                            int(cursor_generations.get(k) or 0),
                            int(stale.generation or 0),
                        )
                        resource_generations[stale.resource] = max(
                            int(resource_generations.get(stale.resource) or 0),
                            int(stale.generation or 0),
                        )
                        leases.pop(k, None)
                except Exception:
                    leases.pop(k, None)
            self._save(
                {
                    "leases": leases,
                    "generations": {
                        "cursor": cursor_generations,
                        "resource": resource_generations,
                    },
                }
            )
            return dict(lease)

    def renew(
        self,
        *,
        cursor_key: str,
        holder_id: str,
        generation: int,
        now: float | None = None,
    ) -> dict[str, Any] | None:
        ts = now if now is not None else time.time()
        with self._lock_cm():
            data = self._load()
            leases = dict(data.get("leases") or {})
            raw = leases.get(cursor_key)
            if not raw:
                return None
            if raw.get("holder_id") != holder_id:
                return None
            if int(raw.get("generation") or 0) != int(generation):
                return None
            raw = dict(raw)
            raw["heartbeat_at"] = ts
            leases[cursor_key] = raw
            data["leases"] = leases
            self._save(data)
            return dict(raw)

    def release(
        self,
        *,
        cursor_key: str,
        holder_id: str,
        generation: int,
    ) -> bool:
        with self._lock_cm():
            data = self._load()
            leases = dict(data.get("leases") or {})
            raw = leases.get(cursor_key)
            if not raw:
                return False
            if raw.get("holder_id") != holder_id:
                return False
            if int(raw.get("generation") or 0) != int(generation):
                return False
            leases.pop(cursor_key, None)
            generations = data.get("generations") or {}
            cursor_generations = dict(generations.get("cursor") or {})
            resource_generations = dict(generations.get("resource") or {})
            resource = str(raw.get("resource") or "")
            lease_generation = int(raw.get("generation") or 0)
            cursor_generations[cursor_key] = max(
                int(cursor_generations.get(cursor_key) or 0), lease_generation
            )
            resource_generations[resource] = max(
                int(resource_generations.get(resource) or 0), lease_generation
            )
            self._save(
                {
                    "leases": leases,
                    "generations": {
                        "cursor": cursor_generations,
                        "resource": resource_generations,
                    },
                }
            )
            return True

    def get(self, cursor_key: str) -> dict[str, Any] | None:
        with self._lock_cm():
            raw = (self._load().get("leases") or {}).get(cursor_key)
            return dict(raw) if isinstance(raw, dict) else None

    def list_leases(self) -> list[dict[str, Any]]:
        with self._lock_cm():
            return _sorted_leases((self._load().get("leases") or {}).values())

    def debug_set_heartbeat(self, cursor_key: str, heartbeat_at: float) -> None:
        with self._lock_cm():
            data = self._load()
            leases = dict(data.get("leases") or {})
            raw = leases.get(cursor_key)
            if not raw:
                return
            raw = dict(raw)
            raw["heartbeat_at"] = float(heartbeat_at)
            leases[cursor_key] = raw
            data["leases"] = leases
            self._save(data)


class RedisLeaseStore(LeaseStore):
    """Multi-node lease authority via Redis + Lua (atomic acquire/renew/release)."""

    name = "redis"

    def __init__(self, url: str, *, key_prefix: str = "") -> None:
        self.url = (url or "").strip()
        if not self.url:
            raise LeaseStoreError("Redis lease URL is empty")
        self._prefix = (key_prefix or "").strip()
        self._client = None
        self._acquire_sha: str | None = None
        self._renew_sha: str | None = None
        self._release_sha: str | None = None

    def _connect(self):
        if self._client is not None:
            return self._client
        try:
            import redis
        except ImportError as exc:
            raise LeaseStoreError(
                "redis package required for DATAFLOW_CDC_LEASE_BACKEND=redis"
            ) from exc
        try:
            client = redis.from_url(
                self.url,
                socket_connect_timeout=2.0,
                socket_timeout=3.0,
                decode_responses=True,
            )
            client.ping()
        except Exception as exc:
            raise LeaseStoreError(f"CDC Redis lease store unreachable: {exc}") from exc
        self._client = client
        return client

    def _lease_key(self, cursor_key: str) -> str:
        return f"{self._prefix}{LEASE_KEY_PREFIX}{cursor_key}"

    def _res_key(self, resource: str) -> str:
        return f"{self._prefix}{RESOURCE_KEY_PREFIX}{resource}"

    def _cursor_generation_key(self, cursor_key: str) -> str:
        return f"{self._prefix}{GENERATION_KEY_PREFIX}ck:{cursor_key}"

    def _resource_generation_key(self, resource: str) -> str:
        return f"{self._prefix}{GENERATION_KEY_PREFIX}res:{resource}"

    def _eval(self, script: str, keys: list[str], args: list[Any]) -> Any:
        client = self._connect()
        try:
            return client.eval(script, len(keys), *keys, *args)
        except Exception as exc:
            self._client = None
            raise LeaseStoreError(f"CDC Redis lease script failed: {exc}") from exc

    def acquire(
        self,
        *,
        cursor_key: str,
        resource: str,
        holder_id: str,
        ttl_sec: float,
        meta: dict[str, Any],
        now: float | None = None,
        min_generation: int = 0,
    ) -> dict[str, Any]:
        from services.cdc_lease import CdcLeaseConflict

        ts = now if now is not None else time.time()
        gc_ttl = max(30, int(float(ttl_sec) * 3))
        result = self._eval(
            _REDIS_ACQUIRE_SCRIPT,
            [
                self._lease_key(cursor_key),
                self._res_key(resource),
                self._cursor_generation_key(cursor_key),
                self._resource_generation_key(resource),
            ],
            [
                ts,
                float(ttl_sec),
                holder_id,
                cursor_key,
                resource,
                json.dumps(meta or {}),
                1.0,
                gc_ttl,
                int(min_generation),
            ],
        )
        if not isinstance(result, (list, tuple)) or len(result) < 1:
            raise LeaseStoreError(f"unexpected Redis acquire result: {result!r}")
        # redis-py may return list of [k,v,k,v...] for flat dict returns from Lua tables
        parsed = _lua_table_to_dict(result)
        err = str(parsed.get("err") or "")
        status = str(parsed.get("status") or "")
        if err == "conflict":
            raise CdcLeaseConflict(
                f"CDC resource {resource!r} held by {parsed.get('holder')!r} "
                f"(cursor_key={parsed.get('cursor')!r}); refuse concurrent consumer",
                holder_id=str(parsed.get("holder") or ""),
                resource=str(parsed.get("resource") or resource),
                cursor_key=str(parsed.get("cursor") or cursor_key),
            )
        # Success must use status=ok — Redis treats Lua tables with an ``err``
        # field as ResponseError (so err="ok" previously raised "ok").
        if status != "ok" and err != "ok":
            raise LeaseStoreError(f"Redis acquire failed: {parsed}")
        _warn_generation_seed(
            cursor_key=cursor_key,
            resource=resource,
            store_generation=int(parsed.get("floor") or 0),
            min_generation=int(min_generation),
            backend=self.name,
        )
        payload = parsed.get("payload")
        if isinstance(payload, str):
            return json.loads(payload)
        if isinstance(payload, dict):
            return payload
        raise LeaseStoreError("Redis acquire missing payload")

    def renew(
        self,
        *,
        cursor_key: str,
        holder_id: str,
        generation: int,
        now: float | None = None,
    ) -> dict[str, Any] | None:
        ts = now if now is not None else time.time()
        existing = self.get(cursor_key)
        res = str((existing or {}).get("resource") or "")
        gc_ttl = max(30, int(float((existing or {}).get("ttl_sec") or 120) * 3))
        res_key = self._res_key(res) if res else self._res_key("__none__")
        result = self._eval(
            _REDIS_RENEW_SCRIPT,
            [self._lease_key(cursor_key), res_key],
            [ts, holder_id, int(generation), gc_ttl],
        )
        parsed = _lua_table_to_dict(result)
        status = str(parsed.get("status") or parsed.get("err") or "")
        if status != "ok":
            return None
        payload = parsed.get("payload")
        if isinstance(payload, str):
            return json.loads(payload)
        if isinstance(payload, dict):
            return payload
        return None

    def release(
        self,
        *,
        cursor_key: str,
        holder_id: str,
        generation: int,
    ) -> bool:
        existing = self.get(cursor_key)
        res = str((existing or {}).get("resource") or "")
        res_key = self._res_key(res) if res else self._res_key("__none__")
        result = self._eval(
            _REDIS_RELEASE_SCRIPT,
            [
                self._lease_key(cursor_key),
                res_key,
                self._cursor_generation_key(cursor_key),
                self._resource_generation_key(res),
            ],
            [holder_id, int(generation)],
        )
        parsed = _lua_table_to_dict(result)
        return str(parsed.get("status") or parsed.get("err") or "") == "ok"

    def get(self, cursor_key: str) -> dict[str, Any] | None:
        try:
            client = self._connect()
            raw = client.get(self._lease_key(cursor_key))
        except LeaseStoreError:
            raise
        except Exception as exc:
            self._client = None
            raise LeaseStoreError(f"CDC Redis get failed: {exc}") from exc
        if not raw:
            return None
        try:
            data = json.loads(raw)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def list_leases(self) -> list[dict[str, Any]]:
        client = self._connect()
        pattern = f"{self._prefix}{LEASE_KEY_PREFIX}*"
        out: list[dict[str, Any]] = []
        try:
            # SCAN, not KEYS: enumeration must not block a shared Redis.
            for key in client.scan_iter(match=pattern, count=200):
                raw = client.get(key)
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                if isinstance(data, dict):
                    out.append(data)
        except LeaseStoreError:
            raise
        except Exception as exc:
            self._client = None
            raise LeaseStoreError(f"CDC Redis lease scan failed: {exc}") from exc
        return _sorted_leases(out)

    def debug_set_heartbeat(self, cursor_key: str, heartbeat_at: float) -> None:
        client = self._connect()
        key = self._lease_key(cursor_key)
        raw = client.get(key)
        if not raw:
            return
        data = json.loads(raw)
        data["heartbeat_at"] = float(heartbeat_at)
        ttl = client.ttl(key)
        payload = json.dumps(data)
        if ttl and ttl > 0:
            client.set(key, payload, ex=ttl)
        else:
            client.set(key, payload)


def _lua_table_to_dict(result: Any) -> dict[str, Any]:
    """Normalize redis-py Lua table returns (dict or flat list)."""
    if isinstance(result, dict):
        return result
    if isinstance(result, (list, tuple)):
        if len(result) == 1 and isinstance(result[0], dict):
            return result[0]
        out: dict[str, Any] = {}
        i = 0
        while i + 1 < len(result):
            out[str(result[i])] = result[i + 1]
            i += 2
        if out:
            return out
    return {"err": "unknown", "raw": result}


_STORE: LeaseStore | None = None
_STORE_LOCK = threading.RLock()
_SINGLE_HOST_WARNING_EMITTED = False


def redis_url_from_env() -> str:
    return (
        getenv_brand("CDC_LEASE_REDIS_URL")
        or getenv_brand("REDIS_URL")
        or ""
    ).strip()


def configured_backend_name() -> str:
    raw = (getenv_brand("CDC_LEASE_BACKEND") or "auto").strip().lower()
    if raw in {"file", "redis", "memory"}:
        return raw
    return "auto"


def resolve_backend_name() -> str:
    name = configured_backend_name()
    if name != "auto":
        return name
    if redis_url_from_env():
        return "redis"
    return "file"


def build_store(backend: str | None = None) -> LeaseStore:
    name = (backend or resolve_backend_name()).strip().lower()
    if name in {"file", "memory"} and (
        (getenv_brand("MULTI_REPLICA", "") or "").strip().lower()
        in {"1", "true", "yes"}
    ):
        raise LeaseStoreError(
            f"CDC lease backend={name} cannot coordinate multiple instances "
            "(DATAFLOW_MULTI_REPLICA is set); set "
            "DATAFLOW_CDC_LEASE_REDIS_URL (or DATAFLOW_REDIS_URL) or "
            "DATAFLOW_CDC_LEASE_BACKEND=redis"
        )
    if name == "memory":
        return MemoryLeaseStore()
    if name == "redis":
        url = redis_url_from_env()
        if not url:
            raise LeaseStoreError(
                "DATAFLOW_CDC_LEASE_BACKEND=redis requires "
                "DATAFLOW_CDC_LEASE_REDIS_URL or DATAFLOW_REDIS_URL"
            )
        prefix = (getenv_brand("CDC_LEASE_REDIS_PREFIX") or "").strip()
        return RedisLeaseStore(url, key_prefix=prefix)
    if name == "file":
        path = getenv_brand("CDC_LEASE_PATH", "").strip() or None
        data_dir = getenv_brand("CDC_LEASE_DIR", "").strip() or None
        return FileLeaseStore(path=path, data_dir=data_dir)
    raise LeaseStoreError(f"Unknown CDC lease backend: {name!r}")


def get_store() -> LeaseStore:
    global _STORE, _SINGLE_HOST_WARNING_EMITTED
    with _STORE_LOCK:
        if _STORE is None:
            _STORE = build_store()
            logger.info("CDC lease backend=%s", _STORE.name)
            if _STORE.name in {"file", "memory"} and not _SINGLE_HOST_WARNING_EMITTED:
                logger.warning(
                    "CDC lease backend=%s is single-host; "
                    "set DATAFLOW_CDC_LEASE_REDIS_URL for multi-instance",
                    _STORE.name,
                )
                _SINGLE_HOST_WARNING_EMITTED = True
        return _STORE


def reset_store(store: LeaseStore | None = None) -> LeaseStore:
    """Replace the process-wide store (tests / reconfigure)."""
    global _STORE
    with _STORE_LOCK:
        _STORE = store if store is not None else build_store()
        return _STORE


def configure_store(*, backend: str | None = None, **kwargs: Any) -> LeaseStore:
    """Explicit store configuration for tests and operators."""
    if backend == "memory":
        store: LeaseStore = MemoryLeaseStore()
    elif backend == "file":
        store = FileLeaseStore(
            path=kwargs.get("path"),
            data_dir=kwargs.get("data_dir"),
        )
    elif backend == "redis":
        url = str(kwargs.get("url") or redis_url_from_env())
        store = RedisLeaseStore(url, key_prefix=str(kwargs.get("prefix") or ""))
    else:
        store = build_store(backend)
    return reset_store(store)
