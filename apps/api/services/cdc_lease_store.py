"""CDC lease persistence backends (file / memory / Redis).

Multi-node workers must share one lease authority. Redis is the distributed
backend; file remains single-host. When Redis is explicitly selected, acquire
is **fail-closed** on connectivity errors (never silent split-brain to file).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

LEASE_KEY_PREFIX = "df:cdc:lease:"
RESOURCE_KEY_PREFIX = "df:cdc:res:"

# Atomic acquire: resource cross-index + fencing generation + stale steal.
_REDIS_ACQUIRE_SCRIPT = """
local lease_key = KEYS[1]
local res_key = KEYS[2]
local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local holder = ARGV[3]
local cursor_key = ARGV[4]
local resource = ARGV[5]
local meta_json = ARGV[6]
local stale_floor = tonumber(ARGV[7])
local gc_ttl = tonumber(ARGV[8])

local function is_stale(lease)
  if not lease then return true end
  local hb = tonumber(lease["heartbeat_at"] or 0)
  local lease_ttl = tonumber(lease["ttl_sec"] or ttl)
  if lease_ttl < stale_floor then lease_ttl = stale_floor end
  return (now - hb) > lease_ttl
end

local owner_ck = redis.call("GET", res_key)
local prior_generation = 0
if owner_ck and owner_ck ~= cursor_key then
  local other_key = "df:cdc:lease:" .. owner_ck
  local other_raw = redis.call("GET", other_key)
  if other_raw then
    local other = cjson.decode(other_raw)
    if not is_stale(other) and other["holder_id"] ~= holder then
      return {err="conflict", holder=other["holder_id"], cursor=other["cursor_key"] or owner_ck, resource=other["resource"] or resource}
    end
    prior_generation = tonumber(other["generation"] or 0)
    redis.call("DEL", other_key)
  end
  redis.call("DEL", res_key)
end

local existing_raw = redis.call("GET", lease_key)
local generation = 1
local acquired_at = now
if existing_raw then
  local existing = cjson.decode(existing_raw)
  if existing["holder_id"] ~= holder and not is_stale(existing) then
    return {err="conflict", holder=existing["holder_id"], cursor=existing["cursor_key"] or cursor_key, resource=existing["resource"] or resource}
  end
  if existing["holder_id"] == holder then
    generation = tonumber(existing["generation"] or 1)
    acquired_at = tonumber(existing["acquired_at"] or now)
  else
    local eg = tonumber(existing["generation"] or 0)
    if prior_generation > eg then eg = prior_generation end
    generation = eg + 1
  end
elseif prior_generation > 0 then
  generation = prior_generation + 1
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
redis.call("SET", lease_key, payload, "EX", gc_ttl)
redis.call("SET", res_key, cursor_key, "EX", gc_ttl)
return {err="ok", payload=payload}
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
  return {err="missing"}
end
local lease = cjson.decode(raw)
if lease["holder_id"] ~= holder then
  return {err="holder"}
end
if tonumber(lease["generation"] or 0) ~= generation then
  return {err="fence"}
end
lease["heartbeat_at"] = now
local payload = cjson.encode(lease)
redis.call("SET", lease_key, payload, "EX", gc_ttl)
if res_key and res_key ~= "" then
  redis.call("SET", res_key, lease["cursor_key"], "EX", gc_ttl)
end
return {err="ok", payload=payload}
"""

_REDIS_RELEASE_SCRIPT = """
local lease_key = KEYS[1]
local res_key = KEYS[2]
local holder = ARGV[1]
local generation = tonumber(ARGV[2])

local raw = redis.call("GET", lease_key)
if not raw then
  return {err="missing"}
end
local lease = cjson.decode(raw)
if lease["holder_id"] ~= holder then
  return {err="holder"}
end
if tonumber(lease["generation"] or 0) ~= generation then
  return {err="fence"}
end
redis.call("DEL", lease_key)
local owner = redis.call("GET", res_key)
if owner == lease["cursor_key"] then
  redis.call("DEL", res_key)
end
return {err="ok"}
"""


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

    def clear(self) -> None:
        with self._lock:
            self._leases.clear()
            self._by_resource.clear()

    def acquire(
        self,
        *,
        cursor_key: str,
        resource: str,
        holder_id: str,
        ttl_sec: float,
        meta: dict[str, Any],
        now: float | None = None,
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
                    self._leases.pop(owner_ck, None)
                self._by_resource.pop(resource, None)

            existing = self._leases.get(cursor_key)
            generation = 1
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
                if ex.holder_id == holder_id:
                    generation = int(ex.generation or 1)
                    acquired_at = float(ex.acquired_at or ts)
                else:
                    generation = max(prior_generation, int(ex.generation or 0)) + 1
            elif prior_generation:
                generation = prior_generation + 1

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
            # Opportunistic GC
            for k, raw in list(self._leases.items()):
                if k == cursor_key:
                    continue
                try:
                    if CdcLease.from_dict(raw).is_stale(now=ts):
                        res = str(raw.get("resource") or "")
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
            self._leases.pop(cursor_key, None)
            if self._by_resource.get(res) == cursor_key:
                self._by_resource.pop(res, None)
            return True

    def get(self, cursor_key: str) -> dict[str, Any] | None:
        with self._lock:
            raw = self._leases.get(cursor_key)
            return dict(raw) if raw else None

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
        self.data_dir = data_dir or default_dir
        self.path = path or os.path.join(self.data_dir, "cdc_leases.json")
        self._thread = threading.RLock()

    def _load(self) -> dict[str, Any]:
        if not os.path.isfile(self.path):
            return {"leases": {}}
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return {"leases": {}}
            leases = data.get("leases")
            if not isinstance(leases, dict):
                leases = {}
            return {"leases": leases}
        except Exception:
            return {"leases": {}}

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
                    except Exception:
                        pass
                try:
                    lock_f.close()
                except Exception:
                    pass

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
    ) -> dict[str, Any]:
        from services.cdc_lease import CdcLease, CdcLeaseConflict

        ts = now if now is not None else time.time()
        with self._lock_cm():
            data = self._load()
            leases: dict[str, Any] = dict(data.get("leases") or {})
            prior_generation = 0

            for other_key, raw in list(leases.items()):
                other = CdcLease.from_dict(raw if isinstance(raw, dict) else {})
                if other.resource != resource:
                    continue
                if other.holder_id == holder_id:
                    continue
                if other.is_stale(now=ts):
                    prior_generation = max(prior_generation, int(other.generation or 0))
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
            generation = 1
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
                if existing.holder_id == holder_id:
                    generation = int(existing.generation or 1)
                    acquired_at = float(existing.acquired_at or ts)
                else:
                    generation = max(prior_generation, int(existing.generation or 0)) + 1
            elif prior_generation:
                generation = prior_generation + 1

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
            for k, raw in list(leases.items()):
                if k == cursor_key:
                    continue
                try:
                    if CdcLease.from_dict(raw).is_stale(now=ts):
                        leases.pop(k, None)
                except Exception:
                    leases.pop(k, None)
            self._save({"leases": leases})
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
            self._save({"leases": leases})
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
            self._save({"leases": leases})
            return True

    def get(self, cursor_key: str) -> dict[str, Any] | None:
        with self._lock_cm():
            raw = (self._load().get("leases") or {}).get(cursor_key)
            return dict(raw) if isinstance(raw, dict) else None

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
            self._save({"leases": leases})


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
    ) -> dict[str, Any]:
        from services.cdc_lease import CdcLeaseConflict

        ts = now if now is not None else time.time()
        gc_ttl = max(30, int(float(ttl_sec) * 3))
        result = self._eval(
            _REDIS_ACQUIRE_SCRIPT,
            [self._lease_key(cursor_key), self._res_key(resource)],
            [
                ts,
                float(ttl_sec),
                holder_id,
                cursor_key,
                resource,
                json.dumps(meta or {}),
                1.0,
                gc_ttl,
            ],
        )
        if not isinstance(result, (list, tuple)) or len(result) < 1:
            raise LeaseStoreError(f"unexpected Redis acquire result: {result!r}")
        # redis-py may return list of [k,v,k,v...] for flat dict returns from Lua tables
        parsed = _lua_table_to_dict(result)
        err = str(parsed.get("err") or "")
        if err == "conflict":
            raise CdcLeaseConflict(
                f"CDC resource {resource!r} held by {parsed.get('holder')!r} "
                f"(cursor_key={parsed.get('cursor')!r}); refuse concurrent consumer",
                holder_id=str(parsed.get("holder") or ""),
                resource=str(parsed.get("resource") or resource),
                cursor_key=str(parsed.get("cursor") or cursor_key),
            )
        if err != "ok":
            raise LeaseStoreError(f"Redis acquire failed: {parsed}")
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
        if str(parsed.get("err")) != "ok":
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
            [self._lease_key(cursor_key), res_key],
            [holder_id, int(generation)],
        )
        parsed = _lua_table_to_dict(result)
        return str(parsed.get("err")) == "ok"

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


def redis_url_from_env() -> str:
    return (
        os.getenv("DATAFLOW_CDC_LEASE_REDIS_URL")
        or os.getenv("DATAFLOW_REDIS_URL")
        or ""
    ).strip()


def configured_backend_name() -> str:
    raw = (os.getenv("DATAFLOW_CDC_LEASE_BACKEND") or "auto").strip().lower()
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
    if name == "memory":
        return MemoryLeaseStore()
    if name == "redis":
        url = redis_url_from_env()
        if not url:
            raise LeaseStoreError(
                "DATAFLOW_CDC_LEASE_BACKEND=redis requires "
                "DATAFLOW_CDC_LEASE_REDIS_URL or DATAFLOW_REDIS_URL"
            )
        prefix = (os.getenv("DATAFLOW_CDC_LEASE_REDIS_PREFIX") or "").strip()
        return RedisLeaseStore(url, key_prefix=prefix)
    if name == "file":
        path = os.getenv("DATAFLOW_CDC_LEASE_PATH", "").strip() or None
        data_dir = os.getenv("DATAFLOW_CDC_LEASE_DIR", "").strip() or None
        return FileLeaseStore(path=path, data_dir=data_dir)
    raise LeaseStoreError(f"Unknown CDC lease backend: {name!r}")


def get_store() -> LeaseStore:
    global _STORE
    with _STORE_LOCK:
        if _STORE is None:
            _STORE = build_store()
            logger.info("CDC lease backend=%s", _STORE.name)
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
