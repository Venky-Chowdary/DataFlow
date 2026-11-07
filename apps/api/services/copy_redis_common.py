"""Shared Redis identity-COPY helpers.

Dest COUNT is ``destination_row_count`` / prefix SCAN cardinality —
never ``DBSIZE``, never ``INFO keyspace``, never COPY ack. Same
host+port+db+prefix declines. Cross-endpoint COPY declines (server-side
``COPY`` cannot leave the instance). Occupancy is counted **before**
delete. Desktop-lab Redis on :6379 is not a customer-tenant
PRODUCTION_SKU.
"""

from __future__ import annotations

from typing import Any

from connectors.redis_reader import (
    _redis_client,
    resolve_key_pattern,
    scan_all_keys,
)
from services.copy_fast_path import FastPathResult, FastPathUnavailable

_REDIS_FAMILY = frozenset({
    "redis",
    "redis_cloud",
    "redislabs",
    "azure_cache_redis",
    "azure_redis",
    "elasticache_redis",
    "keydb",
    "valkey",
})

# Native Redis types ``COPY`` can move without decoding. Module types that
# are not this set decline so the row path keeps quarantine.
_REDIS_COPY_SAFE_TYPES = frozenset({
    "string",
    "hash",
    "list",
    "set",
    "zset",
    "stream",
    "rejson-rl",
})

_COPY_BATCH = 256


def redis_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _REDIS_FAMILY:
        return "redis"
    return n


def redis_key_type_is_copy_safe(ktype: str) -> bool:
    return (ktype or "").strip().lower() in _REDIS_COPY_SAFE_TYPES


def redis_type_is_copy_safe(declared_or_type: str) -> bool:
    """JSON-field DDL is opaque to ``COPY``; Redis TYPE names must be native."""
    raw = (declared_or_type or "").strip().lower()
    if not raw:
        return True
    if redis_key_type_is_copy_safe(raw):
        return True
    # Mapping stamps (long/string/json) are document fields, not Redis TYPEs.
    # COPY does not rewrite JSON, so those stamps are COPY-safe.
    if raw in {
        "long",
        "integer",
        "int",
        "string",
        "text",
        "json",
        "boolean",
        "bool",
        "number",
        "double",
        "float",
    }:
        return True
    return False


def redis_prefix(table: str) -> str:
    prefix = (table or "").strip()
    if not prefix:
        raise FastPathUnavailable("Redis key prefix required")
    if any(ch in prefix for ch in "*?[]"):
        raise FastPathUnavailable("Redis COPY refuses glob characters in the prefix")
    return prefix


def redis_db(cfg: dict[str, Any]) -> int:
    raw = cfg.get("database")
    if str(raw or "").isdigit():
        return int(raw)
    cs = str(cfg.get("connection_string") or "").strip()
    if cs:
        from connectors.url_authority import parse_url_authority

        parsed = parse_url_authority(cs)
        path = (parsed.path or "").lstrip("/")
        first = path.split("/", 1)[0] if path else ""
        if first.isdigit():
            return int(first)
    return 0


def redis_endpoint_key(cfg: dict[str, Any]) -> str:
    host = str(cfg.get("host") or "").strip().lower()
    port = int(cfg.get("port") or 6379)
    cs = str(cfg.get("connection_string") or "").strip()
    if cs:
        from connectors.url_authority import parse_url_authority

        parsed = parse_url_authority(cs)
        if parsed.host:
            host = str(parsed.host).strip().lower()
        if parsed.port:
            port = int(parsed.port)
    host = host.replace("localhost", "127.0.0.1") or "127.0.0.1"
    return f"{host}:{port}"


def redis_object_id(cfg: dict[str, Any], prefix: str) -> tuple[str, int, str]:
    return (redis_endpoint_key(cfg), redis_db(cfg), redis_prefix(prefix))


def redis_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def redis_dest_count(cfg: dict[str, Any], prefix: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "redis",
        {**cfg, "type": "redis", "format": "redis"},
        schema="",
        table_name=redis_prefix(prefix),
    )
    if n is None:
        raise ValueError(f"Redis dest COUNT unmeasured for {prefix}")
    return int(n)


def redis_list_keys(cfg: dict[str, Any], prefix: str) -> list[str]:
    client = _redis_client(cfg)
    try:
        return scan_all_keys(client, resolve_key_pattern(redis_prefix(prefix)))
    except Exception as exc:
        raise FastPathUnavailable(f"Redis SCAN failed: {exc}") from exc
    finally:
        _close_quiet(client)


def redis_delete_keys(cfg: dict[str, Any], keys: list[str]) -> None:
    if not keys:
        return
    client = _redis_client(cfg)
    try:
        for i in range(0, len(keys), _COPY_BATCH):
            chunk = keys[i : i + _COPY_BATCH]
            if chunk:
                client.delete(*chunk)
    finally:
        _close_quiet(client)


def redis_remap_keys(
    src_keys: list[str], src_prefix: str, dest_prefix: str
) -> list[tuple[str, str]]:
    src = redis_prefix(src_prefix)
    dest = redis_prefix(dest_prefix)
    head = f"{src}:"
    dest_head = f"{dest}:"
    out: list[tuple[str, str]] = []
    for key in src_keys:
        if not key.startswith(head):
            raise FastPathUnavailable(
                f"source key {key!r} is not under prefix {src!r}"
            )
        suffix = key[len(head) :]
        if not suffix:
            raise FastPathUnavailable(f"source key {key!r} has empty identity")
        dest_key = f"{dest_head}{suffix}"
        if dest_key == key:
            raise FastPathUnavailable(
                "Redis COPY onto the same key stays on the row path"
            )
        out.append((key, dest_key))
    return out


def redis_copy_available(client: Any) -> bool:
    try:
        info = client.execute_command("COMMAND", "INFO", "COPY")
    except Exception:
        return False
    if not info:
        return False
    first = info[0] if isinstance(info, (list, tuple)) else info
    return bool(first)


def redis_copy_pairs(
    src_client: Any,
    pairs: list[tuple[str, str]],
    *,
    dest_db: int | None = None,
) -> None:
    """Server-side ``COPY``. Never GET/SET/DUMP/RESTORE the payload."""
    for i in range(0, len(pairs), _COPY_BATCH):
        chunk = pairs[i : i + _COPY_BATCH]
        pipe = src_client.pipeline(transaction=False)
        for src_key, dest_key in chunk:
            if dest_db is None:
                pipe.copy(src_key, dest_key)
            else:
                pipe.copy(src_key, dest_key, destination_db=dest_db)
        results = pipe.execute()
        for (src_key, dest_key), ok in zip(chunk, results):
            if not ok:
                raise ValueError(
                    f"Redis COPY returned false for {src_key!r} → {dest_key!r}"
                )


def redis_key_types(client: Any, keys: list[str]) -> dict[str, str]:
    types: dict[str, str] = {}
    for i in range(0, len(keys), _COPY_BATCH):
        chunk = keys[i : i + _COPY_BATCH]
        pipe = client.pipeline(transaction=False)
        for key in chunk:
            pipe.type(key)
        for key, raw in zip(chunk, pipe.execute()):
            if isinstance(raw, bytes):
                raw = raw.decode()
            types[key] = str(raw or "").strip().lower()
    return types


def skip_complete_redis(
    *,
    source_count: int,
    dest_count: int,
    extra_snapshot: dict[str, Any] | None = None,
) -> FastPathResult:
    proof = f"dest_count:{dest_count}"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "skip",
        "copy_partitions": 1,
        "partitions_skipped": 1,
        "partitions_loaded": 0,
        "shard_mode": "prefix",
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=source_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
    )


def _close_quiet(client: Any) -> None:
    try:
        client.close()
    except Exception:
        return
