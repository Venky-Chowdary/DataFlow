"""Redis key-value reader — incremental SCAN without materializing full keyspace."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int = 0
    total_rows: int = 0


@dataclass
class RedisScanState:
    cursor: int = 0
    exhausted: bool = False
    keys_seen: int = 0


def _redis_client(cfg: dict[str, Any]):
    import redis

    if cfg.get("connection_string"):
        return redis.from_url(cfg["connection_string"], socket_timeout=30)
    return redis.Redis(
        host=cfg.get("host") or "localhost",
        port=int(cfg.get("port") or 6379),
        db=int(cfg["database"]) if str(cfg.get("database") or "0").isdigit() else 0,
        username=cfg.get("username") or None,
        password=cfg.get("password") or None,
        ssl=bool(cfg.get("ssl")),
        socket_timeout=30,
    )


def _decode(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def read_keys_batch(
    *,
    cfg: dict[str, Any],
    pattern: str = "*",
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
    scan_state: RedisScanState | None = None,
) -> tuple[ReadBatch, RedisScanState]:
    del offset
    state = scan_state or RedisScanState()
    client = _redis_client(cfg)
    try:
        headers = ["redis_key", "redis_value", "redis_type"]
        rows: list[list[str]] = []

        while len(rows) < limit and not state.exhausted:
            state.cursor, batch = client.scan(
                cursor=state.cursor,
                match=pattern or "*",
                count=max(limit, 100),
            )
            for raw_key in batch:
                key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
                state.keys_seen += 1
                ktype = _decode(client.type(key))
                if ktype == "hash":
                    val = json.dumps({(_decode(f)): _decode(v) for f, v in client.hgetall(key).items()})
                elif ktype == "list":
                    val = json.dumps([_decode(v) for v in client.lrange(key, 0, 50)])
                else:
                    val = _decode(client.get(key))
                rows.append([key, val, ktype])
                if len(rows) >= limit:
                    break
            if state.cursor == 0:
                state.exhausted = True
                break

        # If every stored value is a JSON object, flatten its keys so mappings
        # like id -> id / amount -> amount work against Redis-stored records.
        object_values: list[dict] = []
        for row in rows:
            try:
                parsed = json.loads(row[1])
            except Exception:
                parsed = None
            if not isinstance(parsed, dict):
                object_values = []
                break
            object_values.append(parsed)

        if object_values and len(object_values) == len(rows):
            fieldnames = []
            seen: set[str] = set()
            for obj in object_values:
                for key in obj.keys():
                    if key not in seen:
                        seen.add(key)
                        fieldnames.append(key)
            headers = fieldnames
            rows = [[str(obj.get(field, "")) for field in fieldnames] for obj in object_values]

        total = known_total_rows if known_total_rows is not None else state.keys_seen
        return ReadBatch(headers=headers, rows=rows, offset=state.keys_seen, total_rows=total), state
    finally:
        client.close()
