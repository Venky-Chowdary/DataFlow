"""Redis key-value reader — incremental SCAN without materializing full keyspace."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from connectors.base import ReadBatch
from services.value_serializer import cell_to_string, json_default


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
    """Decode Redis bytes — binary payloads become a typed base64 envelope."""
    from services.value_serializer import json_default

    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            text = value.decode("utf-8")
            # Round-trip check — non-UTF8 or replacement means binary.
            if text.encode("utf-8") == value and "\ufffd" not in text:
                return text
        except UnicodeDecodeError:
            pass
        import base64

        return json.dumps(
            {"_df_redis_binary": True, "encoding": "base64", "data": base64.b64encode(value).decode("ascii")},
            default=json_default,
        )
    if isinstance(value, str):
        return value
    return json.dumps(value, default=json_default)


# Cap large Redis collections — overflow is marked, never silently truncated.
_REDIS_COLLECTION_CAP = 10_000


def _read_redis_value(client: Any, key: str, ktype: str) -> str:
    """Typed Redis value read — never WRONGTYPE on set/zset/stream; mark truncation."""
    if ktype == "hash":
        return json.dumps(
            {(_decode(f)): _decode(v) for f, v in client.hgetall(key).items()},
            default=json_default,
        )
    if ktype == "list":
        llen = int(client.llen(key) or 0)
        vals = [_decode(v) for v in client.lrange(key, 0, _REDIS_COLLECTION_CAP - 1)]
        if llen > _REDIS_COLLECTION_CAP:
            return json.dumps(
                {"_df_redis_list": vals, "_df_truncated": True, "_df_llen": llen},
                default=json_default,
            )
        return json.dumps(vals, default=json_default)
    if ktype == "set":
        members = list(client.smembers(key))
        truncated = len(members) > _REDIS_COLLECTION_CAP
        vals = [_decode(v) for v in members[:_REDIS_COLLECTION_CAP]]
        if truncated:
            return json.dumps(
                {"_df_redis_set": vals, "_df_truncated": True, "_df_scard": len(members)},
                default=json_default,
            )
        return json.dumps(vals, default=json_default)
    if ktype == "zset":
        pairs = client.zrange(key, 0, _REDIS_COLLECTION_CAP - 1, withscores=True)
        zcard = int(client.zcard(key) or 0)
        vals = [[_decode(m), float(s)] for m, s in pairs]
        if zcard > _REDIS_COLLECTION_CAP:
            return json.dumps(
                {"_df_redis_zset": vals, "_df_truncated": True, "_df_zcard": zcard},
                default=json_default,
            )
        return json.dumps(vals, default=json_default)
    if ktype == "stream":
        xlen = int(client.xlen(key) or 0)
        entries = client.xrange(key, count=_REDIS_COLLECTION_CAP)
        vals = [
            {"id": _decode(eid), "fields": {(_decode(f)): _decode(v) for f, v in fields.items()}}
            for eid, fields in entries
        ]
        if xlen > len(vals):
            return json.dumps(
                {"_df_redis_stream": vals, "_df_truncated": True, "_df_xlen": xlen},
                default=json_default,
            )
        return json.dumps(vals, default=json_default)
    # string / unknown — GET (or empty on miss).
    return _decode(client.get(key))


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
    from connectors.header_union import union_attribute_keys

    state = scan_state or RedisScanState()
    client = _redis_client(cfg)
    try:
        identity_headers = ["redis_key", "redis_value", "redis_type"]
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
                val = _read_redis_value(client, key, ktype)
                rows.append([key, val, ktype])
                if len(rows) >= limit:
                    break
            if state.cursor == 0:
                state.exhausted = True
                break

        # If every stored value is a JSON object, flatten keys BUT keep redis_key /
        # redis_type so upsert identity is never silently dropped.
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
            fieldnames: list[str] = []
            seen: set[str] = set()
            for obj in object_values:
                for key in obj.keys():
                    if key in {"redis_key", "redis_type", "redis_value"}:
                        continue
                    if key not in seen:
                        seen.add(key)
                        fieldnames.append(key)
            headers = union_attribute_keys(["redis_key", "redis_type"], fieldnames)
            flat_rows: list[list[str]] = []
            for row, obj in zip(rows, object_values):
                flat_rows.append(
                    [row[0], row[2]] + [cell_to_string(obj.get(field, "")) for field in fieldnames]
                )
            rows = flat_rows
        else:
            headers = identity_headers

        total = known_total_rows if known_total_rows is not None else state.keys_seen
        return ReadBatch(headers=headers, rows=rows, offset=state.keys_seen, total_rows=total), state
    finally:
        client.close()
