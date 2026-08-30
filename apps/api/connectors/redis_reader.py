"""Redis key-value reader — incremental SCAN without materializing full keyspace."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from services.value_serializer import cell_to_string, json_default, json_loads_exact

from connectors.base import ReadBatch


@dataclass
class RedisScanState:
    cursor: int = 0
    exhausted: bool = False
    keys_seen: int = 0
    # Keys returned by SCAN but not yet emitted (COUNT is a hint — never drop the tail).
    pending_keys: list[str] | None = None
    scan_complete: bool = False
    # Redis SCAN may return the same key more than once — dedupe across the scan.
    emitted_keys: set[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "cursor": self.cursor,
            "exhausted": self.exhausted,
            "keys_seen": self.keys_seen,
            "pending_keys": list(self.pending_keys or []),
            "scan_complete": self.scan_complete,
            "emitted_keys": list(self.emitted_keys or []),
        }

    @classmethod
    def from_any(cls, value: Any) -> "RedisScanState":
        if value is None:
            state = cls()
            state.pending_keys = []
            state.emitted_keys = set()
            return state
        if isinstance(value, cls):
            if value.pending_keys is None:
                value.pending_keys = []
            if value.emitted_keys is None:
                value.emitted_keys = set()
            elif not isinstance(value.emitted_keys, set):
                value.emitted_keys = set(value.emitted_keys)
            return value
        if isinstance(value, dict):
            return cls(
                cursor=int(value.get("cursor") or 0),
                exhausted=bool(value.get("exhausted")),
                keys_seen=int(value.get("keys_seen") or 0),
                pending_keys=list(value.get("pending_keys") or []),
                scan_complete=bool(value.get("scan_complete")),
                emitted_keys=set(value.get("emitted_keys") or []),
            )
        return cls()


def _redis_client(cfg: dict[str, Any]):
    import redis

    if cfg.get("connection_string"):
        from connectors.url_authority import parse_url_authority, rebuild_url

        raw = str(cfg["connection_string"]).strip()
        parsed = parse_url_authority(raw)
        if parsed.host and (cfg.get("username") or cfg.get("password")):
            raw = rebuild_url(
                parsed,
                user=str(cfg.get("username") or parsed.user),
                password=str(cfg.get("password") or parsed.password),
            )
        return redis.from_url(raw, socket_timeout=30)
    return redis.Redis(
        host=cfg.get("host") or "localhost",
        port=int(cfg.get("port") or 6379),
        db=int(cfg["database"]) if str(cfg.get("database") or "0").isdigit() else 0,
        username=cfg.get("username") or None,
        password=cfg.get("password") or None,
        ssl=bool(cfg.get("ssl")),
        socket_timeout=30,
    )


def redis_zset_score_carrier(score: Any) -> float:
    """Bind a Redis zset score through the write-path float carrier.

    Redis scores are IEEE doubles. ``float(text)`` invented Auto ``1.234``
    and collapsed ``2**53+1``. Native Python floats pass through (already
    IEEE). Locale money the write path stores still binds.
    """
    from connectors.sql_bind import coerce_float_wire

    try:
        bound = coerce_float_wire(score, ddl_type="FLOAT")
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Redis zset score cannot bind {score!r} — refuse invent"
        ) from exc
    if not isinstance(bound, float):
        raise ValueError(
            f"Redis zset score cannot bind {score!r} — refuse invent"
        )
    return bound


def redis_json_row(raw: Any) -> dict[str, Any]:
    """One Redis value as a sample / Gate-8 row.

    JSON objects keep ``json_loads_exact`` numbers. Invalid or non-object
    values stay under ``value`` — never invent an empty object.
    """
    parsed = load_redis_json_doc(raw)
    if isinstance(parsed, dict):
        return parsed
    if isinstance(raw, (bytes, bytearray)):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = _decode(raw)
    else:
        text = "" if raw is None else str(raw)
    if parsed is None:
        return {"value": text}
    return {"value": parsed}


def load_redis_json_doc(raw: Any) -> Any:
    """Parse a Redis JSON document. Numbers match ``json_loads_exact``.

    Invalid UTF-8, non-JSON, or non-text payloads return ``None`` — callers
    must not invent an empty object.
    """
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        if not isinstance(raw, str):
            return None
        return json_loads_exact(raw)
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return None


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


# Cap large Redis collections — overflow fails closed (never silent truncate).
_REDIS_COLLECTION_CAP = 10_000


def redis_key_for(prefix: str, identity: Any) -> str:
    """Canonical ``prefix:identity`` key layout shared by write and read-back.

    The identity is sanitized, so a row keyed ``1`` is stored at ``prefix:col_1``
    rather than ``prefix:1``. Gate-8 has to rebuild the exact same key to
    re-read a batch, and a second copy of this rule would silently stop matching
    the moment either side changed.
    """
    from connectors.sql_identifiers import sanitize_identifier
    from services.value_serializer import present_cell_text

    text = present_cell_text(identity)
    return f"{prefix}:{sanitize_identifier(text or '', preserve_case=True)}"


def resolve_key_pattern(name: str | None) -> str:
    """Turn a configured keyspace name into a SCAN MATCH pattern.

    A Redis "table" is a key prefix, and the keys under it are
    ``prefix:something`` — so scanning for the bare prefix matches only a key of
    exactly that name, which is almost never what exists. Callers that skipped
    this saw an empty keyspace and concluded the destination had no fields.
    """
    pattern = (name or "").strip() or "*"
    if pattern != "*" and "*" not in pattern and "?" not in pattern:
        return f"{pattern}:*"
    return pattern


def scan_all_keys(client: Any, pattern: str, *, page: int = 1000) -> list[str]:
    """Every key matching ``pattern`` — one full SCAN, deduped.

    Redis SCAN gives no cardinality and may repeat a key across pages, so the
    only honest population figure is the deduped walk. One owner for it: the
    reader's page total, introspection's row estimate and reconcile's key list
    must agree or an operator sees three different Redis row counts.
    """
    keys: list[str] = []
    seen: set[str] = set()
    cursor = 0
    while True:
        cursor, batch = client.scan(cursor=cursor, match=pattern or "*", count=page)
        for raw in batch:
            key = raw.decode() if isinstance(raw, bytes) else str(raw)
            if key not in seen:
                seen.add(key)
                keys.append(key)
        if cursor == 0:
            return keys


def _read_redis_value(client: Any, key: str, ktype: str) -> str:
    """Typed Redis value read — never WRONGTYPE on set/zset/stream; refuse truncation."""
    if ktype == "hash":
        return json.dumps(
            {(_decode(f)): _decode(v) for f, v in client.hgetall(key).items()},
            default=json_default,
        )
    if ktype == "list":
        llen = int(client.llen(key) or 0)
        if llen > _REDIS_COLLECTION_CAP:
            raise RuntimeError(
                f"Redis list {key!r} has {llen} elements exceeding "
                f"{_REDIS_COLLECTION_CAP} cap; refuse silent truncation"
            )
        vals = [_decode(v) for v in client.lrange(key, 0, _REDIS_COLLECTION_CAP - 1)]
        return json.dumps(vals, default=json_default)
    if ktype == "set":
        members = list(client.smembers(key))
        if len(members) > _REDIS_COLLECTION_CAP:
            raise RuntimeError(
                f"Redis set {key!r} has {len(members)} members exceeding "
                f"{_REDIS_COLLECTION_CAP} cap; refuse silent truncation"
            )
        vals = [_decode(v) for v in members]
        return json.dumps(vals, default=json_default)
    if ktype == "zset":
        zcard = int(client.zcard(key) or 0)
        if zcard > _REDIS_COLLECTION_CAP:
            raise RuntimeError(
                f"Redis zset {key!r} has {zcard} members exceeding "
                f"{_REDIS_COLLECTION_CAP} cap; refuse silent truncation"
            )
        pairs = client.zrange(key, 0, _REDIS_COLLECTION_CAP - 1, withscores=True)
        vals = [[_decode(m), redis_zset_score_carrier(s)] for m, s in pairs]
        return json.dumps(vals, default=json_default)
    if ktype == "stream":
        xlen = int(client.xlen(key) or 0)
        if xlen > _REDIS_COLLECTION_CAP:
            raise RuntimeError(
                f"Redis stream {key!r} has {xlen} entries exceeding "
                f"{_REDIS_COLLECTION_CAP} cap; refuse silent truncation"
            )
        entries = client.xrange(key, count=_REDIS_COLLECTION_CAP)
        vals = [
            {"id": _decode(eid), "fields": {(_decode(f)): _decode(v) for f, v in fields.items()}}
            for eid, fields in entries
        ]
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

    state = RedisScanState.from_any(scan_state)
    first_page = not state.keys_seen and not state.cursor and not state.pending_keys
    client = _redis_client(cfg)
    try:
        identity_headers = ["redis_key", "redis_value", "redis_type"]
        rows: list[list[str]] = []

        while len(rows) < limit and not state.exhausted:
            # Drain buffered keys before issuing another SCAN.
            while state.pending_keys and len(rows) < limit:
                key = state.pending_keys.pop(0)
                if key in state.emitted_keys:
                    continue
                state.emitted_keys.add(key)
                state.keys_seen += 1
                ktype = _decode(client.type(key))
                val = _read_redis_value(client, key, ktype)
                rows.append([key, val, ktype])

            if len(rows) >= limit:
                break
            if state.scan_complete:
                state.exhausted = True
                break

            state.cursor, batch = client.scan(
                cursor=state.cursor,
                match=pattern or "*",
                count=max(limit, 100),
            )
            for raw_key in batch:
                key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
                if key not in state.emitted_keys and key not in state.pending_keys:
                    state.pending_keys.append(key)
            if state.cursor == 0:
                state.scan_complete = True

        # If every stored value is a JSON object, flatten keys BUT keep redis_key /
        # redis_type so upsert identity is never silently dropped.
        object_values: list[dict] = []
        for row in rows:
            parsed = load_redis_json_doc(row[1])
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
            from services.value_serializer import (
                DF_MISSING_SENTINEL,
                SQL_NULL_SENTINEL,
                cell_to_string,
            )

            for row, obj in zip(rows, object_values):
                cells: list[str] = [row[0], row[2]]
                for field in fieldnames:
                    if field not in obj:
                        cells.append(DF_MISSING_SENTINEL)
                    elif obj[field] is None:
                        cells.append(SQL_NULL_SENTINEL)
                    else:
                        cells.append(
                            cell_to_string(obj[field], preserve_sql_null=True)
                        )
                flat_rows.append(cells)
            rows = flat_rows
        else:
            headers = identity_headers

        # ``keys_seen`` is how far this scan has walked, not how many keys exist.
        # Reported as the total it told the stream engine "source drained" at the
        # end of the first page: a 100k-key namespace landed 20k rows and the run
        # still reported success. The population is counted once, on the first
        # page, and carried by the caller as ``known_total_rows`` after that.
        if known_total_rows is not None:
            total = known_total_rows
        elif first_page:
            total = len(scan_all_keys(client, pattern or "*"))
        else:
            total = None
        return ReadBatch(headers=headers, rows=rows, offset=state.keys_seen, total_rows=total), state
    finally:
        client.close()
