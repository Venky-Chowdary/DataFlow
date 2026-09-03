"""Shared Qdrant identity-COPY helpers.

Dest COUNT is collection ``points_count`` from GET /collections/{name} —
never ``scan_source_ids`` DISTINCT source_id, never upsert ack, never
writer ``rows_written``. Same host+port+collection declines.
Cross-endpoint scroll+upsert declines (identity COPY stays on one
cluster). Occupancy is counted **before** delete. Desktop-lab Qdrant on
:6333 is not a customer-tenant PRODUCTION_SKU.
"""

from __future__ import annotations

from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.value_serializer import json_default, load_http_json, sanitize_json_value

_QDRANT_FAMILY = frozenset({
    "qdrant",
    "qdrant_cloud",
})

_QDRANT_COPY_SAFE_TYPES = frozenset({
    "long",
    "integer",
    "int",
    "float",
    "double",
    "boolean",
    "bool",
    "string",
    "text",
    "keyword",
    "json",
    "datetime",
    "uuid",
    "geo",
})

_SCROLL_BATCH = 256
_UPSERT_BATCH = 100


def qdrant_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _QDRANT_FAMILY:
        return "qdrant"
    return n


def qdrant_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().lower()
    if not raw:
        return True
    base = raw.split("<", 1)[0].split("(", 1)[0].strip()
    return base in _QDRANT_COPY_SAFE_TYPES


def qdrant_collection(table: str) -> str:
    name = (table or "").strip()
    if not name:
        raise FastPathUnavailable("Qdrant collection required")
    if any(ch in name for ch in "*?\\/ "):
        raise FastPathUnavailable("Qdrant COPY refuses glob characters in the collection")
    return name


def qdrant_endpoint_key(cfg: dict[str, Any]) -> str:
    host = str(cfg.get("host") or "").strip().lower()
    port = int(cfg.get("port") or 6333)
    cs = str(cfg.get("connection_string") or "").strip()
    if cs:
        from connectors.url_authority import parse_url_authority

        parsed = parse_url_authority(cs)
        if parsed.host:
            host = str(parsed.host).strip().lower()
        if parsed.port:
            port = int(parsed.port)
        elif "://" in cs:
            scheme = cs.split("://", 1)[0].lower()
            if not parsed.port:
                port = 443 if scheme == "https" else 6333
    host = host.replace("localhost", "127.0.0.1") or "127.0.0.1"
    return f"{host}:{port}"


def qdrant_object_id(cfg: dict[str, Any], collection: str) -> tuple[str, str]:
    return (qdrant_endpoint_key(cfg), qdrant_collection(collection))


def qdrant_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def _collection_info(cfg: dict[str, Any], collection: str) -> dict[str, Any]:
    from connectors.qdrant_writer import qdrant_rest

    session, base_url, headers = qdrant_rest(cfg)
    resp = session.get(
        f"{base_url}/collections/{qdrant_collection(collection)}",
        headers=headers,
        timeout=15,
    )
    if resp.status_code == 404:
        return {}
    if resp.status_code != 200:
        raise ValueError(
            f"Qdrant collection info failed: {resp.status_code} {resp.text[:200]}"
        )
    payload = load_http_json(resp) if resp.content else {}
    result = payload.get("result") if isinstance(payload, dict) else None
    return result if isinstance(result, dict) else {}


def qdrant_points_count(cfg: dict[str, Any], collection: str) -> int:
    """Physical ``points_count`` — never DISTINCT source_id."""
    info = _collection_info(cfg, collection)
    if not info:
        return 0
    from connectors.qdrant_writer import _qdrant_points_count

    n = _qdrant_points_count({"result": info})
    if n is None:
        raise ValueError(f"Qdrant dest points_count unmeasured for {collection}")
    return int(n)


def qdrant_collection_exists(cfg: dict[str, Any], collection: str) -> bool:
    info = _collection_info(cfg, collection)
    return bool(info)


def qdrant_delete_collection(cfg: dict[str, Any], collection: str) -> None:
    from connectors.qdrant_writer import qdrant_rest

    name = qdrant_collection(collection)
    session, base_url, headers = qdrant_rest(cfg)
    resp = session.delete(
        f"{base_url}/collections/{name}",
        headers=headers,
        timeout=30,
    )
    if resp.status_code not in {200, 404}:
        raise ValueError(
            f"Qdrant delete collection failed: {resp.status_code} {resp.text[:200]}"
        )


def qdrant_vectors_config(cfg: dict[str, Any], collection: str) -> dict[str, Any]:
    """Extract ``config.params.vectors`` from the source collection."""
    info = _collection_info(cfg, collection)
    if not info:
        raise FastPathUnavailable("Qdrant source collection missing")
    config = info.get("config") if isinstance(info.get("config"), dict) else {}
    params = config.get("params") if isinstance(config.get("params"), dict) else {}
    vectors = params.get("vectors")
    if not isinstance(vectors, dict) or not vectors:
        raise FastPathUnavailable("Qdrant source collection has no vector config")
    return vectors


def qdrant_create_collection_from_source(
    *,
    dest_cfg: dict[str, Any],
    source_cfg: dict[str, Any],
    source_collection: str,
    dest_collection: str,
) -> None:
    """Create dest with the source collection's vector config."""
    import json

    from connectors.qdrant_writer import qdrant_rest

    vectors = qdrant_vectors_config(source_cfg, source_collection)
    session, base_url, headers = qdrant_rest(dest_cfg)
    name = qdrant_collection(dest_collection)
    resp = session.put(
        f"{base_url}/collections/{name}",
        data=json.dumps({"vectors": vectors}, default=json_default),
        headers=headers,
        timeout=30,
    )
    if resp.status_code not in {200, 201}:
        raise ValueError(
            f"Qdrant create collection failed: {resp.status_code} {resp.text[:200]}"
        )


def qdrant_scroll_upsert(
    *,
    source_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    src_collection: str,
    dest_collection: str,
) -> int:
    """Scroll source points and upsert raw id/vector/payload to dest."""
    import json

    from connectors.qdrant_writer import qdrant_rest

    src_name = qdrant_collection(src_collection)
    dest_name = qdrant_collection(dest_collection)
    session, base_url, headers = qdrant_rest(source_cfg)
    copied = 0
    offset: Any = None
    while True:
        body: dict[str, Any] = {
            "limit": _SCROLL_BATCH,
            "with_vectors": True,
            "with_payload": True,
        }
        if offset is not None:
            body["offset"] = offset
        resp = session.post(
            f"{base_url}/collections/{src_name}/points/scroll",
            data=json.dumps(body, default=json_default),
            headers=headers,
            timeout=60,
        )
        if resp.status_code != 200:
            raise ValueError(
                f"Qdrant scroll failed: {resp.status_code} {resp.text[:200]}"
            )
        payload = load_http_json(resp) if resp.content else {}
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict):
            raise ValueError("Qdrant scroll returned no result")
        points = result.get("points") or []
        if not isinstance(points, list):
            raise ValueError("Qdrant scroll points is not a list")
        if points:
            for i in range(0, len(points), _UPSERT_BATCH):
                batch = points[i : i + _UPSERT_BATCH]
                upsert = session.put(
                    f"{base_url}/collections/{dest_name}/points?wait=true",
                    data=json.dumps(
                        {"points": batch},
                        default=sanitize_json_value,
                    ),
                    headers=headers,
                    timeout=60,
                )
                if upsert.status_code not in {200, 201}:
                    raise ValueError(
                        f"Qdrant upsert failed: {upsert.status_code} {upsert.text[:200]}"
                    )
                copied += len(batch)
        nxt = result.get("next_page_offset")
        if nxt is None:
            break
        offset = nxt
    return copied


def skip_complete_qdrant(
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
        "shard_mode": "collection",
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
