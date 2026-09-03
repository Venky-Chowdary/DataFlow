"""Shared Pinecone identity-COPY helpers.

Dest COUNT is ``describe_index_stats`` namespace ``vectorCount`` — never
``scan_source_ids`` DISTINCT source_id, never upsert ack, never writer
``rows_written``. Same index host+namespace declines. Cross-index
list+fetch+upsert declines (identity COPY stays on one index). Occupancy
is counted **before** delete. Pod indexes without ``/vectors/list`` decline.
Desktop-lab Pinecone is not a customer-tenant PRODUCTION_SKU.
"""

from __future__ import annotations

import json
from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.value_serializer import json_default, load_http_json, sanitize_json_value

_PINECONE_FAMILY = frozenset({
    "pinecone",
    "pinecone_serverless",
    "pinecone_pod",
})

_PINECONE_COPY_SAFE_TYPES = frozenset({
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
    "array",
})

_LIST_PAGE = 100
_FETCH_BATCH = 100
_UPSERT_BATCH = 100


def pinecone_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _PINECONE_FAMILY:
        return "pinecone"
    return n


def pinecone_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().lower()
    if not raw:
        return True
    base = raw.split("<", 1)[0].split("(", 1)[0].strip()
    return base in _PINECONE_COPY_SAFE_TYPES


def pinecone_namespace(table: str, cfg: dict[str, Any] | None = None) -> str:
    name = (table or "").strip()
    if not name and cfg:
        name = str(cfg.get("database") or cfg.get("table") or cfg.get("schema") or "").strip()
    if not name:
        raise FastPathUnavailable("Pinecone namespace required")
    if any(ch in name for ch in "*?\\/ "):
        raise FastPathUnavailable("Pinecone COPY refuses glob characters in the namespace")
    return name


def pinecone_endpoint_key(cfg: dict[str, Any]) -> str:
    from connectors.pinecone_writer import _index_url

    url = _index_url(
        str(cfg.get("host") or ""),
        str(cfg.get("connection_string") or ""),
    )
    if not url:
        return ""
    return url.rstrip("/").lower()


def pinecone_object_id(cfg: dict[str, Any], namespace: str) -> tuple[str, str]:
    return (pinecone_endpoint_key(cfg), pinecone_namespace(namespace, cfg))


def pinecone_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def _pinecone_session(cfg: dict[str, Any]) -> tuple[Any, str, dict[str, str]]:
    from connectors.pinecone_writer import _headers, _index_url, _requests_session

    session = _requests_session()
    key = str(cfg.get("api_key") or cfg.get("password") or cfg.get("username") or "")
    index_url = _index_url(
        str(cfg.get("host") or ""),
        str(cfg.get("connection_string") or ""),
    )
    if not index_url:
        raise FastPathUnavailable("Pinecone index host required")
    return session, index_url, _headers(key)


def pinecone_vector_count(cfg: dict[str, Any], namespace: str) -> int:
    """Physical namespace vectorCount — never DISTINCT source_id."""
    from connectors.pinecone_writer import _pinecone_total_vector_count

    session, index_url, headers = _pinecone_session(cfg)
    ns = pinecone_namespace(namespace, cfg)
    resp = session.get(
        f"{index_url}/describe_index_stats",
        headers=headers,
        timeout=15,
    )
    if resp.status_code != 200:
        raise ValueError(
            f"Pinecone describe_index_stats failed: {resp.status_code} {resp.text[:200]}"
        )
    body = resp.json() if resp.content else {}
    n = _pinecone_total_vector_count(body if isinstance(body, dict) else None, namespace=ns)
    return int(n)


def pinecone_namespace_exists(cfg: dict[str, Any], namespace: str) -> bool:
    """True when stats report vectors in the namespace (empty ns is not occupied)."""
    try:
        return pinecone_vector_count(cfg, namespace) > 0
    except ValueError:
        return False


def pinecone_delete_namespace(cfg: dict[str, Any], namespace: str) -> None:
    session, index_url, headers = _pinecone_session(cfg)
    ns = pinecone_namespace(namespace, cfg)
    payload: dict[str, Any] = {"deleteAll": True}
    if ns:
        payload["namespace"] = ns
    resp = session.post(
        f"{index_url}/vectors/delete",
        data=json.dumps(payload, default=json_default),
        headers=headers,
        timeout=60,
    )
    if resp.status_code not in {200, 404}:
        raise ValueError(
            f"Pinecone delete namespace failed: {resp.status_code} {resp.text[:200]}"
        )


def _pinecone_list_supported(session: Any, index_url: str, headers: dict[str, str], namespace: str) -> bool:
    params: dict[str, Any] = {"limit": 1}
    if namespace:
        params["namespace"] = namespace
    resp = session.get(
        f"{index_url}/vectors/list",
        headers=headers,
        params=params,
        timeout=15,
    )
    if resp.status_code in {404, 400, 501}:
        return False
    return resp.status_code == 200


def pinecone_list_fetch_upsert(
    *,
    source_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    src_namespace: str,
    dest_namespace: str,
) -> int:
    """List+fetch source vectors and upsert raw id/values/metadata to dest namespace."""
    from connectors.pinecone_writer import _pinecone_vector_id

    session, index_url, headers = _pinecone_session(source_cfg)
    src_ns = pinecone_namespace(src_namespace, source_cfg)
    dest_ns = pinecone_namespace(dest_namespace, dest_cfg)
    if pinecone_endpoint_key(source_cfg) != pinecone_endpoint_key(dest_cfg):
        raise ValueError("Pinecone COPY requires the same index host")
    if not _pinecone_list_supported(session, index_url, headers, src_ns):
        raise FastPathUnavailable(
            "Pinecone index does not support /vectors/list (pod indexes decline identity COPY)"
        )

    ids: list[str] = []
    token = ""
    while True:
        params: dict[str, Any] = {"limit": _LIST_PAGE}
        if src_ns:
            params["namespace"] = src_ns
        if token:
            params["paginationToken"] = token
        listed = session.get(
            f"{index_url}/vectors/list",
            headers=headers,
            params=params,
            timeout=30,
        )
        if listed.status_code != 200:
            raise ValueError(
                f"Pinecone list failed: {listed.status_code} {listed.text[:200]}"
            )
        page = load_http_json(listed) if listed.content else {}
        rows = page.get("vectors") if isinstance(page, dict) else None
        if not isinstance(rows, list):
            raise ValueError("Pinecone list returned no vectors")
        for entry in rows:
            vid = _pinecone_vector_id(entry)
            if vid:
                ids.append(vid)
        pagination = page.get("pagination") if isinstance(page, dict) else None
        nxt = ""
        if isinstance(pagination, dict):
            nxt = str(pagination.get("next") or "").strip()
        if not nxt:
            break
        token = nxt

    if not ids:
        return 0

    copied = 0
    for i in range(0, len(ids), _FETCH_BATCH):
        chunk = ids[i : i + _FETCH_BATCH]
        fetch_body: dict[str, Any] = {"ids": chunk}
        if src_ns:
            fetch_body["namespace"] = src_ns
        fetched = session.post(
            f"{index_url}/vectors/fetch",
            data=json.dumps(fetch_body, default=json_default),
            headers=headers,
            timeout=60,
        )
        if fetched.status_code != 200:
            raise ValueError(
                f"Pinecone fetch failed: {fetched.status_code} {fetched.text[:200]}"
            )
        fetched_body = load_http_json(fetched) if fetched.content else {}
        vectors_map = fetched_body.get("vectors") if isinstance(fetched_body, dict) else None
        if not isinstance(vectors_map, dict):
            raise ValueError("Pinecone fetch returned no vectors map")
        batch_vectors: list[dict[str, Any]] = []
        for vid in chunk:
            vector = vectors_map.get(vid)
            if not isinstance(vector, dict):
                continue
            entry: dict[str, Any] = {
                "id": vid,
                "values": sanitize_json_value(vector.get("values")),
            }
            if isinstance(vector.get("metadata"), dict):
                entry["metadata"] = sanitize_json_value(vector.get("metadata"))
            batch_vectors.append(entry)
        if not batch_vectors:
            continue
        for j in range(0, len(batch_vectors), _UPSERT_BATCH):
            upsert_batch = batch_vectors[j : j + _UPSERT_BATCH]
            payload: dict[str, Any] = {"vectors": upsert_batch}
            if dest_ns:
                payload["namespace"] = dest_ns
            upsert = session.post(
                f"{index_url}/vectors/upsert",
                data=json.dumps(payload, default=sanitize_json_value),
                headers=headers,
                timeout=60,
            )
            if upsert.status_code not in {200, 201}:
                raise ValueError(
                    f"Pinecone upsert failed: {upsert.status_code} {upsert.text[:200]}"
                )
            copied += len(upsert_batch)
    return copied


def skip_complete_pinecone(
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
        "shard_mode": "namespace",
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
