"""Shared Weaviate identity-COPY helpers.

Dest COUNT is Aggregate ``meta.count`` via GraphQL — never
``scan_source_ids`` DISTINCT source_id, never batch ack, never writer
``rows_written``. Same host+port+class declines. Cross-endpoint
list+batch declines (identity COPY stays on one cluster). Occupancy is
counted **before** delete. Desktop-lab Weaviate on :8080 is not a
customer-tenant PRODUCTION_SKU.
"""

from __future__ import annotations

import json
from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.value_serializer import json_default, load_http_json, sanitize_json_value

_WEAVIATE_FAMILY = frozenset({
    "weaviate",
    "weaviate_cloud",
    "semitechnologies_weaviate",
})

_WEAVIATE_COPY_SAFE_TYPES = frozenset({
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
_BATCH_SIZE = 100


def weaviate_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _WEAVIATE_FAMILY:
        return "weaviate"
    return n


def weaviate_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().lower()
    if not raw:
        return True
    base = raw.split("<", 1)[0].split("(", 1)[0].strip()
    return base in _WEAVIATE_COPY_SAFE_TYPES


def weaviate_class(table: str, cfg: dict[str, Any] | None = None) -> str:
    from connectors.weaviate_writer import _class_name

    name = (table or "").strip()
    if not name and cfg:
        name = str(cfg.get("database") or cfg.get("table") or "").strip()
    if not name:
        raise FastPathUnavailable("Weaviate class required")
    if any(ch in name for ch in "*?\\/ "):
        raise FastPathUnavailable("Weaviate COPY refuses glob characters in the class")
    return _class_name(name)


def weaviate_endpoint_key(cfg: dict[str, Any]) -> str:
    host = str(cfg.get("host") or "").strip().lower()
    port = int(cfg.get("port") or 8080)
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
                port = 443 if scheme == "https" else 8080
    host = host.replace("localhost", "127.0.0.1") or "127.0.0.1"
    return f"{host}:{port}"


def weaviate_object_id(cfg: dict[str, Any], class_name: str) -> tuple[str, str]:
    return (weaviate_endpoint_key(cfg), weaviate_class(class_name, cfg))


def weaviate_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def _weaviate_session(cfg: dict[str, Any]) -> tuple[Any, str, dict[str, str]]:
    from connectors.weaviate_writer import _base_url, _headers, _requests_session

    session = _requests_session()
    api_key = str(cfg.get("api_key") or cfg.get("password") or cfg.get("username") or "")
    base_url = _base_url(
        str(cfg.get("host") or ""),
        int(cfg.get("port") or 8080),
        bool(cfg.get("ssl", False)),
        str(cfg.get("connection_string") or ""),
    )
    return session, base_url, _headers(api_key)


def _class_schema(cfg: dict[str, Any], class_name: str) -> dict[str, Any]:
    session, base_url, headers = _weaviate_session(cfg)
    resp = session.get(
        f"{base_url}/v1/schema/{class_name}",
        headers=headers,
        timeout=15,
    )
    if resp.status_code == 404:
        return {}
    if resp.status_code != 200:
        raise ValueError(
            f"Weaviate schema GET failed: {resp.status_code} {resp.text[:200]}"
        )
    body = resp.json() if resp.content else {}
    return body if isinstance(body, dict) else {}


def weaviate_class_exists(cfg: dict[str, Any], class_name: str) -> bool:
    return bool(_class_schema(cfg, class_name))


def weaviate_object_count(cfg: dict[str, Any], class_name: str) -> int:
    """Physical Aggregate meta.count — never DISTINCT source_id."""
    from connectors.weaviate_writer import _weaviate_aggregate_count

    session, base_url, headers = _weaviate_session(cfg)
    name = weaviate_class(class_name, cfg)
    n = _weaviate_aggregate_count(session, base_url, headers, name)
    if n is None:
        raise ValueError(f"Weaviate dest object count unmeasured for {class_name}")
    return int(n)


def weaviate_delete_class(cfg: dict[str, Any], class_name: str) -> None:
    session, base_url, headers = _weaviate_session(cfg)
    name = weaviate_class(class_name, cfg)
    resp = session.delete(
        f"{base_url}/v1/schema/{name}",
        headers=headers,
        timeout=30,
    )
    if resp.status_code not in {200, 404}:
        raise ValueError(
            f"Weaviate delete class failed: {resp.status_code} {resp.text[:200]}"
        )


def weaviate_create_class_from_source(
    *,
    dest_cfg: dict[str, Any],
    source_cfg: dict[str, Any],
    source_class: str,
    dest_class: str,
) -> None:
    """Create dest class with the source schema (properties + vectorizer)."""
    src_schema = _class_schema(source_cfg, weaviate_class(source_class, source_cfg))
    if not src_schema:
        raise FastPathUnavailable("Weaviate source class missing")
    props = src_schema.get("properties")
    if not isinstance(props, list) or not props:
        raise FastPathUnavailable("Weaviate source class has no properties")
    dest_name = weaviate_class(dest_class, dest_cfg)
    payload: dict[str, Any] = {
        "class": dest_name,
        "vectorizer": src_schema.get("vectorizer") or "none",
        "properties": props,
    }
    for key in ("vectorIndexType", "vectorIndexConfig", "invertedIndexConfig"):
        if key in src_schema:
            payload[key] = src_schema[key]
    session, base_url, headers = _weaviate_session(dest_cfg)
    resp = session.post(
        f"{base_url}/v1/schema",
        data=json.dumps(payload, default=json_default),
        headers=headers,
        timeout=30,
    )
    if resp.status_code not in {200, 201}:
        if weaviate_class_exists(dest_cfg, dest_class):
            return
        raise ValueError(
            f"Weaviate create class failed: {resp.status_code} {resp.text[:200]}"
        )


def _weaviate_assert_batch_ok(batch: list[dict[str, Any]], response_items: Any) -> None:
    """Fail closed on partial or per-object batch rejections (matches weaviate_writer)."""
    if not isinstance(response_items, list) or len(response_items) != len(batch):
        raise ValueError(
            "Weaviate returned incomplete per-object batch acknowledgement"
        )
    failures = [
        item
        for item in response_items
        if not isinstance(item, dict)
        or (item.get("result") or {}).get("errors")
        or str((item.get("result") or {}).get("status") or "").upper() == "FAILED"
    ]
    if failures:
        sample = failures[0]
        err = (sample.get("result") or {}).get("errors") if isinstance(sample, dict) else sample
        raise ValueError(
            f"Weaviate rejected {len(failures)} batch object(s): {str(err)[:200]}"
        )


def _batch_object(obj: dict[str, Any], dest_class: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "class": dest_class,
        "id": obj.get("id"),
        "properties": dict(obj.get("properties") or {}) if isinstance(obj.get("properties"), dict) else {},
    }
    if "vector" in obj:
        out["vector"] = sanitize_json_value(obj.get("vector"))
    return out


def weaviate_list_batch_upsert(
    *,
    source_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    src_class: str,
    dest_class: str,
) -> int:
    """List source objects (with vectors) and batch-upsert to dest."""
    session, base_url, headers = _weaviate_session(source_cfg)
    src_name = weaviate_class(src_class, source_cfg)
    dest_name = weaviate_class(dest_class, dest_cfg)
    total = weaviate_object_count(source_cfg, src_class)
    copied = 0
    offset = 0
    while offset < total:
        page = min(_LIST_PAGE, total - offset)
        resp = session.get(
            f"{base_url}/v1/objects",
            headers=headers,
            params={
                "class": src_name,
                "limit": page,
                "offset": offset,
                "include": "vector",
            },
            timeout=60,
        )
        if resp.status_code != 200:
            raise ValueError(
                f"Weaviate list objects failed: {resp.status_code} {resp.text[:200]}"
            )
        body = load_http_json(resp) if resp.content else {}
        objects = body.get("objects") if isinstance(body, dict) else None
        if not isinstance(objects, list):
            raise ValueError("Weaviate list returned no objects")
        if not objects:
            break
        for i in range(0, len(objects), _BATCH_SIZE):
            batch_raw = objects[i : i + _BATCH_SIZE]
            batch = [_batch_object(obj, dest_name) for obj in batch_raw if isinstance(obj, dict)]
            if not batch:
                continue
            upsert = session.post(
                f"{base_url}/v1/batch/objects",
                data=json.dumps({"objects": batch}, default=sanitize_json_value),
                headers=headers,
                timeout=60,
            )
            if upsert.status_code not in {200, 201}:
                raise ValueError(
                    f"Weaviate batch upsert failed: {upsert.status_code} {upsert.text[:200]}"
                )
            response_items = upsert.json() if upsert.content else []
            _weaviate_assert_batch_ok(batch, response_items)
            copied += len(batch)
        if len(objects) < page:
            break
        offset += len(objects)
    return copied


def skip_complete_weaviate(
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
        "shard_mode": "class",
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
