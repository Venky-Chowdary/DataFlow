"""Shared Milvus identity-COPY helpers.

Dest COUNT is ``count(*)`` from entities/query — never ``scan_source_ids``
DISTINCT source_id, never upsert ack, never writer ``rows_written``. Same
host+port+collection declines. Cross-endpoint query+upsert declines (identity
COPY stays on one cluster). Occupancy is counted **before** delete.
Desktop-lab Milvus on :19530 is not a customer-tenant PRODUCTION_SKU.
"""

from __future__ import annotations

import json
from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.value_serializer import json_default, load_http_json, sanitize_json_value

from connectors.milvus_writer import _MILVUS_QUERY_WINDOW

_MILVUS_FAMILY = frozenset({
    "milvus",
    "zilliz",
    "zilliz_cloud",
})

_MILVUS_COPY_SAFE_TYPES = frozenset({
    "long",
    "integer",
    "int",
    "bigint",
    "smallint",
    "float",
    "double",
    "boolean",
    "bool",
    "string",
    "text",
    "varchar",
    "keyword",
    "json",
    "datetime",
    "uuid",
})

_QUERY_BATCH = 1000
_UPSERT_BATCH = 100


def milvus_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _MILVUS_FAMILY:
        return "milvus"
    return n


def milvus_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().lower()
    if not raw:
        return True
    base = raw.split("<", 1)[0].split("(", 1)[0].strip()
    return base in _MILVUS_COPY_SAFE_TYPES


def milvus_collection(table: str, cfg: dict[str, Any] | None = None) -> str:
    from connectors.milvus_writer import _collection_name

    name = (table or "").strip()
    if not name and cfg:
        name = str(cfg.get("database") or cfg.get("table") or "").strip()
    if not name:
        raise FastPathUnavailable("Milvus collection required")
    if any(ch in name for ch in "*?\\/ "):
        raise FastPathUnavailable("Milvus COPY refuses glob characters in the collection")
    return _collection_name(name)


def milvus_db_name(cfg: dict[str, Any]) -> str:
    db_name = str(cfg.get("database") or cfg.get("schema") or "").strip()
    if db_name.lower() in {"", "test_db", "default", "public"}:
        return ""
    return db_name


def milvus_endpoint_key(cfg: dict[str, Any]) -> str:
    host = str(cfg.get("host") or "").strip().lower()
    port = int(cfg.get("port") or 19530)
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
                port = 443 if scheme == "https" else 19530
    host = host.replace("localhost", "127.0.0.1") or "127.0.0.1"
    db = milvus_db_name(cfg)
    return f"{host}:{port}:{db}"


def milvus_object_id(cfg: dict[str, Any], collection: str) -> tuple[str, str]:
    return (milvus_endpoint_key(cfg), milvus_collection(collection, cfg))


def milvus_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def _milvus_session(cfg: dict[str, Any]) -> tuple[Any, str, dict[str, str], str]:
    from connectors.milvus_writer import (
        _auth_token,
        _base_url,
        _headers,
        _requests_session,
    )

    session = _requests_session()
    token = _auth_token(
        api_key=str(cfg.get("api_key") or ""),
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
    )
    base_url = _base_url(
        str(cfg.get("host") or ""),
        int(cfg.get("port") or 19530),
        bool(cfg.get("ssl", False)),
        str(cfg.get("connection_string") or ""),
    )
    return session, base_url, _headers(token), milvus_db_name(cfg)


def _with_db(payload: dict[str, Any], db_name: str) -> dict[str, Any]:
    if db_name:
        payload = dict(payload)
        payload["dbName"] = db_name
    return payload


def _ok(body: dict[str, Any] | None, status_code: int) -> bool:
    from connectors.milvus_writer import _ok_response

    return _ok_response(body if isinstance(body, dict) else {}, status_code)


def milvus_collection_exists(cfg: dict[str, Any], collection: str) -> bool:
    from connectors.milvus_writer import _has_collection

    session, base_url, headers, db_name = _milvus_session(cfg)
    name = milvus_collection(collection, cfg)
    return _has_collection(session, base_url, headers, name, db_name=db_name)


def milvus_entity_count(cfg: dict[str, Any], collection: str) -> int:
    """Physical ``count(*)`` — never DISTINCT source_id."""
    from connectors.milvus_writer import _milvus_count_star

    session, base_url, headers, db_name = _milvus_session(cfg)
    name = milvus_collection(collection, cfg)
    milvus_load_collection(cfg, collection)
    payload = _with_db(
        {
            "collectionName": name,
            "filter": _milvus_all_filter(cfg, collection),
            "outputFields": ["count(*)"],
        },
        db_name,
    )
    resp = session.post(
        f"{base_url}/v2/vectordb/entities/query",
        data=json.dumps(payload, default=json_default),
        headers=headers,
        timeout=30,
    )
    body = resp.json() if resp.content else {}
    if not _ok(body if isinstance(body, dict) else {}, resp.status_code):
        raise ValueError(
            f"Milvus count(*) failed: {resp.status_code} {str(body)[:200]}"
        )
    n = _milvus_count_star(body.get("data") if isinstance(body, dict) else body)
    if n is None:
        raise ValueError(f"Milvus dest count(*) unmeasured for {collection}")
    return int(n)


def _milvus_describe_raw(cfg: dict[str, Any], collection: str) -> dict[str, Any]:
    session, base_url, headers, db_name = _milvus_session(cfg)
    name = milvus_collection(collection, cfg)
    payload = _with_db({"collectionName": name}, db_name)
    resp = session.post(
        f"{base_url}/v2/vectordb/collections/describe",
        data=json.dumps(payload, default=json_default),
        headers=headers,
        timeout=15,
    )
    body = resp.json() if resp.content else {}
    if not _ok(body if isinstance(body, dict) else {}, resp.status_code):
        return {}
    data = body.get("data") if isinstance(body, dict) else None
    return data if isinstance(data, dict) else {}


def _milvus_field_names(describe: dict[str, Any]) -> list[str]:
    fields = describe.get("fields") or describe.get("schema", {}).get("fields") or []
    names: list[str] = []
    if not isinstance(fields, list):
        return names
    for field in fields:
        if not isinstance(field, dict):
            continue
        name = str(field.get("fieldName") or field.get("name") or "").strip()
        if name:
            names.append(name)
    return names


def _milvus_pk_info(describe: dict[str, Any]) -> tuple[str, str]:
    """Return primary-key field name and Milvus data type from describe payload."""
    fields = describe.get("fields") or describe.get("schema", {}).get("fields") or []
    pk_name = "id"
    pk_type = "VarChar"
    if isinstance(fields, list):
        for field in fields:
            if not isinstance(field, dict):
                continue
            if field.get("isPrimary") or field.get("primaryKey"):
                pk_name = str(
                    field.get("fieldName") or field.get("name") or "id"
                ).strip() or "id"
                pk_type = str(field.get("dataType") or field.get("type") or "VarChar")
                break
    return pk_name, pk_type


def _milvus_all_filter(cfg: dict[str, Any], collection: str) -> str:
    describe = _milvus_describe_raw(cfg, collection)
    pk_name, pk_type = _milvus_pk_info(describe)
    if "INT" in pk_type.upper():
        return f"{pk_name} >= 0"
    return f'{pk_name} != ""'


def _milvus_index_params(
    src_desc: dict[str, Any],
    vector_field: str,
) -> list[dict[str, Any]]:
    """Prefer source collection index params; fall back to AUTOINDEX/COSINE."""
    raw = src_desc.get("indexParams") or src_desc.get("index_params")
    if isinstance(raw, list) and raw:
        return raw
    return [
        {
            "fieldName": vector_field,
            "indexName": f"{vector_field}_idx",
            "metricType": "COSINE",
            "params": {"index_type": "AUTOINDEX"},
        }
    ]


def milvus_load_collection(cfg: dict[str, Any], collection: str) -> None:
    session, base_url, headers, db_name = _milvus_session(cfg)
    name = milvus_collection(collection, cfg)
    payload = _with_db({"collectionName": name}, db_name)
    resp = session.post(
        f"{base_url}/v2/vectordb/collections/load",
        data=json.dumps(payload, default=json_default),
        headers=headers,
        timeout=60,
    )
    body = resp.json() if resp.content else {}
    if not _ok(body if isinstance(body, dict) else {}, resp.status_code):
        raise ValueError(
            f"Milvus load collection failed: {resp.status_code} {str(body)[:200]}"
        )


def milvus_delete_collection(cfg: dict[str, Any], collection: str) -> None:
    session, base_url, headers, db_name = _milvus_session(cfg)
    name = milvus_collection(collection, cfg)
    payload = _with_db({"collectionName": name}, db_name)
    resp = session.post(
        f"{base_url}/v2/vectordb/collections/drop",
        data=json.dumps(payload, default=json_default),
        headers=headers,
        timeout=30,
    )
    body = resp.json() if resp.content else {}
    if not _ok(body if isinstance(body, dict) else {}, resp.status_code):
        if resp.status_code == 404:
            return
        raise ValueError(
            f"Milvus drop collection failed: {resp.status_code} {str(body)[:200]}"
        )


def milvus_create_collection_from_source(
    *,
    dest_cfg: dict[str, Any],
    source_cfg: dict[str, Any],
    source_collection: str,
    dest_collection: str,
) -> None:
    """Create dest with the source collection schema from describe."""
    session, base_url, headers, dest_db = _milvus_session(dest_cfg)
    src_desc = _milvus_describe_raw(source_cfg, source_collection)
    fields = src_desc.get("fields") or src_desc.get("schema", {}).get("fields") or []
    if not isinstance(fields, list) or not fields:
        raise FastPathUnavailable("Milvus source collection schema missing")
    vector_field = ""
    for field in fields:
        if not isinstance(field, dict):
            continue
        dtype = str(field.get("dataType") or field.get("type") or "").upper()
        if "VECTOR" in dtype:
            vector_field = str(field.get("fieldName") or field.get("name") or "vector")
            break
    if not vector_field:
        raise FastPathUnavailable("Milvus source collection has no vector field")
    payload: dict[str, Any] = {
        "collectionName": milvus_collection(dest_collection, dest_cfg),
        "schema": {
            "autoID": bool(src_desc.get("autoID") or src_desc.get("autoId") or False),
            "enableDynamicField": bool(
                src_desc.get("enableDynamicField")
                or src_desc.get("enable_dynamic_field")
                or False
            ),
            "fields": fields,
        },
        "indexParams": _milvus_index_params(src_desc, vector_field),
    }
    payload = _with_db(payload, dest_db)
    resp = session.post(
        f"{base_url}/v2/vectordb/collections/create",
        data=json.dumps(payload, default=json_default),
        headers=headers,
        timeout=60,
    )
    body = resp.json() if resp.content else {}
    if not _ok(body if isinstance(body, dict) else {}, resp.status_code):
        if milvus_collection_exists(dest_cfg, dest_collection):
            return
        raise ValueError(
            f"Milvus create collection failed: {resp.status_code} {str(body)[:200]}"
        )


def milvus_query_upsert(
    *,
    source_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    src_collection: str,
    dest_collection: str,
) -> int:
    """Query source entities and upsert raw rows to dest."""
    session, base_url, headers, db_name = _milvus_session(source_cfg)
    src_name = milvus_collection(src_collection, source_cfg)
    dest_name = milvus_collection(dest_collection, dest_cfg)
    milvus_load_collection(source_cfg, src_collection)
    milvus_load_collection(dest_cfg, dest_collection)
    output_fields = _milvus_field_names(_milvus_describe_raw(source_cfg, src_collection))
    if not output_fields:
        raise ValueError("Milvus source has no output fields")
    filt = _milvus_all_filter(source_cfg, src_collection)
    total = milvus_entity_count(source_cfg, src_collection)
    copied = 0
    offset = 0
    while offset < total:
        limit = min(
            _QUERY_BATCH,
            total - offset,
            _MILVUS_QUERY_WINDOW - 1 - offset,
        )
        if limit <= 0:
            break
        query_payload = _with_db(
            {
                "collectionName": src_name,
                "filter": filt,
                "outputFields": output_fields,
                "limit": limit,
                "offset": offset,
            },
            db_name,
        )
        resp = session.post(
            f"{base_url}/v2/vectordb/entities/query",
            data=json.dumps(query_payload, default=json_default),
            headers=headers,
            timeout=60,
        )
        body = load_http_json(resp) if resp.content else {}
        if not _ok(body if isinstance(body, dict) else {}, resp.status_code):
            raise ValueError(
                f"Milvus query failed: {resp.status_code} {str(body)[:200]}"
            )
        rows = body.get("data") if isinstance(body, dict) else None
        if not isinstance(rows, list):
            raise ValueError("Milvus query returned no data")
        if rows:
            for i in range(0, len(rows), _UPSERT_BATCH):
                batch = rows[i : i + _UPSERT_BATCH]
                upsert_payload = _with_db(
                    {"collectionName": dest_name, "data": batch},
                    db_name,
                )
                upsert = session.post(
                    f"{base_url}/v2/vectordb/entities/upsert",
                    data=json.dumps(upsert_payload, default=sanitize_json_value),
                    headers=headers,
                    timeout=60,
                )
                upsert_body = upsert.json() if upsert.content else {}
                if not _ok(
                    upsert_body if isinstance(upsert_body, dict) else {},
                    upsert.status_code,
                ):
                    raise ValueError(
                        f"Milvus upsert failed: {upsert.status_code} {str(upsert_body)[:200]}"
                    )
                copied += len(batch)
        if len(rows) < limit:
            break
        offset += len(rows)
    return copied


def skip_complete_milvus(
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
