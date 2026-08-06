"""Milvus vector destination writer — turns rows into upserted entities.

Uses the Milvus RESTful API v2 (``/v2/vectordb/...``) so no pymilvus SDK is
required. Default listen port is ``19530``. Auth token is
``Bearer username:password`` (Milvus default ``root:Milvus``) or a raw API key
from ``api_key`` / ``password``. Delivery is at-least-once upsert by ``id``.
"""

from __future__ import annotations

import importlib.util
import json
import re
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import cell_to_string, sanitize_json_value
from services.vectorization import vectorize_records

from connectors.writer_common import WriteResult as _WriteResult


def _requests_session() -> Any:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("HEAD", "GET", "PUT", "POST"),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _base_url(host: str, port: int, ssl: bool, connection_string: str = "") -> str:
    if connection_string.strip():
        return connection_string.rstrip("/")
    scheme = "https" if ssl else "http"
    host = host or "localhost"
    port = port or 19530
    return f"{scheme}://{host}:{port}"


def _auth_token(
    *,
    api_key: str = "",
    username: str = "",
    password: str = "",
) -> str:
    """Milvus REST expects Bearer ``user:pass`` or a cloud API key."""
    if api_key.strip():
        return api_key.strip()
    user = (username or "root").strip() or "root"
    pwd = password if password is not None else ""
    if not pwd and not username:
        # Local default when no credentials supplied.
        return "root:Milvus"
    return f"{user}:{pwd}"


def _headers(token: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }


def _collection_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", (name or "dataflow_chunks").strip()) or "dataflow_chunks"
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned[:255]


def _ok_response(payload: dict[str, Any] | None, status_code: int) -> bool:
    if status_code not in {200, 201}:
        return False
    if not isinstance(payload, dict):
        return status_code in {200, 201}
    code = payload.get("code")
    return code in (0, None, "0", 200)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "requests"
    load_method: str = "milvus_upsert"


def test_milvus(
    *,
    host: str = "",
    port: int = 19530,
    api_key: str = "",
    username: str = "",
    password: str = "",
    ssl: bool = False,
    connection_string: str = "",
    **_kwargs: Any,
) -> tuple[bool, str]:
    """Quick connectivity check against Milvus REST v2."""
    try:
        session = _requests_session()
        token = _auth_token(api_key=api_key, username=username, password=password)
        resp = session.post(
            f"{_base_url(host, port, ssl, connection_string)}/v2/vectordb/collections/list",
            data=json.dumps({}),
            headers=_headers(token),
            timeout=10,
        )
        try:
            body = resp.json()
        except Exception:
            body = {}
        if resp.status_code in {401, 403}:
            return False, f"Milvus auth failed ({resp.status_code})"
        if _ok_response(body if isinstance(body, dict) else {}, resp.status_code):
            return True, "Milvus reachable"
        return False, f"Milvus returned {resp.status_code}: {body or resp.text}"
    except Exception as exc:
        return False, str(exc)


def build_milvus_entities(
    vector_rows: list[dict[str, Any]],
    *,
    dimension: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Map Datawrap vector rows to Milvus upsert entities (testable, no I/O).

    Returns ``(entities, rejected)``. Missing/mismatched embeddings and missing
    stable ids are rejected — never fabricate zero vectors or ``"missing"`` ids.
    """
    import hashlib
    import uuid

    from services.vector_embedding import (
        coerce_chunk_index,
        coerce_embedding,
        embedding_reject_reason,
    )

    entities: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in vector_rows:
        meta = dict(sanitize_json_value(row.get("metadata") or {}) or {})
        try:
            chunk = coerce_chunk_index(row.get("chunk_index"))
        except ValueError as exc:
            rejected.append({
                "row": cell_to_string(row.get("id") or ""),
                "column": "chunk_index",
                "target": "chunk_index",
                "value": cell_to_string(row.get("chunk_index")),
                "reason": str(exc),
                "policy": "quarantine",
            })
            continue
        values, err = coerce_embedding(row.get("embedding"), expected_dimension=dimension)
        if err or values is None:
            rejected.append({
                "row": cell_to_string(row.get("id") or ""),
                "column": "embedding",
                "target": "vector",
                "value": "",
                "reason": embedding_reject_reason(row, err),
                "policy": "quarantine",
            })
            continue
        from services.cdc_identity import is_present_cdc_row_key

        raw_id = row.get("id")
        raw_s = (
            cell_to_string(raw_id).strip() if is_present_cdc_row_key(raw_id) else ""
        )
        if raw_s:
            if len(raw_s) > 64:
                # Keep original as metadata; use collision-resistant digest as entity id.
                digest = hashlib.sha256(raw_s.encode("utf-8")).hexdigest()
                entity_id = digest[:64]
                meta["source_entity_id"] = raw_s[:512]
            else:
                entity_id = raw_s
        else:
            source = cell_to_string(row.get("source_id", ""))
            content = str(row.get("content") or "")
            if not source and not content:
                rejected.append({
                    "row": "",
                    "column": "id",
                    "target": "id",
                    "value": "",
                    "reason": "missing id — refuse fabricated 'missing' identity",
                    "policy": "quarantine",
                })
                continue
            digest = hashlib.sha256(f"{source}\0{chunk}\0{content}".encode("utf-8")).hexdigest()
            entity_id = str(uuid.UUID(digest[:32]))
        entity: dict[str, Any] = {
            "id": entity_id,
            "vector": sanitize_json_value(values),
            "content": str(row.get("content") or "")[:65000],
            "source_id": cell_to_string(row.get("source_id", ""))[:256],
            "chunk_index": chunk,
            "filename": str(meta.get("filename") or "")[:512],
            "page": str(meta.get("page") or "")[:64],
            "heading": str(meta.get("heading") or "")[:1024],
            "element_type": str(meta.get("element_type") or row.get("element_type") or "")[:128],
        }
        entities.append(entity)
    return entities, rejected


def _has_collection(
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection_name: str,
    db_name: str = "",
) -> bool:
    payload: dict[str, Any] = {"collectionName": collection_name}
    if db_name:
        payload["dbName"] = db_name
    resp = session.post(
        f"{base_url}/v2/vectordb/collections/has",
        data=json.dumps(payload),
        headers=headers,
        timeout=15,
    )
    body = resp.json() if resp.content else {}
    if not _ok_response(body if isinstance(body, dict) else {}, resp.status_code):
        raise RuntimeError(f"Milvus has-collection failed: {resp.status_code} {body or resp.text}")
    data = body.get("data") if isinstance(body, dict) else None
    if isinstance(data, dict):
        return bool(data.get("has"))
    return bool(data)


def _ensure_collection(
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection_name: str,
    dimension: int,
    db_name: str = "",
) -> None:
    if _has_collection(session, base_url, headers, collection_name, db_name=db_name):
        return

    schema_fields = [
        {
            "fieldName": "id",
            "dataType": "VarChar",
            "isPrimary": True,
            "elementTypeParams": {"max_length": 64},
        },
        {
            "fieldName": "vector",
            "dataType": "FloatVector",
            "elementTypeParams": {"dim": int(dimension)},
        },
        {
            "fieldName": "content",
            "dataType": "VarChar",
            "elementTypeParams": {"max_length": 65535},
        },
        {
            "fieldName": "source_id",
            "dataType": "VarChar",
            "elementTypeParams": {"max_length": 256},
        },
        {"fieldName": "chunk_index", "dataType": "Int64"},
        {
            "fieldName": "filename",
            "dataType": "VarChar",
            "elementTypeParams": {"max_length": 512},
        },
        {
            "fieldName": "page",
            "dataType": "VarChar",
            "elementTypeParams": {"max_length": 64},
        },
        {
            "fieldName": "heading",
            "dataType": "VarChar",
            "elementTypeParams": {"max_length": 1024},
        },
        {
            "fieldName": "element_type",
            "dataType": "VarChar",
            "elementTypeParams": {"max_length": 128},
        },
    ]
    payload: dict[str, Any] = {
        "collectionName": collection_name,
        "schema": {
            "autoID": False,
            "enableDynamicField": False,
            "fields": schema_fields,
        },
        "indexParams": [
            {
                "fieldName": "vector",
                "indexName": "vector_idx",
                "metricType": "COSINE",
                "params": {"index_type": "AUTOINDEX"},
            }
        ],
    }
    if db_name:
        payload["dbName"] = db_name

    resp = session.post(
        f"{base_url}/v2/vectordb/collections/create",
        data=json.dumps(payload),
        headers=headers,
        timeout=30,
    )
    body = resp.json() if resp.content else {}
    if not _ok_response(body if isinstance(body, dict) else {}, resp.status_code):
        # Race: another worker created it.
        if _has_collection(session, base_url, headers, collection_name, db_name=db_name):
            return
        raise RuntimeError(f"Milvus create collection failed: {resp.status_code} {body or resp.text}")


def write_mapped_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    content_column: str | None = None,
    embedding_column: str | None = None,
    metadata_columns: list[str] | None = None,
    exclude_pii_columns: list[str] | None = None,
    embedding_model: str | None = None,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    skip_chunking: bool = False,
    durable_embedding_cache: bool | None = None,
    api_key: str = "",
    **_kwargs: Any,
) -> WriteResult:
    """Write text rows as embedded entities into a Milvus collection."""
    if importlib.util.find_spec("requests") is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error="requests is required for Milvus writes",
            driver="none",
        )

    collection = _collection_name(table_name or database or "dataflow_chunks")
    db_name = (database or schema or "").strip()
    # Milvus default DB is empty / default — only pass when operator set a non-placeholder.
    if db_name.lower() in {"", "test_db", "default", "public"}:
        db_name = ""

    from connectors.writer_common import prepare_records_for_vector_write

    pk_cols = list(
        _kwargs.get("destination_pk_columns")
        or _kwargs.get("conflict_columns")
        or []
    ) or None
    records, map_rejected, map_abort = prepare_records_for_vector_write(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        column_types=column_types,
        error_policy=error_policy,
        dest_kind="milvus",
        destination_pk_columns=pk_cols,
        stream_contracts=_kwargs.get("stream_contracts"),
        contract_primary_key=_kwargs.get("contract_primary_key"),
        label="milvus",
    )
    if map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=collection,
            target_schema=db_name,
            checksum="",
            chunks_completed=0,
            error=map_abort,
            rejected_details=map_rejected,
            rejected_rows=len(map_rejected),
        )
    try:
        vector_rows = vectorize_records(
            records,
            content_column=content_column,
            embedding_column=embedding_column,
            metadata_columns=metadata_columns,
            exclude_pii_columns=exclude_pii_columns,
            model=embedding_model,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            skip_chunking=skip_chunking,
            durable_embedding_cache=durable_embedding_cache,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=collection,
            target_schema=db_name,
            checksum="",
            chunks_completed=0,
            error=f"Vectorization failed: {exc}",
            rejected_details=list(map_rejected),
            rejected_rows=len(map_rejected),
        )

    if not vector_rows:
        return WriteResult(
            ok=True,
            rows_written=0,
            table_name=collection,
            target_schema=db_name,
            checksum="",
            chunks_completed=0,
            rejected_details=list(map_rejected),
            rejected_rows=len(map_rejected),
            warnings=[r.get("reason") or "" for r in map_rejected[:10] if r.get("reason")],
        )

    from services.vector_embedding import resolve_embedding_dimension

    dimension, dim_err = resolve_embedding_dimension(vector_rows, default=None)
    if dimension is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=collection,
            target_schema=db_name,
            checksum="",
            chunks_completed=0,
            error=dim_err or "embedding dimension unknown — refuse fabricated defaults",
            rejected_details=list(map_rejected)
            + [
                {
                    "row": "",
                    "column": "embedding",
                    "target": "vector",
                    "value": "",
                    "reason": dim_err or "no embeddings",
                    "policy": "fail",
                }
            ],
            rejected_rows=len(map_rejected) + 1,
        )

    entities, embed_rejected = build_milvus_entities(vector_rows, dimension=dimension)
    rejected = list(map_rejected) + list(embed_rejected)
    if not entities and rejected:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=collection,
            target_schema=db_name,
            checksum="",
            chunks_completed=0,
            error=(embed_rejected[0].get("reason") if embed_rejected else None)
            or "all embeddings rejected",
            rejected_details=rejected,
            rejected_rows=len(rejected),
        )
    from connectors.writer_common import reject_on_strict_policy

    strict_error = reject_on_strict_policy(error_policy, rejected, "Milvus")
    if strict_error:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=collection,
            target_schema=db_name,
            checksum="",
            chunks_completed=0,
            error=strict_error,
            rejected_details=rejected,
            rejected_rows=len(rejected),
        )

    token = _auth_token(api_key=api_key, username=username, password=password)
    base_url = _base_url(host, port, ssl, connection_string)
    inserted = 0
    try:
        session = _requests_session()
        hdrs = _headers(token)
        collection_exists = _has_collection(
            session, base_url, hdrs, collection, db_name=db_name
        )
        if not collection_exists:
            if not create_table:
                raise RuntimeError(
                    f"Milvus collection '{collection}' is missing and "
                    "create_table is disabled"
                )
            _ensure_collection(
                session, base_url, hdrs, collection, dimension, db_name=db_name
            )

        batch_size = 100
        total = len(entities)
        for i in range(0, total, batch_size):
            batch = entities[i : i + batch_size]
            payload: dict[str, Any] = {
                "collectionName": collection,
                "data": batch,
            }
            if db_name:
                payload["dbName"] = db_name
            resp = session.post(
                f"{base_url}/v2/vectordb/entities/upsert",
                data=json.dumps(payload, default=sanitize_json_value),
                headers=hdrs,
                timeout=60,
            )
            body = resp.json() if resp.content else {}
            if not _ok_response(body if isinstance(body, dict) else {}, resp.status_code):
                raise RuntimeError(f"Milvus upsert failed: {resp.status_code} {body or resp.text}")
            inserted += len(batch)
            if on_checkpoint:
                on_checkpoint((i // batch_size) + 1, (total + batch_size - 1) // batch_size, inserted)
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=inserted,
            table_name=collection,
            target_schema=db_name,
            checksum="",
            chunks_completed=(inserted + 99) // 100,
            error=str(exc),
            rejected_details=rejected,
            rejected_rows=len(rejected),
        )

    _final_abort = reject_on_strict_policy(error_policy, rejected, "Milvus")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=inserted,
            table_name=collection,
            target_schema=db_name,
            checksum="",
            chunks_completed=(inserted + 99) // 100,
            error=_final_abort,
            rejected_details=rejected,
            rejected_rows=len(rejected),
            warnings=[r.get("reason") or "" for r in rejected[:10] if r.get("reason")],
        )

    return WriteResult(
        ok=True,
        rows_written=inserted,
        table_name=collection,
        target_schema=db_name,
        checksum="",
        chunks_completed=(inserted + 99) // 100,
        rejected_details=rejected,
        rejected_rows=len(rejected),
        warnings=[r.get("reason") or "" for r in rejected[:10] if r.get("reason")],
        meta=_milvus_gate8_meta(entities),
    )


def _milvus_gate8_meta(entities: list[dict[str, Any]]) -> dict[str, Any]:
    from connectors.writer_common import vector_gate8_meta

    # Drop opaque embedding floats from Gate-8 sample (identity + metadata only).
    rows = []
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        rows.append({k: v for k, v in ent.items() if k != "vector"})
    return vector_gate8_meta(rows)
