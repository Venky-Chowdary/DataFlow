"""Weaviate vector destination writer — turns rows into upserted objects.

Uses the Weaviate REST API (``/v1``) so no extra Python client is required.
Classes use ``vectorizer: none`` — DataFlow supplies embeddings via
``services/vectorization.py``. Delivery is at-least-once upsert by object id.
"""

from __future__ import annotations

import importlib.util
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from connectors.writer_common import WriteResult as _WriteResult
from services.value_serializer import cell_to_string, json_default, sanitize_json_value
from services.vectorization import vectorize_records


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
    port = port or 8080
    return f"{scheme}://{host}:{port}"


def _headers(api_key: str) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _class_name(name: str) -> str:
    """Weaviate class names must start with uppercase letter."""
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", (name or "DataflowChunk").strip()) or "DataflowChunk"
    if cleaned[0].isdigit():
        cleaned = f"C_{cleaned}"
    return cleaned[0].upper() + cleaned[1:]


def _object_uuid(raw: str) -> str:
    """Weaviate object ids must be UUID strings; map stable hashes deterministically.

    Empty input raises — never uuid4(), which would duplicate under at-least-once retry.
    """
    text = (raw or "").strip()
    if not text:
        raise ValueError("missing id — refuse random UUID (non-idempotent)")
    try:
        return str(uuid.UUID(text))
    except ValueError:
        pass
    if len(text) == 32 and all(c in "0123456789abcdef" for c in text.lower()):
        return str(uuid.UUID(hex=text))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"dataflow:weaviate:{text}"))


@dataclass
class WriteResult(_WriteResult):
    driver: str = "requests"
    load_method: str = "weaviate_upsert"


def test_weaviate(
    *,
    host: str = "",
    port: int = 8080,
    api_key: str = "",
    ssl: bool = False,
    connection_string: str = "",
    **_kwargs: Any,
) -> tuple[bool, str]:
    """Quick connectivity check for Weaviate."""
    try:
        session = _requests_session()
        resp = session.get(
            f"{_base_url(host, port, ssl, connection_string)}/v1/meta",
            headers=_headers(api_key),
            timeout=10,
        )
        if resp.status_code in {200, 401}:
            return True, "Weaviate reachable"
        return False, f"Weaviate returned {resp.status_code}"
    except Exception as exc:
        return False, str(exc)


def build_weaviate_objects(
    vector_rows: list[dict[str, Any]],
    *,
    class_name: str,
    dimension: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Map DataFlow vector rows to Weaviate batch objects (testable, no I/O).

    Returns ``(objects, rejected)``. Missing embeddings are rejected — never
    fabricated as zero vectors. Missing ids → deterministic UUID over
    source_id+chunk+content (retry-safe), else quarantine.
    """
    from services.vector_embedding import coerce_embedding

    objects: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in vector_rows:
        props = dict(sanitize_json_value(row.get("metadata") or {}) or {})
        props["content"] = row.get("content", "")
        props["source_id"] = cell_to_string(row.get("source_id", ""))
        props["chunk_index"] = int(row.get("chunk_index") or 0)
        vector, err = coerce_embedding(row.get("embedding"), expected_dimension=dimension)
        if err or vector is None:
            rejected.append({
                "row": cell_to_string(row.get("id") or ""),
                "column": "embedding",
                "target": "vector",
                "value": "",
                "reason": err or "invalid embedding",
                "policy": "quarantine",
            })
            continue
        raw_id = cell_to_string(row.get("id") or "")
        if not raw_id:
            source = cell_to_string(row.get("source_id", ""))
            chunk = int(row.get("chunk_index") or 0)
            content = str(row.get("content") or "")
            if not source and not content:
                rejected.append({
                    "row": "",
                    "column": "id",
                    "target": "id",
                    "value": "",
                    "reason": "missing id — refuse random UUID (non-idempotent)",
                    "policy": "quarantine",
                })
                continue
            raw_id = f"{source}\0{chunk}\0{content}"
        objects.append({
            "class": class_name,
            "id": _object_uuid(raw_id),
            "properties": props,
            "vector": sanitize_json_value(vector),
        })
    return objects, rejected


def _ensure_class(
    session: Any,
    base_url: str,
    class_name: str,
    headers: dict[str, str],
) -> None:
    resp = session.get(f"{base_url}/v1/schema/{class_name}", headers=headers, timeout=10)
    if resp.status_code == 200:
        return
    payload = {
        "class": class_name,
        "vectorizer": "none",
        "properties": [
            {"name": "content", "dataType": ["text"]},
            {"name": "source_id", "dataType": ["text"]},
            {"name": "chunk_index", "dataType": ["int"]},
            {"name": "filename", "dataType": ["text"]},
            {"name": "page", "dataType": ["text"]},
            {"name": "heading", "dataType": ["text"]},
            {"name": "element_type", "dataType": ["text"]},
        ],
    }
    resp = session.post(
        f"{base_url}/v1/schema",
        data=json.dumps(payload, default=json_default),
        headers=headers,
        timeout=15,
    )
    if resp.status_code not in {200, 201}:
        raise RuntimeError(f"Weaviate create class failed: {resp.status_code} {resp.text}")


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
    """Write text rows as embedded objects into a Weaviate class."""
    if importlib.util.find_spec("requests") is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error="requests is required for Weaviate writes",
            driver="none",
        )

    records = [dict(zip(headers, row)) for row in data_rows]
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
            table_name=table_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=f"Vectorization failed: {exc}",
        )

    class_name = _class_name(table_name or database or "DataflowChunk")
    if not vector_rows:
        return WriteResult(
            ok=True,
            rows_written=0,
            table_name=class_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
        )

    from services.vector_embedding import resolve_embedding_dimension

    dimension, dim_err = resolve_embedding_dimension(vector_rows, default=None)
    if dimension is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=class_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=dim_err or "embedding dimension unknown — refuse fabricated defaults",
            rejected_details=[{
                "row": "",
                "column": "embedding",
                "target": "vector",
                "value": "",
                "reason": dim_err or "no embeddings",
                "policy": "fail",
            }],
        )

    key = api_key or password or username or ""
    base_url = _base_url(host, port, ssl, connection_string)
    objects, rejected = build_weaviate_objects(vector_rows, class_name=class_name, dimension=dimension)
    if not objects and rejected:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=class_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=rejected[0].get("reason") or "all embeddings rejected",
            rejected_details=rejected,
        )
    from connectors.writer_common import reject_on_strict_policy, transform_error_policy

    policy = transform_error_policy(error_policy)
    strict_error = reject_on_strict_policy(policy, rejected, "Weaviate")
    if strict_error:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=class_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=strict_error,
            rejected_details=rejected,
            rejected_rows=len(rejected),
        )

    inserted = 0
    try:
        session = _requests_session()
        hdrs = _headers(key)
        class_resp = session.get(
            f"{base_url}/v1/schema/{class_name}", headers=hdrs, timeout=10
        )
        if class_resp.status_code != 200:
            if not create_table:
                raise RuntimeError(
                    f"Weaviate class '{class_name}' is missing and "
                    "create_table is disabled"
                )
            _ensure_class(session, base_url, class_name, hdrs)

        batch_size = 100
        total = len(objects)
        for i in range(0, total, batch_size):
            batch = objects[i : i + batch_size]
            resp = session.post(
                f"{base_url}/v1/batch/objects",
                data=json.dumps({"objects": batch}, default=sanitize_json_value),
                headers=hdrs,
                timeout=60,
            )
            if resp.status_code not in {200, 201}:
                raise RuntimeError(f"Weaviate batch upsert failed: {resp.status_code} {resp.text}")
            response_items = resp.json() if getattr(resp, "content", None) else []
            if not isinstance(response_items, list) or len(response_items) != len(batch):
                raise RuntimeError(
                    "Weaviate returned incomplete per-object batch acknowledgement"
                )
            failures = [
                item for item in response_items
                if not isinstance(item, dict)
                or (item.get("result") or {}).get("errors")
                or str((item.get("result") or {}).get("status") or "").upper() == "FAILED"
            ]
            if failures:
                for item in failures:
                    if not isinstance(item, dict):
                        continue
                    rejected.append({
                        "row": str(item.get("id") or ""),
                        "column": "",
                        "target": class_name,
                        "value": "",
                        "reason": str((item.get("result") or {}).get("errors") or item)[:500],
                        "policy": "write_fail" if policy == "fail" else "write_quarantine",
                    })
                if policy == "fail":
                    raise RuntimeError(
                        f"Weaviate rejected {len(failures)} batch object(s); "
                        "strict error policy blocks partial activation"
                    )
            inserted += len(batch) - len(failures)
            if on_checkpoint:
                on_checkpoint((i // batch_size) + 1, (total + batch_size - 1) // batch_size, inserted)
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=inserted,
            table_name=class_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=(inserted + 99) // 100,
            error=str(exc),
            rejected_details=rejected,
        )

    return WriteResult(
        ok=True,
        rows_written=inserted,
        table_name=class_name,
        target_schema=schema or "",
        checksum="",
        chunks_completed=(inserted + 99) // 100,
        rejected_details=rejected,
        rejected_rows=len(rejected),
        warnings=[r.get("reason") or "" for r in rejected[:10] if r.get("reason")],
    )
