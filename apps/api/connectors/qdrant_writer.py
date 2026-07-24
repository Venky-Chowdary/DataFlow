"""Qdrant vector destination writer — turns rows into upserted points.

Uses the Qdrant REST API (v1.x) so no extra Python client is required.
Points use string UUIDs to avoid integer collisions and support upsert
idempotency. All network calls retry on transient 5xx / 429 errors.
"""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import json_default

from connectors.writer_common import WriteResult as _WriteResult
from services.value_serializer import cell_to_string, sanitize_json_value
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


def _base_url(host: str, port: int, ssl: bool) -> str:
    scheme = "https" if ssl else "http"
    if not host:
        host = "localhost"
    port = port or 6333
    return f"{scheme}://{host}:{port}"


def _headers(api_key: str) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["api-key"] = api_key
    return headers


@dataclass
class WriteResult(_WriteResult):
    driver: str = "requests"
    load_method: str = "qdrant_upsert"


def test_qdrant(
    *,
    host: str = "",
    port: int = 6333,
    api_key: str = "",
    ssl: bool = False,
    **_kwargs: Any,
) -> tuple[bool, str]:
    """Quick connectivity check for Qdrant."""
    try:
        session = _requests_session()
        resp = session.get(
            f"{_base_url(host, port, ssl)}/collections",
            headers=_headers(api_key),
            timeout=10,
        )
        if resp.status_code in {200, 401}:
            return True, "Qdrant reachable"
        return False, f"Qdrant returned {resp.status_code}"
    except Exception as exc:
        return False, str(exc)


def _ensure_collection(
    session: Any,
    base_url: str,
    collection: str,
    dimension: int,
    headers: dict[str, str],
    distance: str = "Cosine",
) -> None:
    resp = session.get(f"{base_url}/collections/{collection}", headers=headers, timeout=10)
    if resp.status_code == 200:
        return

    create_payload = {
        "vectors": {
            "size": dimension,
            "distance": distance,
        }
    }
    resp = session.put(
        f"{base_url}/collections/{collection}",
        data=json.dumps(create_payload, default=json_default),
        headers=headers,
        timeout=10,
    )
    if resp.status_code not in {200, 201}:
        raise RuntimeError(f"Qdrant create collection failed: {resp.status_code} {resp.text}")


def build_qdrant_points(
    vector_rows: list[dict[str, Any]],
    *,
    dimension: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Map vector rows to Qdrant points. Returns ``(points, rejected)``.

    Missing embeddings → quarantine (never zero vectors). Missing ids →
    deterministic UUID over source_id+chunk+content (retry-safe), else reject.
    """
    import hashlib
    import uuid as uuid_mod

    from services.vector_embedding import coerce_embedding

    points: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in vector_rows:
        values, err = coerce_embedding(row.get("embedding"), expected_dimension=dimension)
        if err or values is None:
            rejected.append({
                "row": cell_to_string(row.get("id") or ""),
                "column": "embedding",
                "target": "vector",
                "value": "",
                "reason": err or "invalid embedding",
                "policy": "quarantine",
            })
            continue
        raw_id = row.get("id")
        if raw_id not in (None, ""):
            point_id: str | None = cell_to_string(raw_id)
        else:
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
            digest = hashlib.sha256(f"{source}\0{chunk}\0{content}".encode("utf-8")).hexdigest()
            point_id = str(uuid_mod.UUID(digest[:32]))
        payload = sanitize_json_value(row.get("metadata") or {}) or {}
        if not isinstance(payload, dict):
            payload = {"_meta": payload}
        payload["content"] = row.get("content", "")
        payload["source_id"] = cell_to_string(row.get("source_id", ""))
        payload["chunk_index"] = row.get("chunk_index", 0)
        points.append({
            "id": point_id,
            "vector": sanitize_json_value(values),
            "payload": payload,
        })
    return points, rejected


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
    **_kwargs: Any,
) -> WriteResult:
    """Write text rows as embedded points into a Qdrant collection."""
    if importlib.util.find_spec("requests") is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error="requests is required for Qdrant writes",
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

    if not vector_rows:
        return WriteResult(
            ok=True,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
        )

    dimension = 384
    for row in vector_rows:
        if row.get("embedding"):
            dimension = len(row["embedding"])
            break

    points, rejected = build_qdrant_points(vector_rows, dimension=dimension)
    if not points and rejected:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name or "dataflow_vectors",
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=rejected[0].get("reason") or "all embeddings rejected",
            rejected_details=rejected,
            rejected_rows=len(rejected),
        )
    from connectors.writer_common import reject_on_strict_policy

    strict_error = reject_on_strict_policy(error_policy, rejected, "Qdrant")
    if strict_error:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name or "dataflow_vectors",
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=strict_error,
            rejected_details=rejected,
            rejected_rows=len(rejected),
        )

    collection = table_name or "dataflow_vectors"
    api_key = password or username or ""
    base_url = connection_string if connection_string else _base_url(host, port, ssl)

    inserted = 0
    try:
        session = _requests_session()
        hdrs = _headers(api_key)
        exists = session.get(
            f"{base_url}/collections/{collection}", headers=hdrs, timeout=10
        )
        if exists.status_code != 200:
            if not create_table:
                raise RuntimeError(
                    f"Qdrant collection '{collection}' is missing and "
                    "create_table is disabled"
                )
            _ensure_collection(session, base_url, collection, dimension, hdrs)

        batch_size = 100
        total = len(points)
        for i in range(0, total, batch_size):
            batch = points[i : i + batch_size]
            resp = session.put(
                f"{base_url}/collections/{collection}/points?wait=true",
                data=json.dumps({"points": batch}, default=sanitize_json_value),
                headers=hdrs,
                timeout=30,
            )
            if resp.status_code not in {200, 201}:
                raise RuntimeError(f"Qdrant upsert failed: {resp.status_code} {resp.text}")
            inserted += len(batch)
            if on_checkpoint:
                on_checkpoint((i // batch_size) + 1, (total + batch_size - 1) // batch_size, inserted)
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=inserted,
            table_name=collection,
            target_schema=schema or "",
            checksum="",
            chunks_completed=(inserted + 99) // 100,
            error=str(exc),
            rejected_details=rejected,
            rejected_rows=len(rejected),
        )

    return WriteResult(
        ok=True,
        rows_written=inserted,
        table_name=collection,
        target_schema=schema or "",
        checksum="",
        chunks_completed=(inserted + 99) // 100,
        rejected_details=rejected,
        rejected_rows=len(rejected),
        warnings=[r.get("reason") or "" for r in rejected[:10] if r.get("reason")],
    )
