"""Qdrant vector destination writer — turns rows into upserted points.

Uses the Qdrant REST API (v1.x) so no extra Python client is required.
Points use string UUIDs to avoid integer collisions and support upsert
idempotency. All network calls retry on transient 5xx / 429 errors.
"""

from __future__ import annotations

import importlib.util
import json
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from connectors.writer_common import WriteResult as _WriteResult
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
        data=json.dumps(create_payload),
        headers=headers,
        timeout=10,
    )
    if resp.status_code not in {200, 201}:
        raise RuntimeError(f"Qdrant create collection failed: {resp.status_code} {resp.text}")


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
    embedding_model: str | None = None,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
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
            model=embedding_model,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
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

    collection = table_name or "dataflow_vectors"
    api_key = password or username or ""
    base_url = connection_string if connection_string else _base_url(host, port, ssl)

    inserted = 0
    try:
        session = _requests_session()
        hdrs = _headers(api_key)
        if create_table:
            _ensure_collection(session, base_url, collection, dimension, hdrs)

        batch_size = 100
        total = len(vector_rows)
        for i in range(0, total, batch_size):
            batch = vector_rows[i : i + batch_size]
            points = []
            for row in batch:
                point = {
                    "id": row["id"] if row["id"] else str(uuid.uuid4()),
                    "vector": row["embedding"] if row.get("embedding") else [0.0] * dimension,
                    "payload": row.get("metadata") or {},
                }
                # Qdrant payload must be JSON serializable.
                point["payload"]["content"] = row.get("content", "")
                point["payload"]["source_id"] = row.get("source_id", "")
                point["payload"]["chunk_index"] = row.get("chunk_index", 0)
                points.append(point)

            resp = session.put(
                f"{base_url}/collections/{collection}/points?wait=true",
                data=json.dumps({"points": points}),
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
        )

    return WriteResult(
        ok=True,
        rows_written=inserted,
        table_name=collection,
        target_schema=schema or "",
        checksum="",
        chunks_completed=(inserted + 99) // 100,
    )
