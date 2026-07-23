"""Pinecone vector destination writer — turns rows into upserted vectors.

Uses the Pinecone data-plane REST API (``/vectors/upsert``) so no SDK is
required. ``host`` / ``connection_string`` must be the index host
(e.g. ``https://my-index-xxxx.svc.pinecone.io``). API key is taken from
``api_key`` / ``password``. Namespace defaults to ``table_name`` or ``""``.
Delivery is at-least-once upsert by vector id.
"""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from typing import Any, Callable

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


def _index_url(host: str, connection_string: str = "") -> str:
    raw = (connection_string or host or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw.rstrip("/")
    return f"https://{raw.rstrip('/')}"


def _headers(api_key: str) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Api-Key"] = api_key
    return headers


@dataclass
class WriteResult(_WriteResult):
    driver: str = "requests"
    load_method: str = "pinecone_upsert"


def test_pinecone(
    *,
    host: str = "",
    connection_string: str = "",
    api_key: str = "",
    password: str = "",
    **_kwargs: Any,
) -> tuple[bool, str]:
    """Quick connectivity check against a Pinecone index host."""
    url = _index_url(host, connection_string)
    key = api_key or password or ""
    if not url:
        return False, "Pinecone index host is required"
    if not key:
        return False, "Pinecone API key is required"
    try:
        session = _requests_session()
        resp = session.get(f"{url}/describe_index_stats", headers=_headers(key), timeout=15)
        if resp.status_code in {200, 401, 403}:
            # 401/403 prove the host is reachable; auth may still be wrong.
            return resp.status_code == 200, (
                "Pinecone index reachable" if resp.status_code == 200 else f"Pinecone auth failed ({resp.status_code})"
            )
        return False, f"Pinecone returned {resp.status_code}"
    except Exception as exc:
        return False, str(exc)


def build_pinecone_vectors(
    vector_rows: list[dict[str, Any]],
    *,
    dimension: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Map DataFlow vector rows to Pinecone upsert vectors (testable, no I/O).

    Returns ``(vectors, rejected)``. Missing/mismatched embeddings are rejected
    — never replaced with fabricated zero vectors.
    """
    from services.vector_embedding import coerce_embedding

    vectors: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in vector_rows:
        meta = dict(sanitize_json_value(row.get("metadata") or {}) or {})
        meta["content"] = str(row.get("content") or "")[:40000]
        meta["source_id"] = cell_to_string(row.get("source_id", ""))
        meta["chunk_index"] = int(row.get("chunk_index") or 0)
        # Pinecone metadata values must be string/number/bool/list[string].
        clean_meta: dict[str, Any] = {}
        for k, v in meta.items():
            if v is None:
                continue
            if isinstance(v, (str, int, float, bool)):
                clean_meta[str(k)] = v
            elif isinstance(v, list) and all(isinstance(x, str) for x in v):
                clean_meta[str(k)] = v
            else:
                clean_meta[str(k)] = str(v)
        values, err = coerce_embedding(row.get("embedding"), expected_dimension=dimension)
        if err or values is None:
            rejected.append({
                "row": cell_to_string(row.get("id") or ""),
                "column": "embedding",
                "target": "values",
                "value": "",
                "reason": err or "invalid embedding",
                "policy": "quarantine",
            })
            continue
        vectors.append({
            "id": cell_to_string(row.get("id") or ""),
            "values": sanitize_json_value(values),
            "metadata": clean_meta,
        })
    return vectors, rejected


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
    create_table: bool = True,  # noqa: ARG001 — Pinecone indexes are provisioned out-of-band
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
    """Write text rows as embedded vectors into a Pinecone index namespace."""
    if importlib.util.find_spec("requests") is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error="requests is required for Pinecone writes",
            driver="none",
        )

    index_url = _index_url(host, connection_string)
    key = api_key or password or username or ""
    namespace = (table_name or schema or "").strip()
    if not index_url:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=namespace or "default",
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Pinecone index host is required (host or connection_string)",
        )
    if not key:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=namespace or "default",
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Pinecone API key is required",
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
            table_name=namespace or "default",
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=f"Vectorization failed: {exc}",
        )

    target = namespace or "default"
    if not vector_rows:
        return WriteResult(
            ok=True,
            rows_written=0,
            table_name=target,
            target_schema="",
            checksum="",
            chunks_completed=0,
        )

    from services.vector_embedding import resolve_embedding_dimension

    dimension, dim_err = resolve_embedding_dimension(vector_rows, default=None)
    if dimension is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=target,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=dim_err or "embedding dimension unknown — refuse fabricated defaults",
            rejected_details=[{
                "row": "",
                "column": "embedding",
                "target": "values",
                "value": "",
                "reason": dim_err or "no embeddings",
                "policy": "fail",
            }],
        )

    vectors, rejected = build_pinecone_vectors(vector_rows, dimension=dimension)
    if not vectors and rejected:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=target,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=rejected[0].get("reason") or "all embeddings rejected",
            rejected_details=rejected,
        )
    inserted = 0
    try:
        session = _requests_session()
        hdrs = _headers(key)
        batch_size = 100
        total = len(vectors)
        for i in range(0, total, batch_size):
            batch = vectors[i : i + batch_size]
            payload: dict[str, Any] = {"vectors": batch}
            if namespace:
                payload["namespace"] = namespace
            resp = session.post(
                f"{index_url}/vectors/upsert",
                data=json.dumps(payload, default=sanitize_json_value),
                headers=hdrs,
                timeout=60,
            )
            if resp.status_code not in {200, 201}:
                raise RuntimeError(f"Pinecone upsert failed: {resp.status_code} {resp.text}")
            inserted += len(batch)
            if on_checkpoint:
                on_checkpoint((i // batch_size) + 1, (total + batch_size - 1) // batch_size, inserted)
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=inserted,
            table_name=target,
            target_schema="",
            checksum="",
            chunks_completed=(inserted + 99) // 100,
            error=str(exc),
        )

    return WriteResult(
        ok=True,
        rows_written=inserted,
        table_name=target,
        target_schema="",
        checksum="",
        chunks_completed=(inserted + 99) // 100,
        rejected_details=rejected,
        rejected_rows=len(rejected),
        warnings=[r.get("reason") or "" for r in rejected[:10] if r.get("reason")],
    )
