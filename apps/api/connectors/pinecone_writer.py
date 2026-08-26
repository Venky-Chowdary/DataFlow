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
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import cell_to_string, load_http_json, sanitize_json_value
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


def _pinecone_live_dimension(payload: dict[str, Any] | None) -> int | None:
    """Extract index dimension from ``describe_index_stats`` JSON."""
    if not isinstance(payload, dict):
        return None
    raw = payload.get("dimension")
    if raw is None:
        return None
    try:
        dim = int(raw)
    except (TypeError, ValueError):
        return None
    return dim if dim > 0 else None


def _pinecone_total_vector_count(
    payload: dict[str, Any] | None,
    *,
    namespace: str = "",
) -> int:
    """Live vector count from describe_index_stats.

    When ``namespace`` is set, prefer that namespace's count so an empty target
    namespace is not blocked by siblings (and a populated target is not missed
    when only per-namespace stats are present).
    """
    if not isinstance(payload, dict):
        return 0
    ns = (namespace or "").strip()
    namespaces = payload.get("namespaces")
    if ns and isinstance(namespaces, dict):
        entry = namespaces.get(ns)
        if isinstance(entry, dict):
            raw = entry.get("vectorCount")
            if raw is None:
                raw = entry.get("vector_count")
            try:
                return max(0, int(raw or 0))
            except (TypeError, ValueError):
                return 0
        # Named namespace absent from stats → treat as empty for that namespace.
        return 0
    raw = payload.get("totalVectorCount")
    if raw is None:
        raw = payload.get("total_vector_count")
    try:
        return max(0, int(raw or 0))
    except (TypeError, ValueError):
        return 0


_PINECONE_LIST_PAGE = 100
_PINECONE_FETCH_BATCH = 100


def _pinecone_vector_id(entry: Any) -> str:
    if isinstance(entry, dict):
        return str(entry.get("id") or "").strip()
    return str(entry or "").strip()


def _pinecone_metadata_source_id(vector: Any) -> Any:
    if not isinstance(vector, dict):
        return None
    meta = vector.get("metadata")
    if not isinstance(meta, dict):
        return None
    if "source_id" in meta:
        return meta.get("source_id")
    return meta.get("sourceId")


def scan_source_ids(
    cfg: Mapping[str, Any],
    *,
    table_name: str,
    max_entities: int = 20_000,
) -> tuple[str, list[Any]]:
    """Dest-engine metadata ``source_id`` values. Never ``vectorCount``.

    Pinecone ``describe_index_stats`` is physical vectors (the Fivetran
    ``_deleted`` analogue for RAG: 2 documents → 5 chunks looks like
    duplication). Identity is DISTINCT ``source_id`` from list+fetch of
    the namespace the writer filled. Pod indexes that cannot ``/vectors/list``
    stay unmeasured — never fall back to ``vectorCount``.

    Returns ``(state, values)`` matching Milvus/Qdrant:

    * ``missing`` — named namespace absent / empty (create-on-first-write → 0)
    * ``no_field`` — fetched vectors have no ``source_id`` metadata
    * ``truncated`` — physical cardinality exceeds the census bound
    * ``complete`` — every listed vector's ``source_id`` is in ``values``
    * ``unmeasured`` — auth / transport / list-unsupported / fetch failure
    """
    namespace = str(table_name or cfg.get("schema") or "").strip()
    index_url = _index_url(
        str(cfg.get("host") or ""),
        str(cfg.get("connection_string") or ""),
    )
    if not index_url:
        return "unmeasured", []
    try:
        session = _requests_session()
        key = str(cfg.get("api_key") or cfg.get("password") or cfg.get("username") or "")
        hdrs = _headers(key)
        stats = session.get(
            f"{index_url}/describe_index_stats", headers=hdrs, timeout=15
        )
        status = int(stats.status_code)
        if status in {401, 403}:
            return "unmeasured", []
        if status == 404:
            return "unmeasured", []
        if status != 200:
            return "unmeasured", []
        try:
            body = stats.json()
        except Exception:
            return "unmeasured", []
        payload = body if isinstance(body, dict) else None
        physical = _pinecone_total_vector_count(payload, namespace=namespace)
        cap = int(max_entities)
        if physical == 0:
            return "complete", []
        if physical > cap:
            return "truncated", []
        ids: list[str] = []
        token = ""
        while True:
            params: dict[str, Any] = {"limit": _PINECONE_LIST_PAGE}
            if namespace:
                params["namespace"] = namespace
            if token:
                params["paginationToken"] = token
            listed = session.get(
                f"{index_url}/vectors/list",
                headers=hdrs,
                params=params,
                timeout=30,
            )
            if listed.status_code in {404, 400, 501}:
                # Pod-based indexes have no list API. vectorCount is not identity.
                return "unmeasured", []
            if listed.status_code != 200:
                return "unmeasured", []
            try:
                page = load_http_json(listed)
            except Exception:
                return "unmeasured", []
            rows = page.get("vectors") if isinstance(page, dict) else None
            if not isinstance(rows, list):
                return "unmeasured", []
            for entry in rows:
                vid = _pinecone_vector_id(entry)
                if vid:
                    ids.append(vid)
                if len(ids) > cap:
                    return "truncated", []
            pagination = page.get("pagination") if isinstance(page, dict) else None
            nxt = ""
            if isinstance(pagination, dict):
                nxt = str(pagination.get("next") or "").strip()
            if not nxt:
                break
            token = nxt
        if not ids:
            # Stats said vectors exist but list returned none — lag or ACL, not empty.
            return "unmeasured", []
        if len(ids) > physical:
            return "truncated", []
        values: list[Any] = []
        saw_field = False
        for i in range(0, len(ids), _PINECONE_FETCH_BATCH):
            chunk = ids[i : i + _PINECONE_FETCH_BATCH]
            fetch_body: dict[str, Any] = {"ids": chunk}
            if namespace:
                fetch_body["namespace"] = namespace
            fetched = session.post(
                f"{index_url}/vectors/fetch",
                data=json.dumps(fetch_body),
                headers=hdrs,
                timeout=60,
            )
            if fetched.status_code != 200:
                return "unmeasured", []
            try:
                fetched_body = load_http_json(fetched)
            except Exception:
                return "unmeasured", []
            vectors = fetched_body.get("vectors") if isinstance(fetched_body, dict) else None
            if not isinstance(vectors, dict):
                return "unmeasured", []
            for vid in chunk:
                vector = vectors.get(vid)
                if isinstance(vector, dict) and (
                    "source_id" in (vector.get("metadata") or {})
                    or "sourceId" in (vector.get("metadata") or {})
                ):
                    saw_field = True
                values.append(_pinecone_metadata_source_id(vector))
        if not saw_field:
            return "no_field", []
        return "complete", values
    except Exception:
        return "unmeasured", []


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


def _pinecone_metadata_value(value: Any) -> Any | None:
    """Single-value view of ``vector_prepare_metadata``."""
    from connectors.writer_common import vector_prepare_metadata

    prepared = vector_prepare_metadata({"_": value})
    return prepared.get("_")


def build_pinecone_vectors(
    vector_rows: list[dict[str, Any]],
    *,
    dimension: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Map Datawrap vector rows to Pinecone upsert vectors (testable, no I/O).

    Returns ``(vectors, rejected)``. Missing/mismatched embeddings are rejected
    — never replaced with fabricated zero vectors. Missing ids → deterministic
    hash over source_id+chunk+content (retry-safe), else quarantine — never
    empty-string ids that collide under at-least-once upsert.
    """
    import hashlib

    from services.vector_embedding import (
        coerce_chunk_index,
        coerce_embedding,
        embedding_reject_reason,
        vector_cell_token,
        vector_fallback_material,
        vector_reject_row_label,
    )

    vectors: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in vector_rows:
        meta = dict(sanitize_json_value(row.get("metadata") or {}) or {})
        meta["content"] = vector_cell_token(row.get("content"))[:40000]
        meta["source_id"] = vector_cell_token(row.get("source_id"))
        try:
            chunk = coerce_chunk_index(row.get("chunk_index"))
        except ValueError as exc:
            rejected.append({
                "row": vector_reject_row_label(row),
                "column": "chunk_index",
                "target": "chunk_index",
                "value": cell_to_string(row.get("chunk_index")),
                "reason": str(exc),
                "policy": "quarantine",
            })
            continue
        meta["chunk_index"] = chunk
        # Pinecone metadata values must be string/number/bool/list[string].
        from connectors.writer_common import vector_prepare_metadata

        clean_meta = vector_prepare_metadata(meta)
        values, err = coerce_embedding(row.get("embedding"), expected_dimension=dimension)
        if err or values is None:
            rejected.append({
                "row": vector_reject_row_label(row),
                "column": "embedding",
                "target": "values",
                "value": "",
                "reason": embedding_reject_reason(row, err),
                "policy": "quarantine",
            })
            continue
        from services.cdc_identity import is_present_cdc_row_key

        raw_id = row.get("id")
        vector_id = (
            cell_to_string(raw_id).strip() if is_present_cdc_row_key(raw_id) else ""
        )
        if not vector_id:
            material = vector_fallback_material(row.get("source_id"), chunk, row.get("content"))
            if material is None:
                rejected.append({
                    "row": "",
                    "column": "id",
                    "target": "id",
                    "value": "",
                    "reason": "missing id — refuse empty vector identity (non-idempotent)",
                    "policy": "quarantine",
                })
                continue
            vector_id = hashlib.sha256(material.encode("utf-8")).hexdigest()
        vectors.append({
            "id": vector_id,
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

    from connectors.writer_common import prepare_records_for_vector_write

    pk_cols = list(
        _kwargs.get("destination_pk_columns")
        or _kwargs.get("conflict_columns")
        or []
    ) or None
    studio_live = _kwargs.get("destination_column_types")
    live_meta_types: dict[str, str] = {}
    if isinstance(studio_live, dict):
        live_meta_types.update(
            {str(k): str(v) for k, v in studio_live.items() if k and v}
        )
    mapped_targets = [
        str(m.get("target") or m.get("source") or "").strip()
        for m in (mappings or [])
        if str(m.get("target") or m.get("source") or "").strip()
    ]
    if not mapped_targets:
        mapped_targets = [str(h) for h in (headers or []) if h]
    studio_typed_all = (
        isinstance(studio_live, dict)
        and bool(mapped_targets)
        and all(str(studio_live.get(c) or "").strip() for c in mapped_targets)
    )

    # Pinecone has no payload DDL API — when the index already holds vectors,
    # Map VARCHAR invent on metadata is refuse-closed unless Studio is complete
    # (Redis empty-sample / object-store class).
    cached_stats: dict[str, Any] | None = None
    cached_live_dim: int | None = None
    index_has_vectors = False
    try:
        session = _requests_session()
        hdrs = _headers(key)
        stats = session.get(
            f"{index_url}/describe_index_stats", headers=hdrs, timeout=15
        )
        status = int(stats.status_code)
        if status in {401, 403}:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=namespace or "default",
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Pinecone describe_index_stats auth failed ({status}) — "
                    "refuse Map VARCHAR metadata bind (empty→null invent risk)."
                ),
            )
        if status == 200:
            try:
                body = stats.json()
                cached_stats = body if isinstance(body, dict) else {}
            except Exception:
                cached_stats = {}
            cached_live_dim = _pinecone_live_dimension(cached_stats)
            index_has_vectors = (
                _pinecone_total_vector_count(cached_stats, namespace=namespace) > 0
            )
            if index_has_vectors and not studio_typed_all:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=namespace or "default",
                    target_schema="",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        "Pinecone index already contains vectors but Studio "
                        "destination types are incomplete — refuse Map VARCHAR "
                        "metadata bind (empty→null / filter-type invent risk). "
                        "Pass full destination_column_types or clear the namespace."
                    ),
                )
        elif not studio_typed_all:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=namespace or "default",
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Pinecone describe_index_stats failed ({status}) — "
                    "refuse Map VARCHAR metadata bind without live index stats."
                ),
            )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=namespace or "default",
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=f"Pinecone schema probe failed: {exc}",
        )

    records, map_rejected, map_abort = prepare_records_for_vector_write(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        column_types=column_types,
        error_policy=error_policy,
        dest_kind="pinecone",
        destination_pk_columns=pk_cols,
        stream_contracts=_kwargs.get("stream_contracts"),
        contract_primary_key=_kwargs.get("contract_primary_key"),
        label="pinecone",
        destination_column_nullability=_kwargs.get("destination_column_nullability"),
        # Pass Studio/live whenever present — partial Studio fail-closes in
        # prepare_records (never soft-bind Map invent on create-new).
        destination_column_types=(live_meta_types if live_meta_types else None),
    )
    if map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=namespace or "default",
            target_schema="",
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
            table_name=namespace or "default",
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=f"Vectorization failed: {exc}",
            rejected_details=list(map_rejected),
            rejected_rows=len(map_rejected),
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
            table_name=target,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=dim_err or "embedding dimension unknown — refuse fabricated defaults",
            rejected_details=list(map_rejected) + [{
                "row": "",
                "column": "embedding",
                "target": "values",
                "value": "",
                "reason": dim_err or "no embeddings",
                "policy": "fail",
            }],
            rejected_rows=len(map_rejected) + 1,
        )

    vectors, embed_rejected = build_pinecone_vectors(vector_rows, dimension=dimension)
    rejected = list(map_rejected) + list(embed_rejected)
    if not vectors and rejected:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=target,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=(embed_rejected[0].get("reason") if embed_rejected else None)
            or "all embeddings rejected",
            rejected_details=rejected,
            rejected_rows=len(rejected),
        )
    from connectors.writer_common import reject_on_strict_policy

    strict_error = reject_on_strict_policy(error_policy, rejected, "Pinecone")
    if strict_error:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=target,
            target_schema="",
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
        live_dim = cached_live_dim
        if live_dim is None:
            stats = session.get(
                f"{index_url}/describe_index_stats", headers=hdrs, timeout=15
            )
            if stats.status_code != 200:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=target,
                    target_schema="",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Pinecone describe_index_stats failed ({stats.status_code}) — "
                        "refuse upsert without live index dimension "
                        f"(source embeddings are {dimension}-d)."
                    ),
                    rejected_details=rejected,
                    rejected_rows=len(rejected),
                )
            try:
                live_dim = _pinecone_live_dimension(stats.json())
            except Exception:
                live_dim = None
        if live_dim is None:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=target,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    "Pinecone index dimension unavailable from describe_index_stats — "
                    "refuse upsert with source-only dimension (silent dim invent risk)."
                ),
                rejected_details=rejected,
                rejected_rows=len(rejected),
            )
        if int(live_dim) != int(dimension):
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=target,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Pinecone index dimension is {live_dim}, but embeddings are "
                    f"dimension {dimension} — refuse silent truncate/pad invent. "
                    "Use a matching model or a new index."
                ),
                rejected_details=list(rejected)
                + [
                    {
                        "row": "",
                        "column": "embedding",
                        "target": "values",
                        "value": f"source={dimension} live={live_dim}",
                        "reason": "vector dimension mismatch",
                        "policy": "fail",
                    }
                ],
                rejected_rows=len(rejected) + 1,
            )
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
            rejected_details=rejected,
            rejected_rows=len(rejected),
        )

    _final_abort = reject_on_strict_policy(error_policy, rejected, "Pinecone")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=inserted,
            table_name=target,
            target_schema="",
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
        table_name=target,
        target_schema="",
        checksum="",
        chunks_completed=(inserted + 99) // 100,
        rejected_details=rejected,
        rejected_rows=len(rejected),
        warnings=[r.get("reason") or "" for r in rejected[:10] if r.get("reason")],
        meta=_pinecone_gate8_meta(vectors),
    )


def _pinecone_gate8_meta(vectors: list[dict[str, Any]]) -> dict[str, Any]:
    from connectors.writer_common import vector_gate8_meta

    rows = []
    for v in vectors:
        meta = dict(v.get("metadata") or {}) if isinstance(v.get("metadata"), dict) else {}
        rows.append({"id": v.get("id"), **meta})
    return vector_gate8_meta(rows)
