"""Qdrant vector destination writer — turns rows into upserted points.

Uses the Qdrant REST API (v1.x) so no extra Python client is required.
Points use string UUIDs to avoid integer collisions and support upsert
idempotency. All network calls retry on transient 5xx / 429 errors.
"""

from __future__ import annotations

import importlib.util
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import (
    cell_to_string,
    json_default,
    load_http_json,
    sanitize_json_value,
)
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


def _qdrant_live_vector_size(collection_info: dict[str, Any]) -> int | None:
    """Extract configured vector size from GET /collections/{name} JSON."""
    result = collection_info.get("result") if isinstance(collection_info, dict) else None
    if not isinstance(result, dict):
        result = collection_info if isinstance(collection_info, dict) else {}
    config = result.get("config") if isinstance(result, dict) else None
    params = (config or {}).get("params") if isinstance(config, dict) else None
    vectors = (params or {}).get("vectors") if isinstance(params, dict) else None
    if isinstance(vectors, dict):
        # Named vectors: take first size; unnamed: {"size": N, "distance": ...}.
        if "size" in vectors:
            try:
                return int(vectors["size"])
            except (TypeError, ValueError):
                return None
        for _name, spec in vectors.items():
            if isinstance(spec, dict) and "size" in spec:
                try:
                    return int(spec["size"])
                except (TypeError, ValueError):
                    continue
    return None


def _qdrant_payload_data_type_to_carrier(data_type: Any) -> str:
    """Map Qdrant payload_schema ``data_type`` to a Datawrap logical carrier."""
    raw = str(data_type or "").strip().lower()
    if not raw:
        return ""
    mapping = {
        "integer": "INTEGER",
        "int": "INTEGER",
        "float": "FLOAT",
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
        "keyword": "TEXT",
        "text": "TEXT",
        "datetime": "TIMESTAMPTZ",
        "uuid": "UUID",
        "geo": "JSON",
    }
    return mapping.get(raw, raw.upper())


def _qdrant_live_payload_types(collection_info: dict[str, Any] | None) -> dict[str, str]:
    """Extract payload_schema field carriers from GET /collections/{name} JSON."""
    if not isinstance(collection_info, dict):
        return {}
    result = collection_info.get("result")
    if not isinstance(result, dict):
        result = collection_info
    schema = result.get("payload_schema") if isinstance(result, dict) else None
    if not isinstance(schema, dict):
        return {}
    out: dict[str, str] = {}
    for name, spec in schema.items():
        key = str(name or "").strip()
        if not key:
            continue
        if isinstance(spec, dict):
            carrier = _qdrant_payload_data_type_to_carrier(
                spec.get("data_type") or spec.get("dataType")
            )
        else:
            carrier = _qdrant_payload_data_type_to_carrier(spec)
        # Keep empty carriers so require_physical can refuse incomplete schema.
        out[key] = carrier
        out.setdefault(key.lower(), carrier)
        out.setdefault(key.upper(), carrier)
    return out


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

    from services.vector_embedding import (
        coerce_chunk_index,
        coerce_embedding,
        embedding_reject_reason,
        vector_cell_token,
        vector_fallback_material,
        vector_reject_row_label,
    )

    points: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in vector_rows:
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
        values, err = coerce_embedding(row.get("embedding"), expected_dimension=dimension)
        if err or values is None:
            rejected.append({
                "row": vector_reject_row_label(row),
                "column": "embedding",
                "target": "vector",
                "value": "",
                "reason": embedding_reject_reason(row, err),
                "policy": "quarantine",
            })
            continue
        from services.cdc_identity import is_present_cdc_row_key

        raw_id = row.get("id")
        point_id: str | None = (
            cell_to_string(raw_id).strip() if is_present_cdc_row_key(raw_id) else ""
        )
        if not point_id:
            material = vector_fallback_material(row.get("source_id"), chunk, row.get("content"))
            if material is None:
                rejected.append({
                    "row": "",
                    "column": "id",
                    "target": "id",
                    "value": "",
                    "reason": "missing id — refuse random UUID (non-idempotent)",
                    "policy": "quarantine",
                })
                continue
            digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
            point_id = str(uuid_mod.UUID(digest[:32]))
        from connectors.writer_common import vector_prepare_metadata

        payload = vector_prepare_metadata(
            sanitize_json_value(row.get("metadata") or {}) or {}
        )
        if not isinstance(payload, dict):
            payload = {"_meta": payload}
        payload["content"] = vector_cell_token(row.get("content"))
        payload["source_id"] = vector_cell_token(row.get("source_id"))
        payload["chunk_index"] = chunk
        points.append({
            "id": point_id,
            "vector": sanitize_json_value(values),
            "payload": payload,
        })
    return points, rejected


def _qdrant_points_count(info: dict[str, Any] | None) -> int | None:
    """Physical points, never identity. Used only as a census bound gate."""
    if not isinstance(info, dict):
        return None
    result = info.get("result") if isinstance(info.get("result"), dict) else info
    if not isinstance(result, dict):
        return None
    raw = result.get("points_count")
    if raw is None:
        raw = result.get("pointsCount")
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    return n if n >= 0 else None


def scan_source_ids(
    cfg: Mapping[str, Any],
    *,
    table_name: str,
    max_entities: int = 20_000,
) -> tuple[str, list[Any]]:
    """Dest-engine payload ``source_id`` values. Never ``points_count``.

    Facet aggregation requires a keyword index this writer does not create;
    mutating dest to index during COUNT is forbidden. Scroll + DISTINCT is
    the dest-engine analogue of SQL COUNT(DISTINCT). A truncated scroll is
    never complete.
    """
    table = str(table_name or "").strip()
    if not table:
        return "unmeasured", []
    try:
        session = _requests_session()
        api_key = str(cfg.get("api_key") or cfg.get("password") or cfg.get("username") or "")
        connection_string = str(cfg.get("connection_string") or "").strip()
        base_url = connection_string.rstrip("/") if connection_string else _base_url(
            str(cfg.get("host") or ""),
            int(cfg.get("port") or 6333),
            bool(cfg.get("ssl", False)),
        )
        hdrs = _headers(api_key)
        collection = table or "dataflow_vectors"
        exists = session.get(
            f"{base_url}/collections/{collection}", headers=hdrs, timeout=10
        )
        if exists.status_code == 404:
            return "missing", []
        if exists.status_code != 200:
            return "unmeasured", []
        try:
            info = exists.json()
        except Exception:
            return "unmeasured", []
        physical = _qdrant_points_count(info if isinstance(info, dict) else None)
        cap = int(max_entities)
        if physical is not None and physical > cap:
            return "truncated", []
        if physical == 0:
            return "complete", []
        values: list[Any] = []
        offset: Any = None
        scanned = 0
        page = 256
        while True:
            body: dict[str, Any] = {
                "limit": page,
                "with_payload": ["source_id"],
                "with_vector": False,
            }
            if offset is not None:
                body["offset"] = offset
            resp = session.post(
                f"{base_url}/collections/{collection}/points/scroll",
                data=json.dumps(body, default=json_default),
                headers=hdrs,
                timeout=30,
            )
            if resp.status_code != 200:
                return "unmeasured", []
            payload = load_http_json(resp) if resp.content else {}
            result = payload.get("result") if isinstance(payload, dict) else None
            if not isinstance(result, dict):
                return "unmeasured", []
            points = result.get("points") or []
            if not isinstance(points, list):
                return "unmeasured", []
            for point in points:
                scanned += 1
                if scanned > cap:
                    return "truncated", []
                row_payload = point.get("payload") if isinstance(point, dict) else None
                if isinstance(row_payload, dict):
                    values.append(row_payload.get("source_id"))
                else:
                    values.append(None)
            nxt = result.get("next_page_offset")
            if nxt is None:
                return "complete", values
            offset = nxt
    except Exception:
        return "unmeasured", []


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

    from connectors.writer_common import (
        prepare_records_for_vector_write,
        require_physical_types_for_existing_table,
    )

    pk_cols = list(
        _kwargs.get("destination_pk_columns")
        or _kwargs.get("conflict_columns")
        or []
    ) or None
    collection = table_name or "dataflow_vectors"
    api_key = password or username or ""
    base_url = connection_string if connection_string else _base_url(host, port, ssl)

    studio_live = _kwargs.get("destination_column_types")
    live_payload_types: dict[str, str] = {}
    if isinstance(studio_live, dict):
        live_payload_types.update(
            {str(k): str(v) for k, v in studio_live.items() if k and v}
        )
    mapped_targets = [
        str(m.get("target") or m.get("source") or "").strip()
        for m in (mappings or [])
        if str(m.get("target") or m.get("source") or "").strip()
    ]
    if not mapped_targets:
        mapped_targets = [str(h) for h in (headers or []) if h]

    collection_existed = False
    cached_live_dim: int | None = None
    try:
        session = _requests_session()
        hdrs = _headers(api_key)
        exists = session.get(
            f"{base_url}/collections/{collection}", headers=hdrs, timeout=10
        )
        status = int(exists.status_code)
        if status in {401, 403}:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection,
                target_schema=schema or "",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Qdrant schema probe auth failed ({status}) — "
                    "refuse Map VARCHAR bind (empty→null invent risk)."
                ),
            )
        if status == 200:
            collection_existed = True
            try:
                info = exists.json()
            except Exception:
                info = {}
            cached_live_dim = _qdrant_live_vector_size(
                info if isinstance(info, dict) else {}
            )
            schema_types = _qdrant_live_payload_types(
                info if isinstance(info, dict) else {}
            )
            # Schemaless collections stay Map-tolerant. Typed payload_schema is
            # the invent cliff — Studio may fill; else require_physical.
            if schema_types:
                live_payload_types.update(schema_types)
                mapped_existing = [
                    c
                    for c in mapped_targets
                    if c
                    and (
                        c in schema_types
                        or str(c).lower() in schema_types
                        or str(c).upper() in schema_types
                    )
                ]
                effective = dict(live_payload_types)
                if isinstance(studio_live, dict):
                    for c in mapped_existing:
                        if (
                            effective.get(c)
                            or effective.get(str(c).lower())
                            or effective.get(str(c).upper())
                        ):
                            continue
                        st = str(studio_live.get(c) or "").strip()
                        if st:
                            effective[c] = st
                if mapped_existing:
                    phys_err = require_physical_types_for_existing_table(
                        table_existed=True,
                        physical=effective,
                        dialect_label="Qdrant",
                        target_cols=mapped_existing,
                    )
                    if phys_err:
                        return WriteResult(
                            ok=False,
                            rows_written=0,
                            table_name=collection,
                            target_schema=schema or "",
                            checksum="",
                            chunks_completed=0,
                            error=phys_err,
                        )
                live_payload_types = effective
        elif status == 404:
            if not create_table:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection,
                    target_schema=schema or "",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Qdrant collection '{collection}' is missing and "
                        "create_table is disabled"
                    ),
                )
        else:
            # Unexpected status — never soft-skip dim/existence gates via Studio.
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection,
                target_schema=schema or "",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Qdrant schema probe failed ({status}) — "
                    "refuse Map VARCHAR bind without live collection config."
                ),
            )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=collection,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=f"Qdrant schema probe failed: {exc}",
        )

    records, map_rejected, map_abort = prepare_records_for_vector_write(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        column_types=column_types,
        error_policy=error_policy,
        dest_kind="qdrant",
        destination_pk_columns=pk_cols,
        stream_contracts=_kwargs.get("stream_contracts"),
        contract_primary_key=_kwargs.get("contract_primary_key"),
        label="qdrant",
        destination_column_nullability=_kwargs.get("destination_column_nullability"),
        # Pass Studio/live whenever present — partial Studio fail-closes in
        # prepare_records (never soft-bind Map invent on create-new).
        # Schemaless empty collections with no Studio still Map-bind (None).
        destination_column_types=(
            live_payload_types if live_payload_types else None
        ),
    )
    if map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "",
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
            table_name=table_name,
            target_schema=schema or "",
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
            table_name=table_name,
            target_schema=schema or "",
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
            table_name=table_name or "dataflow_vectors",
            target_schema=schema or "",
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

    points, embed_rejected = build_qdrant_points(vector_rows, dimension=dimension)
    rejected = list(map_rejected) + list(embed_rejected)
    if not points and rejected:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name or "dataflow_vectors",
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=(embed_rejected[0].get("reason") if embed_rejected else None)
            or "all embeddings rejected",
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
    # api_key / base_url already resolved for schema probe above.
    inserted = 0
    try:
        session = _requests_session()
        hdrs = _headers(api_key)
        if collection_existed:
            live_dim = cached_live_dim
            if live_dim is None:
                exists = session.get(
                    f"{base_url}/collections/{collection}", headers=hdrs, timeout=10
                )
                if exists.status_code == 200:
                    try:
                        live_dim = _qdrant_live_vector_size(exists.json())
                    except Exception:
                        live_dim = None
            if live_dim is None:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection,
                    target_schema=schema or "",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Qdrant collection {collection!r} exists but live vector "
                        "size was unavailable — refuse upsert with source-only "
                        "dimension (silent dim invent / reject risk). Re-check "
                        "collection config and retry."
                    ),
                    rejected_details=list(rejected),
                    rejected_rows=len(rejected),
                )
            if int(live_dim) != int(dimension):
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection,
                    target_schema=schema or "",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Qdrant collection {collection!r} vector size is "
                        f"{live_dim}, but embeddings are dimension {dimension} — "
                        "refuse silent truncate/pad invent. Use a matching model "
                        "or a new collection."
                    ),
                    rejected_details=list(rejected)
                    + [
                        {
                            "row": "",
                            "column": "embedding",
                            "target": "vector",
                            "value": f"source={dimension} live={live_dim}",
                            "reason": "vector dimension mismatch",
                            "policy": "fail",
                        }
                    ],
                    rejected_rows=len(rejected) + 1,
                )
        elif not create_table:
            raise RuntimeError(
                f"Qdrant collection '{collection}' is missing and "
                "create_table is disabled"
            )
        else:
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

    _final_abort = reject_on_strict_policy(error_policy, rejected, "Qdrant")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=inserted,
            table_name=collection,
            target_schema=schema or "",
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
        target_schema=schema or "",
        checksum="",
        chunks_completed=(inserted + 99) // 100,
        rejected_details=rejected,
        rejected_rows=len(rejected),
        warnings=[r.get("reason") or "" for r in rejected[:10] if r.get("reason")],
        meta=_qdrant_gate8_meta(points),
    )


def _qdrant_gate8_meta(points: list[dict[str, Any]]) -> dict[str, Any]:
    from connectors.writer_common import vector_gate8_meta

    rows = []
    for p in points:
        payload = dict(p.get("payload") or {}) if isinstance(p.get("payload"), dict) else {}
        rows.append({"id": p.get("id"), **payload})
    return vector_gate8_meta(rows)
