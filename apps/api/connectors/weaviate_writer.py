"""Weaviate vector destination writer — turns rows into upserted objects.

Uses the Weaviate REST API (``/v1``) so no extra Python client is required.
Classes use ``vectorizer: none`` — Datawrap supplies embeddings via
``services/vectorization.py``. Delivery is at-least-once upsert by object id.
"""

from __future__ import annotations

import importlib.util
import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import cell_to_string, json_default, sanitize_json_value
from services.vectorization import vectorize_records

from connectors.writer_common import reject_on_strict_policy, WriteResult as _WriteResult


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
    """Map Datawrap vector rows to Weaviate batch objects (testable, no I/O).

    Returns ``(objects, rejected)``. Missing embeddings are rejected — never
    fabricated as zero vectors. Missing ids → deterministic UUID over
    source_id+chunk+content (retry-safe), else quarantine.
    """
    from services.vector_embedding import (
        coerce_chunk_index,
        coerce_embedding,
        embedding_reject_reason,
    )

    objects: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in vector_rows:
        props = dict(sanitize_json_value(row.get("metadata") or {}) or {})
        props["content"] = row.get("content", "")
        props["source_id"] = cell_to_string(row.get("source_id", ""))
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
        props["chunk_index"] = chunk
        vector, err = coerce_embedding(row.get("embedding"), expected_dimension=dimension)
        if err or vector is None:
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

        raw_id_val = row.get("id")
        if is_present_cdc_row_key(raw_id_val):
            raw_id = cell_to_string(raw_id_val)
        else:
            raw_id = ""
        if not raw_id:
            source = cell_to_string(row.get("source_id", ""))
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


def _weaviate_property_to_carrier(data_types: Any) -> str:
    """Map Weaviate property ``dataType`` list to a Datawrap logical carrier."""
    if isinstance(data_types, str):
        types = [data_types.lower()]
    elif isinstance(data_types, list):
        types = [str(t).lower() for t in data_types if t]
    else:
        return "TEXT"
    if not types:
        return "TEXT"
    primary = types[0]
    return {
        "text": "TEXT",
        "string": "TEXT",
        "int": "INTEGER",
        "number": "FLOAT",
        "boolean": "BOOLEAN",
        "date": "TIMESTAMPTZ",
        "uuid": "UUID",
        "blob": "BINARY",
        "geoCoordinates": "JSON",
        "phoneNumber": "JSON",
        "object": "JSON",
        "text[]": "ARRAY",
        "int[]": "ARRAY",
        "number[]": "ARRAY",
        "boolean[]": "ARRAY",
        "date[]": "ARRAY",
        "uuid[]": "ARRAY",
    }.get(primary, "TEXT")


def _weaviate_live_property_types(schema_doc: dict[str, Any] | None) -> dict[str, str]:
    """Extract property carriers from GET /v1/schema/{class} JSON."""
    if not isinstance(schema_doc, dict):
        return {}
    props = schema_doc.get("properties")
    if not isinstance(props, list):
        return {}
    out: dict[str, str] = {}
    for prop in props:
        if not isinstance(prop, dict):
            continue
        name = str(prop.get("name") or "").strip()
        if not name:
            continue
        carrier = _weaviate_property_to_carrier(prop.get("dataType"))
        out[name] = carrier
        out.setdefault(name.lower(), carrier)
        out.setdefault(name.upper(), carrier)
    return out


_WEAVIATE_OBJECT_PAGE = 100


def _weaviate_object_source_id(obj: Any) -> Any:
    if not isinstance(obj, dict):
        return None
    props = obj.get("properties")
    if not isinstance(props, dict):
        return None
    if "source_id" in props:
        return props.get("source_id")
    return props.get("sourceId")


def _weaviate_aggregate_count(
    session: Any,
    base_url: str,
    hdrs: dict[str, str],
    class_name: str,
) -> int | None:
    """Physical object COUNT — truncation bound only, never dest identity."""
    query = f"{{ Aggregate {{ {class_name} {{ meta {{ count }} }} }} }}"
    resp = session.post(
        f"{base_url}/v1/graphql",
        data=json.dumps({"query": query}),
        headers=hdrs,
        timeout=30,
    )
    if resp.status_code != 200:
        return None
    try:
        body = resp.json()
    except Exception:
        return None
    if not isinstance(body, dict) or body.get("errors"):
        return None
    data = body.get("data")
    if not isinstance(data, dict):
        return None
    agg = data.get("Aggregate")
    if not isinstance(agg, dict):
        return None
    rows = agg.get(class_name)
    if not isinstance(rows, list) or not rows:
        return 0
    meta = rows[0].get("meta") if isinstance(rows[0], dict) else None
    if not isinstance(meta, dict):
        return None
    raw = meta.get("count")
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
    """Dest-engine property ``source_id`` values. Never Aggregate ``meta.count``.

    Weaviate ``Aggregate { meta { count } }`` is physical objects (chunks).
    Identity is DISTINCT ``source_id`` from a complete object listing of the
    class the writer filled. A truncated offset window is never DISTINCT of
    a prefix. Missing class is 0 (create-on-first-write).

    Returns ``(state, values)`` matching Milvus/Qdrant/Pinecone.
    """
    table = str(table_name or "").strip()
    if not table:
        return "unmeasured", []
    try:
        session = _requests_session()
        api_key = str(cfg.get("api_key") or cfg.get("password") or "")
        base_url = _base_url(
            str(cfg.get("host") or ""),
            int(cfg.get("port") or 8080),
            bool(cfg.get("ssl", False)),
            str(cfg.get("connection_string") or ""),
        )
        hdrs = _headers(api_key)
        class_name = _class_name(table)
        schema_resp = session.get(
            f"{base_url}/v1/schema/{class_name}", headers=hdrs, timeout=10
        )
        if schema_resp.status_code == 404:
            return "missing", []
        if schema_resp.status_code != 200:
            return "unmeasured", []
        try:
            schema_doc = schema_resp.json()
        except Exception:
            return "unmeasured", []
        props = _weaviate_live_property_types(
            schema_doc if isinstance(schema_doc, dict) else None
        )
        if "source_id" not in {str(k).lower() for k in props}:
            return "no_field", []
        cap = int(max_entities)
        physical = _weaviate_aggregate_count(session, base_url, hdrs, class_name)
        if physical is None:
            return "unmeasured", []
        if physical == 0:
            return "complete", []
        if physical > cap:
            return "truncated", []
        values: list[Any] = []
        offset = 0
        while offset < physical:
            page = min(_WEAVIATE_OBJECT_PAGE, physical - offset)
            listed = session.get(
                f"{base_url}/v1/objects",
                headers=hdrs,
                params={"class": class_name, "limit": page, "offset": offset},
                timeout=30,
            )
            if listed.status_code != 200:
                return "unmeasured", []
            try:
                body = listed.json()
            except Exception:
                return "unmeasured", []
            objects = body.get("objects") if isinstance(body, dict) else None
            if not isinstance(objects, list):
                return "unmeasured", []
            if not objects:
                break
            for obj in objects:
                values.append(_weaviate_object_source_id(obj))
                if len(values) > cap:
                    return "truncated", []
            offset += len(objects)
        if len(values) > physical:
            return "truncated", []
        return "complete", values
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

    from connectors.writer_common import prepare_records_for_vector_write

    pk_cols = list(
        _kwargs.get("destination_pk_columns")
        or _kwargs.get("conflict_columns")
        or []
    ) or None
    class_name = _class_name(table_name or database or "DataflowChunk")
    key = api_key or password or username or ""
    base_url = _base_url(host, port, ssl, connection_string)

    # Probe live class properties before Map bind so text≠int invent is closed.
    live_prop_types: dict[str, str] = {}
    studio_live = _kwargs.get("destination_column_types")
    if isinstance(studio_live, dict):
        live_prop_types.update(
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

    session = _requests_session()
    hdrs = _headers(key)
    class_existed = False
    try:
        class_resp = session.get(
            f"{base_url}/v1/schema/{class_name}", headers=hdrs, timeout=10
        )
        status = int(class_resp.status_code)
        if status in {401, 403}:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=class_name,
                target_schema=schema or "",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Weaviate schema probe auth failed ({status}) — "
                    "refuse Map VARCHAR bind (empty→null invent risk)."
                ),
            )
        if status == 200:
            class_existed = True
            try:
                schema_types = _weaviate_live_property_types(class_resp.json())
            except Exception:
                schema_types = {}
            if not schema_types and not studio_typed_all:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=class_name,
                    target_schema=schema or "",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Weaviate class {class_name!r} exists but live property "
                        "types were unavailable — refuse Map VARCHAR bind "
                        "(empty→null invent risk). Re-run schema introspect."
                    ),
                )
            # Live schema wins over Studio stamps for overlapping properties.
            live_prop_types.update(schema_types)
            # Partial class properties: Studio may fill gaps (ES/Mongo bar —
            # intentional Studio stamps beat Map VARCHAR invent on missing
            # props / dynamic schema). Else require_physical refuses.
            from connectors.writer_common import require_physical_types_for_existing_table

            effective = dict(live_prop_types)
            if isinstance(studio_live, dict):
                for c in mapped_targets:
                    if (
                        effective.get(c)
                        or effective.get(str(c).lower())
                        or effective.get(str(c).upper())
                    ):
                        continue
                    st = str(studio_live.get(c) or "").strip()
                    if st:
                        effective[c] = st
            phys_err = require_physical_types_for_existing_table(
                table_existed=True,
                physical=effective,
                dialect_label="Weaviate",
                target_cols=mapped_targets,
            )
            if phys_err:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=class_name,
                    target_schema=schema or "",
                    checksum="",
                    chunks_completed=0,
                    error=phys_err,
                )
            live_prop_types = effective
        elif status == 404:
            if not create_table:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=class_name,
                    target_schema=schema or "",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Weaviate class '{class_name}' is missing and "
                        "create_table is disabled"
                    ),
                )
        else:
            if not studio_typed_all:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=class_name,
                    target_schema=schema or "",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Weaviate schema probe failed ({status}) — "
                        "refuse Map VARCHAR bind without live property types."
                    ),
                )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=class_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=f"Weaviate schema probe failed: {exc}",
        )

    records, map_rejected, map_abort = prepare_records_for_vector_write(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        column_types=column_types,
        error_policy=error_policy,
        dest_kind="weaviate",
        destination_pk_columns=pk_cols,
        stream_contracts=_kwargs.get("stream_contracts"),
        contract_primary_key=_kwargs.get("contract_primary_key"),
        label="weaviate",
        destination_column_nullability=_kwargs.get("destination_column_nullability"),
        # Pass Studio/live whenever present — partial Studio fail-closes in
        # prepare_records (never soft-bind Map invent on create-new).
        destination_column_types=(
            live_prop_types if live_prop_types else None
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
            table_name=class_name,
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
            table_name=class_name,
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
            table_name=class_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=dim_err or "embedding dimension unknown — refuse fabricated defaults",
            rejected_details=list(map_rejected) + [{
                "row": "",
                "column": "embedding",
                "target": "vector",
                "value": "",
                "reason": dim_err or "no embeddings",
                "policy": "fail",
            }],
            rejected_rows=len(map_rejected) + 1,
        )

    objects, embed_rejected = build_weaviate_objects(
        vector_rows, class_name=class_name, dimension=dimension
    )
    rejected = list(map_rejected) + list(embed_rejected)
    if not objects and rejected:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=class_name,
            target_schema=schema or "",
            checksum="",
            chunks_completed=0,
            error=(embed_rejected[0].get("reason") if embed_rejected else None)
            or "all embeddings rejected",
            rejected_details=rejected,
            rejected_rows=len(rejected),
        )
    from connectors.writer_common import transform_error_policy

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
        if not class_existed:
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
        # Re-check FAIL_JOB / strict after mid-write per-object rejects.
        _final_abort = reject_on_strict_policy(policy, rejected, "Weaviate")
        if _final_abort:
            return WriteResult(
                ok=False,
                rows_written=inserted,
                table_name=class_name,
                target_schema=schema or "",
                checksum="",
                chunks_completed=(inserted + 99) // 100,
                error=_final_abort,
                rejected_details=rejected,
                rejected_rows=len(rejected),
                warnings=[r.get("reason") or "" for r in rejected[:10] if r.get("reason")],
            )
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
            rejected_rows=len(rejected),
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
        meta=_weaviate_gate8_meta(objects),
    )


def _weaviate_gate8_meta(objects: list[dict[str, Any]]) -> dict[str, Any]:
    from connectors.writer_common import vector_gate8_meta

    rows = []
    for obj in objects:
        props = dict(obj.get("properties") or {}) if isinstance(obj.get("properties"), dict) else {}
        rows.append({"id": obj.get("id"), **props})
    return vector_gate8_meta(rows)
