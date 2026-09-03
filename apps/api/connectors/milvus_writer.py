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
from collections.abc import Iterator, Mapping
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


def _milvus_schema_text(value: Any, cap: int) -> str:
    """Dest-canonical schema field text. ``or ''`` wiped integer 0 / False."""
    from services.value_serializer import present_cell_text

    text = present_cell_text(value)
    return (text or "")[:cap]


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
        vector_cell_token,
        vector_fallback_material,
        vector_reject_row_label,
    )

    entities: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in vector_rows:
        from connectors.writer_common import vector_prepare_metadata

        meta = vector_prepare_metadata(
            sanitize_json_value(row.get("metadata") or {}) or {}
        )
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
            material = vector_fallback_material(row.get("source_id"), chunk, row.get("content"))
            if material is None:
                rejected.append({
                    "row": "",
                    "column": "id",
                    "target": "id",
                    "value": "",
                    "reason": "missing id — refuse fabricated 'missing' identity",
                    "policy": "quarantine",
                })
                continue
            digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
            entity_id = str(uuid.UUID(digest[:32]))
        entity: dict[str, Any] = {
            "id": entity_id,
            "vector": sanitize_json_value(values),
            "content": vector_cell_token(row.get("content"))[:65000],
            "source_id": vector_cell_token(row.get("source_id"))[:256],
            "chunk_index": chunk,
            "filename": _milvus_schema_text(meta.get("filename"), 512),
            "page": _milvus_schema_text(meta.get("page"), 64),
            "heading": _milvus_schema_text(meta.get("heading"), 1024),
            "element_type": _milvus_schema_text(
                meta["element_type"] if "element_type" in meta else row.get("element_type"),
                128,
            ),
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


def _milvus_dtype_to_carrier(data_type: Any) -> str:
    """Map Milvus field ``dataType`` to a Datawrap logical carrier.

    Vector fields return ``""`` (not Map-bound). Unknown empty → ``""`` so
    require_physical can refuse incomplete describe (never invent VARCHAR).
    """
    raw = str(data_type or "").strip()
    if not raw:
        return ""
    u = raw.upper().replace(" ", "")
    if "VECTOR" in u:
        return ""
    mapping = {
        "VARCHAR": "VARCHAR",
        "STRING": "TEXT",
        "INT64": "BIGINT",
        "INT32": "INTEGER",
        "INT16": "SMALLINT",
        "INT8": "SMALLINT",
        "FLOAT": "FLOAT",
        "DOUBLE": "DOUBLE",
        "BOOL": "BOOLEAN",
        "BOOLEAN": "BOOLEAN",
        "JSON": "JSON",
        "ARRAY": "ARRAY",
    }
    return mapping.get(u, u)


def _milvus_describe_data(
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection_name: str,
    db_name: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {"collectionName": collection_name}
    if db_name:
        payload["dbName"] = db_name
    resp = session.post(
        f"{base_url}/v2/vectordb/collections/describe",
        data=json.dumps(payload),
        headers=headers,
        timeout=15,
    )
    body = resp.json() if resp.content else {}
    if not _ok_response(body if isinstance(body, dict) else {}, resp.status_code):
        return {}
    data = body.get("data") if isinstance(body, dict) else None
    return data if isinstance(data, dict) else {}


def milvus_pk_info_from_describe_data(data: dict[str, Any]) -> tuple[str, str]:
    """Primary-key field name and Milvus data type from a describe payload."""
    fields = data.get("fields") or data.get("schema", {}).get("fields") or []
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


def _milvus_describe_collection(
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection_name: str,
    db_name: str = "",
) -> tuple[dict[str, str], int | None]:
    """POST /collections/describe → (non-vector field carriers, vector dim)."""
    data = _milvus_describe_data(
        session, base_url, headers, collection_name, db_name=db_name
    )
    if not data:
        return {}, None
    fields = data.get("fields") or data.get("schema", {}).get("fields") or []
    if not isinstance(fields, list):
        return {}, None
    carriers: dict[str, str] = {}
    live_dim: int | None = None
    for field in fields:
        if not isinstance(field, dict):
            continue
        name = str(
            field.get("fieldName") or field.get("name") or ""
        ).strip()
        dtype = field.get("dataType") or field.get("type") or ""
        dtype_u = str(dtype).upper()
        if "VECTOR" in dtype_u:
            params = field.get("elementTypeParams") or field.get("params") or {}
            if isinstance(params, dict):
                raw = params.get("dim")
                try:
                    dim = int(raw) if raw is not None else 0
                except (TypeError, ValueError):
                    dim = 0
                if dim > 0:
                    live_dim = dim
            continue
        if not name:
            continue
        carrier = _milvus_dtype_to_carrier(dtype)
        # Keep empty carriers so require_physical can refuse incomplete describe
        # (never drop the field name and soft-skip the invent cliff).
        carriers[name] = carrier
        carriers.setdefault(name.lower(), carrier)
        carriers.setdefault(name.upper(), carrier)
    return carriers, live_dim


def _milvus_live_vector_dim(
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection_name: str,
    db_name: str = "",
) -> int | None:
    """POST /collections/describe → FloatVector elementTypeParams.dim."""
    _carriers, dim = _milvus_describe_collection(
        session, base_url, headers, collection_name, db_name=db_name
    )
    return dim


def _milvus_live_field_types(
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection_name: str,
    db_name: str = "",
) -> dict[str, str]:
    """Non-vector field carriers from POST /collections/describe."""
    carriers, _dim = _milvus_describe_collection(
        session, base_url, headers, collection_name, db_name=db_name
    )
    return carriers


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


# REST query offset+limit must stay below this (Milvus v2 entities/query).
# Identity COPY does not use offset past this window: PK keyset / INT range
# split keeps every request at offset 0.
_MILVUS_QUERY_WINDOW = 16384
_MILVUS_INT64_MIN = -9223372036854775808
_MILVUS_INT64_MAX = 9223372036854775807
_MILVUS_QUERY_PAGE = 1000


class MilvusUnorderedPkPage(Exception):
    """Query page was not strictly increasing on PK — refuse keyset skip/dup."""


def _milvus_ident(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "", (name or "").strip())
    if not cleaned:
        raise ValueError("Milvus field name empty after sanitise")
    return cleaned


def milvus_pk_is_int(pk_type: str) -> bool:
    return "INT" in str(pk_type or "").upper()


def milvus_quote_pk_expr(value: Any, *, integer: bool) -> str:
    if integer:
        return str(int(value))
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def milvus_all_pk_filter(pk_name: str, pk_type: str) -> str:
    """Always-true PK predicate — includes negative Int64 (never ``pk >= 0``)."""
    ident = _milvus_ident(pk_name)
    if milvus_pk_is_int(pk_type):
        return f"{ident} >= {_MILVUS_INT64_MIN}"
    return f'{ident} != ""'


def milvus_pk_gt_filter(pk_name: str, pk_type: str, last: Any) -> str:
    ident = _milvus_ident(pk_name)
    return f"{ident} > {milvus_quote_pk_expr(last, integer=milvus_pk_is_int(pk_type))}"


def milvus_pk_range_filter(pk_name: str, lo: int, hi: int) -> str:
    ident = _milvus_ident(pk_name)
    return f"{ident} >= {int(lo)} and {ident} <= {int(hi)}"


def milvus_normalize_pk(value: Any, *, integer: bool) -> Any:
    if integer:
        return int(value)
    return str(value)


def milvus_pks_strictly_increasing(values: list[Any], *, integer: bool) -> bool:
    prev: Any = None
    for raw in values:
        if raw is None or raw == "":
            return False
        cur = milvus_normalize_pk(raw, integer=integer)
        if prev is not None and not (cur > prev):
            return False
        prev = cur
    return True


def milvus_int_pk_split_windows(
    lo: int,
    hi: int,
    count_in_range: Callable[[int, int], int],
    page_size: int,
    *,
    _depth: int = 0,
) -> Iterator[tuple[int, int, int]]:
    """Inclusive ``[lo, hi]`` windows each with ``0 < count <= page_size``.

    Binary range split — pymilvus QueryIterator's REST-compatible form.
    Offset stays 0 so ``limit + offset`` never hits ``_MILVUS_QUERY_WINDOW``.
    Int64 depth is ≤ 64; a single PK with count > page_size fails closed.
    """
    if lo > hi:
        return
    if _depth > 64:
        raise ValueError("Milvus INT PK range split exceeded Int64 depth")
    size = int(page_size)
    if size <= 0:
        raise ValueError("Milvus query page_size must be > 0")
    n = int(count_in_range(int(lo), int(hi)))
    if n <= 0:
        return
    if n <= size:
        yield (int(lo), int(hi), n)
        return
    if lo == hi:
        raise ValueError(
            f"Milvus PK {lo} reports count {n} > page {size}; refuse silent loss"
        )
    mid = lo + (hi - lo) // 2
    yield from milvus_int_pk_split_windows(
        lo, mid, count_in_range, size, _depth=_depth + 1
    )
    yield from milvus_int_pk_split_windows(
        mid + 1, hi, count_in_range, size, _depth=_depth + 1
    )


def _milvus_with_db(payload: dict[str, Any], db_name: str) -> dict[str, Any]:
    if db_name:
        payload = dict(payload)
        payload["dbName"] = db_name
    return payload


def _milvus_order_by_rejected(body: Any, status_code: int) -> bool:
    if _ok_response(body if isinstance(body, dict) else {}, status_code):
        return False
    text = json.dumps(body if body is not None else {}, default=str).lower()
    return any(token in text for token in ("orderby", "order_by", "order-by", "sort"))


def milvus_query_entities_page(
    *,
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection: str,
    db_name: str,
    filt: str,
    output_fields: list[str],
    limit: int,
    order_by_pk: str | None = None,
) -> list[dict[str, Any]]:
    """One entities/query page at offset 0 (never walks the 16,384 window)."""
    payload: dict[str, Any] = {
        "collectionName": collection,
        "filter": filt,
        "outputFields": output_fields,
        "limit": int(limit),
        "offset": 0,
    }
    if order_by_pk:
        payload["orderByFields"] = [f"{_milvus_ident(order_by_pk)}:asc"]
    payload = _milvus_with_db(payload, db_name)
    resp = session.post(
        f"{base_url}/v2/vectordb/entities/query",
        data=json.dumps(payload),
        headers=headers,
        timeout=60,
    )
    body = load_http_json(resp) if resp.content else {}
    if order_by_pk and _milvus_order_by_rejected(body, resp.status_code):
        raise MilvusUnorderedPkPage("orderByFields rejected by this Milvus")
    if not _ok_response(body if isinstance(body, dict) else {}, resp.status_code):
        raise ValueError(
            f"Milvus query failed: {resp.status_code} {str(body)[:200]}"
        )
    rows = body.get("data") if isinstance(body, dict) else None
    if not isinstance(rows, list):
        raise ValueError("Milvus query returned no data")
    return [row for row in rows if isinstance(row, dict)]


def milvus_count_in_filter(
    *,
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection: str,
    db_name: str,
    filt: str,
) -> int:
    payload = _milvus_with_db(
        {
            "collectionName": collection,
            "filter": filt,
            "outputFields": ["count(*)"],
        },
        db_name,
    )
    resp = session.post(
        f"{base_url}/v2/vectordb/entities/query",
        data=json.dumps(payload),
        headers=headers,
        timeout=30,
    )
    body = resp.json() if resp.content else {}
    if not _ok_response(body if isinstance(body, dict) else {}, resp.status_code):
        raise ValueError(
            f"Milvus count(*) failed: {resp.status_code} {str(body)[:200]}"
        )
    n = _milvus_count_star(body.get("data") if isinstance(body, dict) else body)
    if n is None:
        raise ValueError("Milvus count(*) unmeasured")
    return int(n)


def iter_milvus_query_pages(
    *,
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection: str,
    db_name: str,
    pk_name: str,
    pk_type: str,
    output_fields: list[str],
    page_size: int = _MILVUS_QUERY_PAGE,
) -> Iterator[list[dict[str, Any]]]:
    """Yield entity pages covering the collection without offset pagination.

    Prefer PK keyset (``pk > last`` + ``orderByFields``). INT PK falls back to
    binary range split when this Milvus ignores/rejects ORDER BY. VarChar
    collections without ordered pages fail closed rather than skip keys.
    """
    ident = _milvus_ident(pk_name)
    integer = milvus_pk_is_int(pk_type)
    page = max(1, min(int(page_size), _MILVUS_QUERY_PAGE))
    base_filt = milvus_all_pk_filter(ident, pk_type)
    keyset = _iter_milvus_pk_keyset(
        session=session,
        base_url=base_url,
        headers=headers,
        collection=collection,
        db_name=db_name,
        pk_name=ident,
        integer=integer,
        base_filt=base_filt,
        output_fields=output_fields,
        page_size=page,
    )
    try:
        first = next(keyset)
    except StopIteration:
        return
    except MilvusUnorderedPkPage:
        if integer:
            yield from _iter_milvus_int_ranges(
                session=session,
                base_url=base_url,
                headers=headers,
                collection=collection,
                db_name=db_name,
                pk_name=ident,
                output_fields=output_fields,
                page_size=page,
            )
            return
        raise ValueError(
            "Milvus VarChar PK COPY requires ordered query (orderByFields); "
            "refuse offset past the 16,384 window"
        )
    yield first
    try:
        yield from keyset
    except MilvusUnorderedPkPage as exc:
        raise ValueError(
            "Milvus PK keyset lost order mid-copy; refuse silent skip"
        ) from exc


def _iter_milvus_pk_keyset(
    *,
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection: str,
    db_name: str,
    pk_name: str,
    integer: bool,
    base_filt: str,
    output_fields: list[str],
    page_size: int,
) -> Iterator[list[dict[str, Any]]]:
    last: Any = None
    seen: set[Any] = set()
    while True:
        filt = base_filt
        if last is not None:
            gt = milvus_pk_gt_filter(pk_name, "Int64" if integer else "VarChar", last)
            filt = f"({base_filt}) and ({gt})"
        rows = milvus_query_entities_page(
            session=session,
            base_url=base_url,
            headers=headers,
            collection=collection,
            db_name=db_name,
            filt=filt,
            output_fields=output_fields,
            limit=page_size,
            order_by_pk=pk_name,
        )
        if not rows:
            break
        pks = [row.get(pk_name) for row in rows]
        if any(pk is None or pk == "" for pk in pks):
            raise ValueError("Milvus query page missing PK; refuse silent loss")
        if not milvus_pks_strictly_increasing(pks, integer=integer):
            raise MilvusUnorderedPkPage("query page not strictly increasing on PK")
        for pk in pks:
            key = milvus_normalize_pk(pk, integer=integer)
            if key in seen:
                raise ValueError("Milvus keyset returned duplicate PK; refuse silent dup")
            seen.add(key)
        yield rows
        if len(rows) < page_size:
            break
        last = milvus_normalize_pk(pks[-1], integer=integer)


def _iter_milvus_int_ranges(
    *,
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection: str,
    db_name: str,
    pk_name: str,
    output_fields: list[str],
    page_size: int,
) -> Iterator[list[dict[str, Any]]]:
    def _count(lo: int, hi: int) -> int:
        return milvus_count_in_filter(
            session=session,
            base_url=base_url,
            headers=headers,
            collection=collection,
            db_name=db_name,
            filt=milvus_pk_range_filter(pk_name, lo, hi),
        )

    for lo, hi, n in milvus_int_pk_split_windows(
        _MILVUS_INT64_MIN,
        _MILVUS_INT64_MAX,
        _count,
        page_size,
    ):
        rows = milvus_query_entities_page(
            session=session,
            base_url=base_url,
            headers=headers,
            collection=collection,
            db_name=db_name,
            filt=milvus_pk_range_filter(pk_name, lo, hi),
            output_fields=output_fields,
            limit=n,
            order_by_pk=None,
        )
        if len(rows) != n:
            raise ValueError(
                f"Milvus range [{lo}, {hi}] count(*) {n} != query {len(rows)}; "
                "refuse partial copy"
            )
        yield rows


def _milvus_count_star(data: Any) -> int | None:
    """Parse entities/query ``count(*)`` — physical entities, never identity."""
    if isinstance(data, int):
        return data if data >= 0 else None
    if isinstance(data, dict):
        for key in ("rowCount", "row_count", "count(*)", "count"):
            if key in data:
                try:
                    n = int(data[key])
                except (TypeError, ValueError):
                    return None
                return n if n >= 0 else None
        data = data.get("data")
    if isinstance(data, list) and data:
        row = data[0]
        if isinstance(row, dict):
            for key, raw in row.items():
                compact = str(key).lower().replace(" ", "")
                if "count" in compact:
                    try:
                        n = int(raw)
                    except (TypeError, ValueError):
                        return None
                    return n if n >= 0 else None
        if isinstance(row, (int, float)):
            n = int(row)
            return n if n >= 0 else None
    return None


def _entity_source_id(entity: Any) -> Any:
    if not isinstance(entity, dict):
        return None
    if "source_id" in entity:
        return entity.get("source_id")
    return entity.get("sourceId")


def scan_source_ids(
    cfg: Mapping[str, Any],
    *,
    table_name: str,
    max_entities: int = 20_000,
) -> tuple[str, list[Any]]:
    """Dest-engine ``source_id`` values. Never ``num_entities`` / ``rowCount``.

    Returns ``(state, values)``:

    * ``missing`` — collection absent (create-on-first-write → identity 0)
    * ``no_field`` — live schema has no ``source_id`` (unmeasured)
    * ``truncated`` — physical cardinality exceeds the REST/census bound
    * ``complete`` — every entity's ``source_id`` is in ``values``
    * ``unmeasured`` — transport / describe / query failure

    A prefix scan is never ``complete``. Callers DISTINCT only on complete.
    """
    table = str(table_name or "").strip()
    if not table:
        return "unmeasured", []
    try:
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
        hdrs = _headers(token)
        collection = _collection_name(table or str(cfg.get("database") or "") or "dataflow_chunks")
        db_name = str(cfg.get("database") or cfg.get("schema") or "").strip()
        if db_name.lower() in {"", "test_db", "default", "public"}:
            db_name = ""
        if not _has_collection(session, base_url, hdrs, collection, db_name=db_name):
            return "missing", []
        carriers, _dim = _milvus_describe_collection(
            session, base_url, hdrs, collection, db_name=db_name
        )
        if not carriers:
            return "unmeasured", []
        if "source_id" not in {str(k).lower() for k in carriers}:
            return "no_field", []
        cap = int(max_entities)
        describe = _milvus_describe_data(
            session, base_url, hdrs, collection, db_name=db_name
        )
        pk_name, pk_type = milvus_pk_info_from_describe_data(describe)
        filt = milvus_all_pk_filter(pk_name, pk_type)
        physical = milvus_count_in_filter(
            session=session,
            base_url=base_url,
            headers=hdrs,
            collection=collection,
            db_name=db_name,
            filt=filt,
        )
        if physical == 0:
            return "complete", []
        if physical > cap:
            return "truncated", []
        values: list[Any] = []
        for page in iter_milvus_query_pages(
            session=session,
            base_url=base_url,
            headers=hdrs,
            collection=collection,
            db_name=db_name,
            pk_name=pk_name,
            pk_type=pk_type,
            output_fields=["source_id", pk_name],
        ):
            for row in page:
                values.append(_entity_source_id(row))
                if len(values) > cap:
                    return "truncated", []
        if len(values) != physical:
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

    from connectors.writer_common import (
        prepare_records_for_vector_write,
        require_physical_types_for_existing_table,
    )

    pk_cols = list(
        _kwargs.get("destination_pk_columns")
        or _kwargs.get("conflict_columns")
        or []
    ) or None

    # Probe live collection schema before Map bind (Weaviate/ES bar).
    studio_live = _kwargs.get("destination_column_types")
    live_field_types: dict[str, str] = {}
    if isinstance(studio_live, dict):
        live_field_types.update(
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

    token = _auth_token(api_key=api_key, username=username, password=password)
    base_url = _base_url(host, port, ssl, connection_string)
    collection_existed = False
    cached_live_dim: int | None = None
    try:
        session = _requests_session()
        hdrs = _headers(token)
        collection_existed = _has_collection(
            session, base_url, hdrs, collection, db_name=db_name
        )
        if collection_existed:
            schema_types, cached_live_dim = _milvus_describe_collection(
                session, base_url, hdrs, collection, db_name=db_name
            )
            if not schema_types and not studio_typed_all:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection,
                    target_schema=db_name,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Milvus collection {collection!r} exists but live field "
                        "types were unavailable — refuse Map VARCHAR bind "
                        "(empty→null invent risk). Re-run schema introspect."
                    ),
                )
            # Live schema wins over Studio for overlapping fields.
            live_field_types.update(schema_types)
            # Gate only fields already on the collection (additive Map cols
            # become unused metadata — fixed schema enableDynamicField=False).
            primary_existing = set(schema_types.keys())
            mapped_existing = [
                c
                for c in mapped_targets
                if c
                and (
                    c in primary_existing
                    or str(c).lower() in primary_existing
                    or str(c).upper() in primary_existing
                )
            ]
            effective = dict(live_field_types)
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
                    dialect_label="Milvus",
                    target_cols=mapped_existing,
                )
                if phys_err:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=collection,
                        target_schema=db_name,
                        checksum="",
                        chunks_completed=0,
                        error=phys_err,
                    )
            live_field_types = effective
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=collection,
            target_schema=db_name,
            checksum="",
            chunks_completed=0,
            error=f"Milvus schema probe failed: {exc}",
        )

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
        destination_column_nullability=_kwargs.get("destination_column_nullability"),
        # Pass Studio/live whenever present — partial Studio fail-closes in
        # prepare_records (never soft-bind Map invent on create-new).
        destination_column_types=(
            live_field_types if live_field_types else None
        ),
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
        from connectors.writer_common import refuse_empty_vectorization

        empty_err = refuse_empty_vectorization(records=records, data_rows=data_rows)
        if empty_err:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection,
                target_schema=db_name,
                checksum="",
                chunks_completed=0,
                error=empty_err,
                rejected_details=list(map_rejected),
                rejected_rows=len(map_rejected),
            )
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
    rejected: list[dict[str, Any]] = list(map_rejected)
    try:
        session = _requests_session()
        hdrs = _headers(token)
        if not collection_existed:
            if not create_table:
                raise RuntimeError(
                    f"Milvus collection '{collection}' is missing and "
                    "create_table is disabled"
                )
            _ensure_collection(
                session, base_url, hdrs, collection, dimension, db_name=db_name
            )
            live_dim = None
        else:
            live_dim = cached_live_dim
            if live_dim is None:
                live_dim = _milvus_live_vector_dim(
                    session, base_url, hdrs, collection, db_name=db_name
                )
            if live_dim is None:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection,
                    target_schema=db_name,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Milvus collection {collection!r} exists but live vector "
                        "dimension was unavailable — refuse upsert with source-only "
                        "dimension (silent dim invent risk). Re-check collection "
                        "schema and retry."
                    ),
                    rejected_details=list(rejected),
                    rejected_rows=len(rejected),
                )
            if int(live_dim) != int(dimension):
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection,
                    target_schema=db_name,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Milvus collection {collection!r} vector dim is {live_dim}, "
                        f"but embeddings are dimension {dimension} — refuse silent "
                        "truncate/pad invent. Use a matching model or a new collection."
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
