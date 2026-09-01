"""Elasticsearch index reader — search_after pagination for million-row indexes."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.header_union import union_attribute_keys

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


def _exact_es_serializers() -> dict[str, Any]:
    """JSON / NDJSON codecs that do not IEEE-collapse cell numbers.

    elastic_transport.JsonSerializer.json_loads is stdlib ``json.loads``,
    so a long fraction in ``_source`` collapses before ``cell_to_string``.
    ``default(Decimal)`` is ``float(data)`` — a second invent on dump.
    """
    from decimal import Decimal

    from elastic_transport import JsonSerializer, NdjsonSerializer

    from services.value_serializer import json_default, json_loads_exact

    def _json_loads(_self: Any, data: Any) -> Any:
        if isinstance(data, (bytes, bytearray, memoryview)):
            text = bytes(data).decode("utf-8")
        else:
            text = data
        return json_loads_exact(text)

    def _default(self: Any, data: Any) -> Any:
        if isinstance(data, Decimal):
            return json_default(data)
        return JsonSerializer.default(self, data)

    class ExactJsonSerializer(JsonSerializer):
        json_loads = _json_loads
        default = _default

    class ExactNdjsonSerializer(NdjsonSerializer):
        json_loads = _json_loads
        default = _default

    return {
        ExactJsonSerializer.mimetype: ExactJsonSerializer(),
        ExactNdjsonSerializer.mimetype: ExactNdjsonSerializer(),
    }


def _client(cfg: dict[str, Any]):
    from elasticsearch import Elasticsearch

    if cfg.get("connection_string"):
        url = cfg["connection_string"]
    else:
        scheme = "https" if cfg.get("ssl") or int(cfg.get("port") or 9200) == 443 else "http"
        url = f"{scheme}://{cfg.get('host') or 'localhost'}:{cfg.get('port') or 9200}"
    kwargs: dict[str, Any] = {
        "hosts": [url],
        "request_timeout": 60,
        "serializers": _exact_es_serializers(),
    }
    if cfg.get("username") and cfg.get("password"):
        kwargs["basic_auth"] = (cfg["username"], cfg["password"])
    elif cfg.get("api_key"):
        api_key = cfg["api_key"].strip()
        if ":" in api_key:
            key_id, key_value = api_key.split(":", 1)
            kwargs["api_key"] = (key_id, key_value)
        else:
            kwargs["api_key"] = api_key
    return Elasticsearch(**kwargs)


def _cell(value: Any) -> str:
    return cell_to_string(value, preserve_sql_null=True)


def index_native_types(client: Any, index: str, fields: list[str]) -> dict[str, str]:
    """Declared carrier per field, from the index mapping.

    An index *declares* its fields, so its types are a catalog and not a guess.
    A reader that reports none leaves every field the bare ``string``
    placeholder ``_schema_from_batch`` falls back to, and a ``long`` identity
    column then reads back as text: Map sees an exact-name pair whose declared
    destination carrier cannot hold the source integer and demotes ``id → id``
    for review, although nothing about the route is lossy.

    Field types that cannot be read as a single carrier (object/nested
    containers) are left out rather than flattened into an invented one; those
    fields keep the placeholder.
    """
    from connectors.elasticsearch_writer import _fetch_es_physical_types

    wanted = [str(f) for f in (fields or []) if f]
    carriers, exc = _fetch_es_physical_types(client, index, wanted)
    if exc is not None:
        return {}
    # ``_fetch_es_physical_types`` also answers case variants for the writer's
    # own lookups; a schema carries the field's own spelling and nothing else.
    return {name: carriers[name] for name in wanted if name in carriers}


def read_index_batch(
    *,
    cfg: dict[str, Any],
    index: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
    search_after: list | None = None,
) -> tuple[ReadBatch, list | None]:
    del offset  # search_after replaces offset for scale
    client = _client(cfg)
    try:
        if known_total_rows is not None:
            total = known_total_rows
        else:
            count_resp = client.count(index=index)
            total = int(count_resp.get("count", 0))

        body: dict[str, Any] = {
            "size": min(limit, 10000),
            "query": {"match_all": {}},
            "sort": ["_doc"],
        }
        if search_after:
            body["search_after"] = search_after

        resp = client.search(index=index, body=body)
        hits = resp.get("hits", {}).get("hits") or []
        # Preserve ES document identity — _source alone cannot upsert truthfully.
        records: list[dict[str, Any]] = []
        for hit in hits:
            src = dict(hit.get("_source") or {})
            # Reserved identity fields; never overwrite an application field of the same name
            # already present in _source (operator data wins for collision).
            if "_id" not in src:
                src["_id"] = hit.get("_id")
            if "_index" not in src and hit.get("_index") is not None:
                src["_index"] = hit.get("_index")
            if "_routing" not in src and hit.get("_routing") is not None:
                src["_routing"] = hit.get("_routing")
            if "_seq_no" not in src and "_seq_no" in hit:
                src["_seq_no"] = hit.get("_seq_no")
            if "_primary_term" not in src and "_primary_term" in hit:
                src["_primary_term"] = hit.get("_primary_term")
            records.append(src)
        page_keys: list[str] = []
        seen: set[str] = set()
        # Prefer identity columns first for Map / conflict_columns suggestion.
        for preferred in ("_id", "_index", "_routing", "_seq_no", "_primary_term"):
            if any(preferred in r for r in records) and preferred not in seen:
                seen.add(preferred)
                page_keys.append(preferred)
        for rec in records:
            for k in rec.keys():
                if k not in seen:
                    seen.add(k)
                    page_keys.append(k)
        headers = union_attribute_keys(columns, page_keys) if columns else page_keys
        from services.value_serializer import DF_MISSING_SENTINEL

        rows = []
        for r in records:
            row: list[str] = []
            for h in headers:
                if h not in r:
                    row.append(DF_MISSING_SENTINEL)
                else:
                    row.append(_cell(r[h]))
            rows.append(row)
        next_after = hits[-1].get("sort") if hits else None
        meta: dict[str, Any] = {}
        if not search_after:
            # First page only: the schema is stamped from it, and the mapping
            # does not change under a scroll.
            native = index_native_types(client, index, headers)
            if native:
                meta["native_types"] = native
        batch = ReadBatch(
            headers=headers,
            rows=rows,
            offset=0,
            total_rows=total,
            meta=meta or None,
        )
        return batch, next_after
    finally:
        client.close()
