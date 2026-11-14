"""Read the stored ``_df_lsn`` of key-addressed destinations (CLM-2).

``connectors.table_manager._fetch_pk_lsn_map`` guards a CDC delete: a key whose
stored ``_df_lsn`` is newer than the delete keeps its row. Redis,
Elasticsearch/OpenSearch and DynamoDB had no such read, so every CDC run into
them failed at the first source DELETE after the update before it had already
applied. These readers address each key exactly as the writer and the delete
path do (``services.dest_precount``): the same Redis key, the same ``_id``, the
same DynamoDB key attributes. A key with no stored row maps to ``None``.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence


def _tuples(keys: Sequence[str], width: int) -> list[tuple[str, tuple[Any, ...]]]:
    from services.row_conservation import parse_delete_keys

    out: list[tuple[str, tuple[Any, ...]]] = []
    for raw in keys:
        parsed = parse_delete_keys([raw], width)
        if parsed:
            out.append((str(raw), parsed[0]))
    return out


def redis_lsn_map(
    cfg: Mapping[str, Any], *, prefix: str, cols: list[str], keys: Sequence[str], lsn_column: str
) -> dict[str, Any]:
    from connectors.redis_reader import (
        _redis_client,
        load_redis_json_doc,
        redis_key_for,
        redis_key_format,
    )
    from services.value_serializer import present_cell_text

    existing: dict[str, Any] = {str(k): None for k in keys}
    pairs = _tuples(keys, len(cols))
    if not pairs or not prefix:
        return existing
    fmt = redis_key_format(dict(cfg))
    names = [
        redis_key_for(prefix, "|".join(present_cell_text(p) or "" for p in tup), key_format=fmt)
        for _raw, tup in pairs
    ]
    client = _redis_client(dict(cfg))
    raws = client.mget(names)
    for (raw_key, _tup), payload in zip(pairs, raws or []):
        if payload is None:
            continue
        doc = load_redis_json_doc(payload)
        if not isinstance(doc, dict):
            raise RuntimeError(
                f"Redis key for {raw_key!r} holds no JSON document; its stored "
                "change position can't be read"
            )
        existing[raw_key] = doc.get(lsn_column)
    return existing


def elasticsearch_lsn_map(
    cfg: Mapping[str, Any], *, index: str, cols: list[str], keys: Sequence[str], lsn_column: str
) -> dict[str, Any]:
    from connectors.elasticsearch_reader import _client
    from services.dest_precount import _elasticsearch_doc_id

    existing: dict[str, Any] = {str(k): None for k in keys}
    pairs = [(raw, _elasticsearch_doc_id(tup)) for raw, tup in _tuples(keys, len(cols))]
    pairs = [(raw, doc_id) for raw, doc_id in pairs if doc_id]
    if not pairs or not index:
        return existing
    client = _client(dict(cfg))
    try:
        if not client.indices.exists(index=index):
            return existing
        resp = client.mget(index=index, ids=[doc_id for _raw, doc_id in pairs])
        docs = resp.get("docs") if isinstance(resp, Mapping) else getattr(resp, "body", {}).get("docs")
        by_id = {str(d.get("_id")): d for d in (docs or []) if isinstance(d, Mapping)}
        for raw, doc_id in pairs:
            doc = by_id.get(doc_id)
            if doc and doc.get("found"):
                existing[raw] = (doc.get("_source") or {}).get(lsn_column)
        return existing
    finally:
        client.close()


def dynamodb_lsn_map(
    cfg: Mapping[str, Any], *, table_name: str, cols: list[str], keys: Sequence[str], lsn_column: str
) -> dict[str, Any]:
    from connectors.aws_common import boto3_client
    from connectors.dynamodb_reader import describe_key_schema
    from connectors.dynamodb_writer import _coerce_dynamo_cell, _to_attr

    existing: dict[str, Any] = {str(k): None for k in keys}
    pairs = _tuples(keys, len(cols))
    table = (table_name or "").strip()
    if not pairs or not table:
        return existing
    schema = describe_key_schema(dict(cfg), table)
    declared = {
        str(item.get("name") or "").lower(): str(item.get("attr_type") or "VARCHAR")
        for item in schema
    }
    if sorted(declared) != sorted(c.lower() for c in cols):
        raise RuntimeError(
            f"DynamoDB table {table} is keyed on {sorted(declared)}, not the CDC key "
            f"{cols}; its stored change positions can't be read by that key"
        )
    letters = {n: {"VARCHAR": "S", "DECIMAL": "N", "BINARY": "B"}.get(t, "S") for n, t in declared.items()}
    client = boto3_client("dynamodb", dict(cfg))
    for raw, tup in pairs:
        key = {
            name: _to_attr(
                _coerce_dynamo_cell(
                    value, col=name, logical_type=declared.get(name.lower(), "VARCHAR"),
                    key_types={name: letters.get(name.lower(), "S")},
                ),
                declared.get(name.lower(), "VARCHAR"),
            )
            for name, value in zip(cols, tup)
        }
        item = (client.get_item(
            TableName=table, Key=key, ProjectionExpression="#l",
            ExpressionAttributeNames={"#l": lsn_column}, ConsistentRead=True,
        ) or {}).get("Item")
        if item is None:
            continue
        attr = item.get(lsn_column)
        existing[raw] = next(iter(attr.values())) if isinstance(attr, Mapping) and attr else None
    return existing


__all__ = ["dynamodb_lsn_map", "elasticsearch_lsn_map", "redis_lsn_map"]
