"""Elasticsearch index writer — bulk indexing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import json_default

from connectors.elasticsearch_reader import _client
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    build_mapped_rows,
    resolve_target_columns,
    row_checksum,
)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "elasticsearch-py"


def _to_es_value(value: Any, source_type: str) -> Any:
    """Convert transform-engine values to Elasticsearch-native JSON shapes."""
    if value is None:
        return None
    upper = source_type.upper()
    if upper in {"DECIMAL", "NUMERIC"}:
        try:
            return float(value)
        except (ValueError, TypeError):
            return value
    if upper in {"JSON", "OBJECT", "ARRAY", "VARIANT"}:
        # ES dynamic mapping can only assign one JSON kind per field; storing the
        # JSON as a string keeps the transfer lossless and avoids object/array
        # collisions when the same logical column contains mixed JSON shapes.
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=json_default)
        return value
    return value


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
    api_key: str = "",
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    backfill_new_fields: bool = False,
    **_kwargs: Any,
) -> WriteResult:
    del schema, error_policy, backfill_new_fields
    index = table_name or database
    cfg = {
        "host": host, "port": port, "username": username, "password": password,
        "connection_string": connection_string, "ssl": ssl, "api_key": api_key,
    }
    target_cols, logical_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}
    mapped_rows, errors = build_mapped_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=dest_types,
        preserve_case=True,
    )

    client = _client(cfg)
    try:
        if create_table and not client.indices.exists(index=index):
            # Use one shard and zero replicas for predictable test/CI behavior
            # and to avoid blowing through small cluster shard limits.
            client.indices.create(
                index=index,
                body={"settings": {"number_of_shards": 1, "number_of_replicas": 0}},
            )

        from elasticsearch.helpers import bulk

        def gen_actions():
            for row in mapped_rows:
                source = {
                    target_cols[i]: _to_es_value(value, logical_types[i])
                    for i, value in enumerate(row)
                }
                doc_id = source.pop("_id", None)
                action: dict[str, Any] = {"_index": index, "_source": source}
                if doc_id is not None:
                    action["_id"] = str(doc_id)
                yield action

        written, bulk_errors = bulk(client, gen_actions(), raise_on_error=False)
        try:
            client.indices.refresh(index=index)
        except Exception:
            pass
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=index,
            target_schema=host or "localhost",
            checksum=row_checksum(mapped_rows, target_cols),
            chunks_completed=1,
            warnings=(errors + [str(e) for e in bulk_errors[:5]])[:10],
            rejected_rows=len(errors) + len(bulk_errors or []),
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=index, target_schema=host or "",
            checksum="", chunks_completed=0, error=str(exc),
        )
    finally:
        client.close()
