"""Elasticsearch index writer — bulk indexing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from connectors.elasticsearch_reader import _client
from connectors.writer_common import build_mapped_rows, resolve_target_columns, row_checksum


@dataclass
class WriteResult:
    ok: bool
    rows_written: int
    table_name: str
    target_schema: str
    checksum: str
    chunks_completed: int
    error: str | None = None
    driver: str = "elasticsearch-py"
    rejected_rows: int = 0
    warnings: list[str] = field(default_factory=list)


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
) -> WriteResult:
    del schema, error_policy
    index = table_name or database
    cfg = {
        "host": host, "port": port, "username": username, "password": password,
        "connection_string": connection_string, "ssl": ssl,
    }
    target_cols = resolve_target_columns(mappings, headers)
    mapped_rows, errors = build_mapped_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
    )

    client = _client(cfg)
    try:
        if create_table and not client.indices.exists(index=index):
            client.indices.create(index=index)

        from elasticsearch.helpers import bulk

        def gen_actions():
            for row in mapped_rows:
                yield {"_index": index, "_source": dict(zip(target_cols, row))}

        written, bulk_errors = bulk(client, gen_actions(), raise_on_error=False)
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=index,
            target_schema=host or "localhost",
            checksum=row_checksum(mapped_rows),
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
