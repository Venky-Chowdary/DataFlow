"""Redis writer — store records as JSON strings under key prefix."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable

from connectors.redis_reader import _redis_client
from connectors.writer_common import build_mapped_rows, resolve_target_columns, row_checksum, sanitize_identifier


@dataclass
class WriteResult:
    ok: bool
    rows_written: int
    table_name: str
    target_schema: str
    checksum: str
    chunks_completed: int
    error: str | None = None
    driver: str = "redis-py"
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
    backfill_new_fields: bool = False,
    **_kwargs: Any,
) -> WriteResult:
    del create_table, error_policy, backfill_new_fields
    prefix = table_name or schema or "dataflow"
    cfg = {
        "host": host, "port": port, "database": database,
        "username": username, "password": password,
        "connection_string": connection_string, "ssl": ssl,
    }
    target_cols, _ = resolve_target_columns(mappings, column_types, preserve_case=True)
    mapped_rows, errors = build_mapped_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        preserve_case=True,
    )

    client = _redis_client(cfg)
    try:
        written = 0
        id_col = target_cols[0] if target_cols else "id"
        for i, row in enumerate(mapped_rows):
            doc = dict(zip(target_cols, row))
            key_id = doc.get(id_col) or str(i)
            key = f"{prefix}:{sanitize_identifier(str(key_id), preserve_case=True)}"
            client.set(key, json.dumps(doc, default=str))
            written += 1
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=prefix,
            target_schema=f"db{database or 0}",
            checksum=row_checksum(mapped_rows),
            chunks_completed=1,
            warnings=errors[:10],
            rejected_rows=len(errors),
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=prefix, target_schema="",
            checksum="", chunks_completed=0, error=str(exc),
        )
    finally:
        client.close()
