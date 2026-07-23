"""Redis writer — store records as JSON strings under key prefix."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from connectors.redis_reader import _redis_client
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    build_mapped_rows_with_details,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)
from services.error_handling import format_exception_message
from services.value_serializer import json_default, sanitize_json_value


@dataclass
class WriteResult(_WriteResult):
    driver: str = "redis-py"


def _resolve_redis_key_id(
    doc: dict[str, Any],
    target_cols: list[str],
    *,
    conflict_columns: list[str],
    row_index: int,
) -> tuple[str | None, str]:
    """Return (key_id, identity_column) — None key_id means identity missing."""
    if conflict_columns:
        parts: list[str] = []
        for col in conflict_columns:
            val = doc.get(col)
            if val is None or str(val).strip() == "":
                return None, col
            parts.append(str(val))
        return "|".join(parts), conflict_columns[0]
    id_col = next(
        (c for c in target_cols if c.lower() in {"id", "_id", "pk", "key", "uuid"}),
        target_cols[0] if target_cols else "id",
    )
    key_id = doc.get(id_col)
    if key_id is None or str(key_id).strip() == "":
        # Never invent batch-relative keys (prefix:0) — retries overwrite siblings.
        return None, id_col
    return str(key_id), id_col


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
    conflict_columns: list[str] | None = None,
    **_kwargs: Any,
) -> WriteResult:
    del create_table, backfill_new_fields
    policy = transform_error_policy(error_policy)
    prefix = table_name or schema or "dataflow"
    cfg = {
        "host": host, "port": port, "database": database,
        "username": username, "password": password,
        "connection_string": connection_string, "ssl": ssl,
    }
    target_cols, logical_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}
    mapped_rows, errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=dest_types,
        preserve_case=True,
        error_policy=policy,
    )

    conflict = [c for c in (conflict_columns or _kwargs.get("conflict_columns") or []) if c in target_cols]
    client = _redis_client(cfg)
    try:
        written = 0
        for i, row in enumerate(mapped_rows):
            doc = dict(zip(target_cols, row))
            key_id, id_col = _resolve_redis_key_id(
                doc, target_cols, conflict_columns=conflict, row_index=i
            )
            if key_id is None:
                msg = (
                    f"Redis identity missing for conflict_columns={conflict}"
                    if conflict
                    else (
                        f"Redis identity missing for column `{id_col}` — "
                        "refuse batch-index key fabrication"
                    )
                )
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=prefix,
                        target_schema=f"db{database or 0}",
                        checksum="",
                        chunks_completed=0,
                        error=msg,
                        warnings=errors[:10],
                        rejected_rows=len({d["row"] for d in rejected_details}) + 1,
                        rejected_details=rejected_details[:100]
                        + [{
                            "row": i + 1,
                            "column": id_col,
                            "target": id_col,
                            "value": "",
                            "reason": msg,
                            "policy": "write_fail",
                            "chars": [],
                        }],
                    )
                rejected_details.append({
                    "row": i + 1,
                    "column": id_col,
                    "target": id_col,
                    "value": "",
                    "reason": msg,
                    "policy": "write_quarantine",
                    "chars": [],
                })
                continue
            key = f"{prefix}:{sanitize_identifier(str(key_id), preserve_case=True)}"
            try:
                # Pre-sanitize so extreme Decimals never raise mid-dumps.
                safe_doc = sanitize_json_value(doc)
                client.set(key, json.dumps(safe_doc, default=json_default, allow_nan=False))
                written += 1
            except Exception as cell_exc:
                msg = format_exception_message(cell_exc)
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=prefix,
                        target_schema=f"db{database or 0}",
                        checksum="",
                        chunks_completed=0,
                        error=msg,
                        warnings=errors[:10],
                        rejected_rows=len({d["row"] for d in rejected_details}) + 1,
                        rejected_details=rejected_details[:100]
                        + [{
                            "row": i + 1,
                            "column": id_col,
                            "target": id_col,
                            "value": str(key_id)[:120],
                            "reason": msg,
                            "policy": "write_fail",
                            "chars": [],
                        }],
                    )
                rejected_details.append({
                    "row": i + 1,
                    "column": id_col,
                    "target": id_col,
                    "value": str(key_id)[:120],
                    "reason": msg,
                    "policy": "write_quarantine",
                    "chars": [],
                })
                errors.append(msg)
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=prefix,
            target_schema=f"db{database or 0}",
            checksum=row_checksum(mapped_rows, target_cols),
            chunks_completed=1,
            warnings=errors[:10],
            rejected_rows=len({d["row"] for d in rejected_details}),
            rejected_details=rejected_details[:100],
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=prefix,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=format_exception_message(exc),
        )
    finally:
        client.close()
