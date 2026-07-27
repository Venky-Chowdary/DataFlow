"""Redis writer — store records as JSON strings under a key prefix."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from services.error_handling import format_exception_message
from services.primary_key import (
    infer_redis_conflict_columns,
    pick_redis_identity_column,
)
from services.sync_cursor import is_overwrite_sync
from services.value_serializer import json_default, sanitize_json_value

from connectors.redis_reader import _redis_client
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    build_mapped_rows_with_details,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "redis-py"


# Thin aliases — tests/engine may import these names from the writer module.
_infer_redis_conflict_columns = infer_redis_conflict_columns
_pick_redis_identity_column = pick_redis_identity_column


def _resolve_redis_key_id(
    doc: dict[str, Any],
    target_cols: list[str],
    conflict_columns: list[str] | None,
    row_index: int,
) -> tuple[str | None, str]:
    """Return (key_id, identity_column) — None key_id means identity missing.

    Identity ranking matches Validate via ``services.primary_key`` (never prefer
    ``capital`` over ``code``).
    """
    del row_index  # retained for call-site compatibility / future diagnostics
    cols = list(conflict_columns or [])
    if not cols:
        picked = pick_redis_identity_column(list(target_cols))
        cols = [picked] if picked else []
    if not cols:
        return None, ""

    parts: list[str] = []
    for col in cols:
        val = doc.get(col)
        if val is None or str(val).strip() == "":
            return None, col
        parts.append(str(val))
    return "|".join(parts), cols[0]


def _clear_redis_prefix(client: Any, prefix: str) -> None:
    """Delete all existing keys under ``prefix:*`` for a full-refresh overwrite."""
    if not prefix:
        return
    pattern = f"{prefix}:*"
    # Delete in small chunks to avoid blocking Redis on large keyspaces.
    batch: list[Any] = []
    for key in client.scan_iter(match=pattern, count=500):
        batch.append(key)
        if len(batch) >= 1000:
            client.delete(*batch)
            batch.clear()
    if batch:
        client.delete(*batch)


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
    write_mode: str = "upsert",
    sync_mode: str = "",
    **_kwargs: Any,
) -> WriteResult:
    del create_table, backfill_new_fields
    file_batch_idx = int(_kwargs.pop("file_batch_idx", 0) or 0)
    policy = transform_error_policy(error_policy)
    prefix = table_name or schema or "dataflow"
    cfg = {
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "ssl": ssl,
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

    conflict = _infer_redis_conflict_columns(target_cols, mappings, conflict_columns)
    client = _redis_client(cfg)
    try:
        # Full-refresh overwrite must replace the destination keyspace once per job,
        # not once per chunk. Only the first chunk clears stale keys.
        if file_batch_idx in (0, 1) and (
            is_overwrite_sync(sync_mode) or write_mode in {"overwrite", "replace", "truncate"}
        ):
            _clear_redis_prefix(client, prefix)

        written = 0
        seen_keys: dict[str, int] = {}
        for i, row in enumerate(mapped_rows):
            doc = dict(zip(target_cols, row))
            key_id, id_col = _resolve_redis_key_id(doc, target_cols, conflict, row_index=i)
            if key_id is None:
                msg = (
                    f"Redis identity missing for conflict_columns={conflict}"
                    if conflict
                    else "Redis identity missing — no id-like column found in mapping"
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
                        + [
                            {
                                "row": i + 1,
                                "column": id_col or "",
                                "target": id_col or "",
                                "value": "",
                                "reason": msg,
                                "policy": "write_fail",
                                "chars": [],
                            }
                        ],
                    )
                rejected_details.append(
                    {
                        "row": i + 1,
                        "column": id_col or "",
                        "target": id_col or "",
                        "value": "",
                        "reason": msg,
                        "policy": "write_quarantine",
                        "chars": [],
                    }
                )
                continue

            key = f"{prefix}:{sanitize_identifier(str(key_id), preserve_case=True)}"
            if key in seen_keys:
                prev = seen_keys[key]
                msg = (
                    f"Duplicate Redis key '{key}' for rows {prev + 1} and {i + 1} "
                    f"(conflict on '{id_col}'). Use a unique primary key or deduplicate the source."
                )
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=prefix,
                    target_schema=f"db{database or 0}",
                    checksum="",
                    chunks_completed=0,
                    error=msg,
                    warnings=errors[:10],
                    rejected_rows=len({d["row"] for d in rejected_details}),
                    rejected_details=rejected_details[:100],
                )
            seen_keys[key] = i

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
                        + [
                            {
                                "row": i + 1,
                                "column": id_col,
                                "target": id_col,
                                "value": str(key_id)[:120],
                                "reason": msg,
                                "policy": "write_fail",
                                "chars": [],
                            }
                        ],
                    )
                rejected_details.append(
                    {
                        "row": i + 1,
                        "column": id_col,
                        "target": id_col,
                        "value": str(key_id)[:120],
                        "reason": msg,
                        "policy": "write_quarantine",
                        "chars": [],
                    }
                )
                errors.append(msg)
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=prefix,
            target_schema=f"db{database or 0}",
            checksum=row_checksum(
                mapped_rows,
                target_cols,
                dest_db_type="redis",
            ),
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
