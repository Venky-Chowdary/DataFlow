"""Elasticsearch index writer — bulk indexing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import json_default

from connectors.elasticsearch_reader import _client
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    build_mapped_rows_with_details,
    resolve_target_columns,
    row_checksum,
    transform_error_policy,
)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "elasticsearch-py"


def _to_es_value(value: Any, source_type: str) -> Any:
    """Convert transform-engine values to Elasticsearch-native JSON shapes."""
    if value is None:
        return None
    upper = source_type.upper()
    if upper in {"DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC"}:
        # Keep as string — float64 would silently lose precision (no quarantine).
        return str(value)
    if upper in {"FLOAT", "DOUBLE", "FLOAT64", "REAL"}:
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


def _resolve_doc_id(
    source: dict[str, Any],
    *,
    conflict_columns: list[str],
    target_cols: list[str],
) -> str | None:
    """Deterministic document identity for idempotent upsert/retry.

    Prefer explicit ``_id``, then configured conflict/PK columns (including
    composite keys), then a single ``id`` field when present.
    """
    if "_id" in source and source.get("_id") is not None and str(source.get("_id")).strip() != "":
        return str(source["_id"])
    keys = [c for c in conflict_columns if c and c in source]
    if not keys:
        for alias in ("id", "ID", "Id", "pk", "PK"):
            if alias in source and source.get(alias) is not None and str(source.get(alias)).strip() != "":
                return str(source[alias])
        for col in target_cols:
            if col.lower() in {"id", "pk", "doc_id", "document_id"} and col in source:
                val = source.get(col)
                if val is not None and str(val).strip() != "":
                    return str(val)
        return None
    parts: list[str] = []
    for col in keys:
        val = source.get(col)
        if val is None or str(val).strip() == "":
            return None
        parts.append(str(val))
    return "|".join(parts) if len(parts) > 1 else parts[0]


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
    conflict_columns: list[str] | None = None,
    write_mode: str = "insert",
    **_kwargs: Any,
) -> WriteResult:
    del schema, backfill_new_fields
    policy = transform_error_policy(error_policy)
    index = table_name or database
    cfg = {
        "host": host, "port": port, "username": username, "password": password,
        "connection_string": connection_string, "ssl": ssl, "api_key": api_key,
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

    conflict = [c for c in (conflict_columns or []) if c]
    mode = (write_mode or "insert").lower()
    requires_identity = mode in {"upsert", "update", "merge", "cdc", "incremental"}

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

        identity_missing = 0
        actions: list[dict[str, Any]] = []
        for row in mapped_rows:
            source = {
                target_cols[i]: _to_es_value(value, logical_types[i])
                for i, value in enumerate(row)
            }
            doc_id = _resolve_doc_id(
                source,
                conflict_columns=conflict,
                target_cols=target_cols,
            )
            source.pop("_id", None)
            action: dict[str, Any] = {"_index": index, "_source": source}
            if doc_id is not None:
                action["_id"] = str(doc_id)
            elif requires_identity:
                identity_missing += 1
                continue
            actions.append(action)

        written, bulk_errors = bulk(client, actions, raise_on_error=False)
        try:
            client.indices.refresh(index=index)
        except Exception:
            pass
        if on_checkpoint:
            on_checkpoint(1, 1, written)

        # Materialize bulk item failures into rejected_details so control-plane
        # JSONL / dest DLQ see them — never return ok=True with silent drops.
        bulk_details: list[dict[str, Any]] = []
        for err in bulk_errors or []:
            if not isinstance(err, dict):
                bulk_details.append({
                    "row": "",
                    "column": "",
                    "target": "",
                    "value": "",
                    "reason": str(err)[:500],
                    "policy": policy,
                })
                continue
            inner = next(iter(err.values()), {}) if err else {}
            if not isinstance(inner, dict):
                inner = {}
            reason = inner.get("error") or err
            if isinstance(reason, dict):
                reason = reason.get("reason") or reason.get("type") or str(reason)
            bulk_details.append({
                "row": str(inner.get("_id") or ""),
                "column": "",
                "target": index,
                "value": "",
                "reason": f"elasticsearch bulk: {reason}"[:500],
                "policy": policy,
            })

        if identity_missing:
            bulk_details.append({
                "row": "",
                "column": ",".join(conflict) if conflict else "_id",
                "target": index,
                "value": "",
                "reason": (
                    f"elasticsearch upsert requires document identity "
                    f"({identity_missing} row(s) skipped) — map a primary key "
                    "to _id or configure conflict_columns"
                )[:500],
                "policy": policy,
            })

        all_rejected = list(rejected_details) + bulk_details
        fail_closed = policy == "fail" and bool(bulk_details)
        if requires_identity and identity_missing > 0 and written == 0:
            fail_closed = True
        err_msg = None
        if fail_closed:
            if identity_missing and written == 0:
                err_msg = (
                    f"elasticsearch upsert blocked: {identity_missing} row(s) "
                    "lack document identity"
                )
            elif bulk_errors:
                err_msg = f"elasticsearch bulk rejected {len(bulk_errors)} item(s)"
            else:
                err_msg = "elasticsearch write failed"
        return WriteResult(
            ok=not fail_closed,
            rows_written=written,
            table_name=index,
            target_schema=host or "localhost",
            checksum=row_checksum(mapped_rows, target_cols) if not fail_closed else "",
            chunks_completed=1,
            error=err_msg,
            warnings=(errors + [str(e) for e in (bulk_errors or [])[:5]])[:10],
            rejected_rows=len({str(d.get("row")) for d in all_rejected if d.get("row") not in (None, "")})
            + (1 if identity_missing else 0)
            + len(bulk_errors or []),
            rejected_details=all_rejected[:100],
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=index,
            target_schema=host or "",
            checksum="",
            chunks_completed=0,
            error=str(exc),
        )
    finally:
        try:
            client.close()
        except Exception:
            pass
