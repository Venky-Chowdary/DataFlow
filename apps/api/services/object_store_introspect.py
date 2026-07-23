"""Object store schema inference — S3/GCS/Redis via statistical profiling.

Prefix introspect samples multiple objects (bounded) and unions schemas so
late columns / type widening are not missed when the first key is atypical.
"""

from __future__ import annotations

from typing import Any

from services.data_profiler import profile_dataset

# Bound network + parse cost for prefix schema union (Airbyte often samples one key).
_MAX_PREFIX_OBJECTS = 5
_ROWS_PER_OBJECT = 200


def rows_from_matrix(headers: list[str], rows: list[list[str]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append({headers[i]: row[i] if i < len(row) else None for i in range(len(headers))})
    return out


def profile_object_batch(headers: list[str], rows: list[list[str]]) -> dict[str, Any]:
    """Infer column types from parsed object rows."""
    if not headers:
        return {"ok": False, "error": "No columns detected", "columns": [], "schema": {}}
    records = rows_from_matrix(headers, rows)
    profile = profile_dataset(headers, records)
    return {
        "ok": True,
        "columns": headers,
        "schema": profile.get("schema", {}),
        "row_estimate": len(records),
        "quality_score": profile.get("quality_score", 0),
        "profiles": profile.get("columns", {}),
    }


def _widen_type(a: str, b: str) -> str:
    """Least-lossy merge of two inferred carriers."""
    from services.type_system import normalize_logical_type

    if not a:
        return b
    if not b:
        return a
    if a == b:
        return a
    la = normalize_logical_type(a)
    lb = normalize_logical_type(b)
    if la == lb:
        # Prefer parametric DECIMAL(p,s) with more digits when both decimal.
        if la == "decimal":
            return a if len(a) >= len(b) else b
        return a
    rank = {
        "boolean": 1,
        "integer": 2,
        "decimal": 3,
        "float": 3,
        "date": 4,
        "time": 4,
        "datetime": 5,
        "uuid": 5,
        "binary": 6,
        "array": 7,
        "json": 8,
        "text": 9,
        "string": 9,
    }
    ra = rank.get(la, 9)
    rb = rank.get(lb, 9)
    if ra != rb:
        return a if ra > rb else b
    # Same rank, different family → text (lossless string carrier).
    return "TEXT"


def merge_object_schemas(batches: list[dict[str, Any]]) -> dict[str, Any]:
    """Union columns and widen types across multiple object samples."""
    if not batches:
        return {"ok": False, "error": "No objects sampled", "columns": [], "schema": {}}
    ok_batches = [b for b in batches if b.get("ok")]
    if not ok_batches:
        return batches[0]

    col_order: list[str] = []
    seen: set[str] = set()
    schema: dict[str, str] = {}
    total_rows = 0
    for batch in ok_batches:
        total_rows += int(batch.get("row_estimate") or batch.get("total_rows") or 0)
        for col in batch.get("columns") or []:
            name = str(col)
            if name not in seen:
                seen.add(name)
                col_order.append(name)
        for col, typ in (batch.get("schema") or {}).items():
            name = str(col)
            schema[name] = _widen_type(schema.get(name, ""), str(typ))

    return {
        "ok": True,
        "columns": col_order,
        "schema": schema,
        "row_estimate": total_rows,
        "objects_sampled": len(ok_batches),
        "quality_score": min(float(b.get("quality_score") or 0) for b in ok_batches),
        "profiles": {},
    }


def _sample_prefix_keys(keys: list[str], *, max_objects: int = _MAX_PREFIX_OBJECTS) -> list[str]:
    data_keys = [k for k in keys if not str(k).endswith("/")]
    if not data_keys:
        return []
    # Prefer first + last + evenly spaced middle keys (schema drift across partitions).
    if len(data_keys) <= max_objects:
        return data_keys
    picks = {0, len(data_keys) - 1}
    step = max(1, (len(data_keys) - 1) // (max_objects - 1))
    for i in range(0, len(data_keys), step):
        picks.add(i)
        if len(picks) >= max_objects:
            break
    return [data_keys[i] for i in sorted(picks)[:max_objects]]


def introspect_s3_object(
    cfg: dict[str, Any],
    *,
    bucket: str,
    key: str | None = None,
    prefix: str = "",
    sample_limit: int = 500,
) -> dict[str, Any]:
    from connectors.s3_reader import list_objects, read_object

    if not bucket:
        return {"ok": False, "error": "S3 bucket required", "columns": [], "schema": {}}

    object_key = key or ""
    sampled_keys: list[str] = []
    if object_key:
        sampled_keys = [object_key]
    else:
        keys = list_objects(cfg, bucket, prefix=prefix)
        sampled_keys = _sample_prefix_keys(keys)
        if not sampled_keys:
            return {
                "ok": False,
                "error": "No objects found under prefix",
                "columns": [],
                "schema": {},
                "objects": keys,
            }

    per_object_limit = max(50, min(sample_limit, _ROWS_PER_OBJECT))
    batches: list[dict[str, Any]] = []
    total_rows = 0
    for ok_key in sampled_keys:
        batch = read_object(cfg=cfg, bucket=bucket, key=ok_key, offset=0, limit=per_object_limit)
        profiled = profile_object_batch(batch.headers, batch.rows)
        profiled["total_rows"] = batch.total_rows
        batches.append(profiled)
        total_rows += int(batch.total_rows or 0)

    merged = merge_object_schemas(batches)
    return {
        **merged,
        "object_key": sampled_keys[0],
        "object_keys": sampled_keys,
        "total_rows": total_rows,
        "tables": sampled_keys,
    }


def introspect_gcs_object(
    cfg: dict[str, Any],
    *,
    bucket: str,
    key: str | None = None,
    prefix: str = "",
    sample_limit: int = 500,
) -> dict[str, Any]:
    from connectors.gcs_reader import list_objects, read_object

    if not bucket:
        return {"ok": False, "error": "GCS bucket required", "columns": [], "schema": {}}

    object_key = key or ""
    sampled_keys: list[str] = []
    if object_key:
        sampled_keys = [object_key]
    else:
        keys = list_objects(cfg, bucket, prefix=prefix)
        sampled_keys = _sample_prefix_keys(keys)
        if not sampled_keys:
            return {
                "ok": False,
                "error": "No objects found under prefix",
                "columns": [],
                "schema": {},
                "objects": keys,
            }

    per_object_limit = max(50, min(sample_limit, _ROWS_PER_OBJECT))
    batches: list[dict[str, Any]] = []
    total_rows = 0
    for ok_key in sampled_keys:
        batch = read_object(cfg=cfg, bucket=bucket, key=ok_key, offset=0, limit=per_object_limit)
        profiled = profile_object_batch(batch.headers, batch.rows)
        profiled["total_rows"] = batch.total_rows
        batches.append(profiled)
        total_rows += int(batch.total_rows or 0)

    merged = merge_object_schemas(batches)
    return {
        **merged,
        "object_key": sampled_keys[0],
        "object_keys": sampled_keys,
        "total_rows": total_rows,
        "tables": sampled_keys,
    }


def introspect_redis_keys(
    cfg: dict[str, Any],
    *,
    pattern: str = "*",
    sample_limit: int = 200,
) -> dict[str, Any]:
    from connectors.redis_reader import read_keys_batch

    batch = read_keys_batch(cfg=cfg, pattern=pattern or "*", offset=0, limit=sample_limit)
    profiled = profile_object_batch(batch.headers, batch.rows)
    return {
        **profiled,
        "pattern": pattern,
        "total_rows": batch.total_rows,
        "tables": [pattern],
    }
