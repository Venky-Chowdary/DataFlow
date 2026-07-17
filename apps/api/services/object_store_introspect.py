"""Object store schema inference — S3/GCS/Redis via statistical profiling."""

from __future__ import annotations

from typing import Any

from services.data_profiler import profile_dataset


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
    if not object_key:
        keys = list_objects(cfg, bucket, prefix=prefix)
        data_keys = [k for k in keys if not k.endswith("/")]
        if not data_keys:
            return {"ok": False, "error": "No objects found under prefix", "columns": [], "schema": {}, "objects": keys}
        object_key = data_keys[0]

    batch = read_object(cfg=cfg, bucket=bucket, key=object_key, offset=0, limit=sample_limit)
    profiled = profile_object_batch(batch.headers, batch.rows)
    return {
        **profiled,
        "object_key": object_key,
        "total_rows": batch.total_rows,
        "tables": [object_key],
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
    if not object_key:
        keys = list_objects(cfg, bucket, prefix=prefix)
        data_keys = [k for k in keys if not k.endswith("/")]
        if not data_keys:
            return {"ok": False, "error": "No objects found under prefix", "columns": [], "schema": {}, "objects": keys}
        object_key = data_keys[0]

    batch = read_object(cfg=cfg, bucket=bucket, key=object_key, offset=0, limit=sample_limit)
    profiled = profile_object_batch(batch.headers, batch.rows)
    return {
        **profiled,
        "object_key": object_key,
        "total_rows": batch.total_rows,
        "tables": [object_key],
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
