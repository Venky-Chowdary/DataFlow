"""Shared helpers for object-store readers."""

from __future__ import annotations

from typing import Any

from connectors.base import ReadBatch
from services.object_streaming import (
    download_for_object_store,
    download_object,
    read_rows_from_spill,
)

__all__ = ["ReadBatch", "read_object_from_store", "_object_version_token"]


def _object_version_token(store: str, cfg: dict[str, Any], bucket: str, key: str) -> str:
    """Best-effort content version for spill cache keys (ETag / generation / size+mtime).

    When version metadata is unavailable, return empty — callers should force refresh
    rather than reuse a TTL cache that can serve stale overwritten objects.
    """
    try:
        if store == "s3":
            from connectors.aws_common import boto3_client

            head = boto3_client("s3", cfg).head_object(Bucket=bucket, Key=key)
            etag = str(head.get("ETag") or "").strip('"')
            ver = str(head.get("VersionId") or "")
            return f"{etag}:{ver}" if etag or ver else ""
        if store == "gcs":
            from connectors.gcs_common import gcs_client

            blob = gcs_client(cfg).bucket(bucket).get_blob(key)
            if blob is None:
                return ""
            return f"{blob.generation or ''}:{blob.metageneration or ''}:{blob.etag or ''}"
        if store == "adls":
            from connectors.adls_common import blob_service_client

            props = blob_service_client(cfg).get_blob_client(bucket, key).get_blob_properties()
            etag = str(getattr(props, "etag", "") or "").strip('"')
            ver = str(getattr(props, "version_id", "") or "")
            return f"{etag}:{ver}"
    except Exception:
        return ""
    return ""


def read_object_from_store(
    store: str,
    cfg: dict[str, Any],
    bucket: str,
    key: str,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    """Download an object from ``store`` to a local spill file and read rows.

    Spill cache keys include source version metadata when available so an overwritten
    object is never served as current under a stale TTL entry.
    """
    version = _object_version_token(store, cfg, bucket, key)
    cache_key = f"{store}:{bucket}:{key}:{version or 'nov'}"
    # No version ⇒ force download (refuse TTL reuse of potentially stale bytes).
    path = download_object(
        cache_key,
        lambda p: download_for_object_store(store, p, cfg, bucket, key),
        force=not bool(version),
    )
    headers, rows, total = read_rows_from_spill(
        path,
        key,
        offset=offset,
        limit=limit,
        known_total=known_total_rows,
    )
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
