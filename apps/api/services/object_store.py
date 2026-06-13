"""Object storage for uploaded file staging (MinIO with local fallback)."""

from __future__ import annotations

from typing import Any

from services.config import settings

_client: Any | None = None
_bucket_ready = False


def _get_client() -> Any | None:
    global _client
    import os

    if os.environ.get("DATAFLOW_DISABLE_OBJECT_STORE"):
        return None
    if _client is not None:
        return _client
    try:
        from minio import Minio

        _client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )
        return _client
    except Exception:
        return None


def _ensure_bucket(client: Any) -> bool:
    global _bucket_ready
    if _bucket_ready:
        return True
    try:
        if not client.bucket_exists(settings.minio_bucket):
            client.make_bucket(settings.minio_bucket)
        _bucket_ready = True
        return True
    except Exception:
        return False


def stage_bytes(key: str, content: bytes, content_type: str = "application/octet-stream") -> str | None:
    """Upload bytes to MinIO. Returns s3:// URI or None if unavailable."""
    import os

    if os.environ.get("DATAFLOW_DISABLE_OBJECT_STORE"):
        return None

    from io import BytesIO

    client = _get_client()
    if not client or not _ensure_bucket(client):
        return None
    try:
        client.put_object(
            settings.minio_bucket,
            key,
            BytesIO(content),
            length=len(content),
            content_type=content_type,
        )
        return f"s3://{settings.minio_bucket}/{key}"
    except Exception:
        return None


def storage_status() -> dict[str, str | bool]:
    client = _get_client()
    if not client:
        return {"backend": "local", "available": False}
    ok = _ensure_bucket(client)
    return {
        "backend": "minio",
        "available": ok,
        "endpoint": settings.minio_endpoint,
        "bucket": settings.minio_bucket,
    }
