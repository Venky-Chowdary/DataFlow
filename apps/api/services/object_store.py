"""Object storage for uploaded file staging (MinIO / S3-compatible with local fallback).

Railway multi-replica HA requires uploads to be readable from any Worker.
When object store is configured, stage_bytes uploads and materialize_local
downloads to a cache path on the claiming worker.
"""

from __future__ import annotations

import logging
import os
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from services.config import settings

_logger = logging.getLogger(__name__)

_client: Any | None = None
_bucket_ready = False
_backend: str | None = None  # "minio" | "s3" | None


def _disabled() -> bool:
    return os.environ.get("DATAFLOW_DISABLE_OBJECT_STORE", "").lower() in ("1", "true", "yes")


def _s3_env_configured() -> bool:
    return bool(
        os.getenv("DATAFLOW_S3_BUCKET")
        or os.getenv("AWS_S3_BUCKET")
        or os.getenv("S3_BUCKET")
    )


def _get_boto3_client() -> Any | None:
    try:
        import boto3
        from botocore.client import Config

        endpoint = (
            os.getenv("DATAFLOW_S3_ENDPOINT")
            or os.getenv("AWS_ENDPOINT_URL")
            or os.getenv("MINIO_ENDPOINT")
            or ""
        ).strip()
        if endpoint and "://" not in endpoint:
            secure = os.getenv("DATAFLOW_S3_SECURE", os.getenv("MINIO_SECURE", "0")).lower() in (
                "1",
                "true",
                "yes",
            )
            endpoint = f"{'https' if secure else 'http'}://{endpoint}"
        region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or "us-east-1"
        kwargs: dict[str, Any] = {
            "service_name": "s3",
            "region_name": region,
            "config": Config(s3={"addressing_style": "path" if os.getenv("DATAFLOW_S3_PATH_STYLE", "1") != "0" else "auto"}),
        }
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        access = (
            os.getenv("DATAFLOW_S3_ACCESS_KEY")
            or os.getenv("AWS_ACCESS_KEY_ID")
            or os.getenv("MINIO_ACCESS_KEY")
            or settings.minio_access_key
        )
        secret = (
            os.getenv("DATAFLOW_S3_SECRET_KEY")
            or os.getenv("AWS_SECRET_ACCESS_KEY")
            or os.getenv("MINIO_SECRET_KEY")
            or settings.minio_secret_key
        )
        if access and secret:
            kwargs["aws_access_key_id"] = access
            kwargs["aws_secret_access_key"] = secret
        return boto3.client(**kwargs)
    except Exception:
        _logger.debug("boto3 S3 client unavailable", exc_info=True)
        return None


def _get_client() -> Any | None:
    global _client, _backend
    if _disabled():
        return None
    if _client is not None:
        return _client
    # Prefer explicit MinIO SDK when MINIO_ENDPOINT is set (dev compose).
    if os.getenv("MINIO_ENDPOINT") or not _s3_env_configured():
        try:
            from minio import Minio

            _client = Minio(
                settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                secure=settings.minio_secure,
            )
            _backend = "minio"
            return _client
        except Exception:
            pass
    if _s3_env_configured() or os.getenv("MINIO_ENDPOINT"):
        client = _get_boto3_client()
        if client is not None:
            _client = client
            _backend = "s3"
            return _client
    return None


def _bucket_name() -> str:
    return (
        os.getenv("DATAFLOW_S3_BUCKET")
        or os.getenv("AWS_S3_BUCKET")
        or os.getenv("S3_BUCKET")
        or settings.minio_bucket
    )


def _ensure_bucket(client: Any) -> bool:
    global _bucket_ready
    if _bucket_ready:
        return True
    bucket = _bucket_name()
    try:
        if _backend == "minio":
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
        else:
            try:
                client.head_bucket(Bucket=bucket)
            except Exception:
                client.create_bucket(Bucket=bucket)
        _bucket_ready = True
        return True
    except Exception:
        _logger.debug("ensure_bucket failed for %s", bucket, exc_info=True)
        return False


def stage_bytes(key: str, content: bytes, content_type: str = "application/octet-stream") -> str | None:
    """Upload bytes to object store. Returns s3:// URI or None if unavailable."""
    if _disabled():
        return None
    client = _get_client()
    if not client or not _ensure_bucket(client):
        return None
    bucket = _bucket_name()
    try:
        if _backend == "minio":
            client.put_object(
                bucket,
                key,
                BytesIO(content),
                length=len(content),
                content_type=content_type,
            )
        else:
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=content,
                ContentType=content_type,
            )
        return f"s3://{bucket}/{key}"
    except Exception:
        _logger.debug("stage_bytes failed for %s", key, exc_info=True)
        return None


def fetch_bytes(uri: str) -> bytes | None:
    """Download object bytes from an s3:// URI."""
    if not uri or not uri.startswith("s3://"):
        return None
    client = _get_client()
    if not client:
        return None
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        return None
    try:
        if _backend == "minio":
            resp = client.get_object(bucket, key)
            try:
                return resp.read()
            finally:
                resp.close()
                resp.release_conn()
        resp = client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except Exception:
        _logger.debug("fetch_bytes failed for %s", uri, exc_info=True)
        return None


def materialize_local(uri: str, dest: Path) -> Path | None:
    """Download ``uri`` to ``dest`` so local file readers can open it."""
    data = fetch_bytes(uri)
    if data is None:
        return None
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return dest


def storage_status() -> dict[str, str | bool]:
    client = _get_client()
    if not client:
        return {"backend": "local", "available": False}
    ok = _ensure_bucket(client)
    return {
        "backend": _backend or "unknown",
        "available": ok,
        "endpoint": os.getenv("DATAFLOW_S3_ENDPOINT") or settings.minio_endpoint,
        "bucket": _bucket_name(),
    }
