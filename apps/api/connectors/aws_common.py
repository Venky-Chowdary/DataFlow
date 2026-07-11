"""Shared AWS client helpers for S3 and DynamoDB connectors."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse


def aws_credentials(cfg: dict[str, Any]) -> tuple[str, str, str]:
    region = (cfg.get("host") or "").strip() or "us-east-1"
    access_key = (cfg.get("username") or "").strip()
    secret_key = (cfg.get("password") or "").strip()
    return region, access_key, secret_key


def resolve_endpoint_url(cfg: dict[str, Any]) -> str:
    """Custom endpoint for DynamoDB Local or private AWS-compatible stacks."""
    explicit = (cfg.get("connection_string") or cfg.get("endpoint_url") or "").strip()
    if explicit.startswith("http://") or explicit.startswith("https://"):
        return explicit.rstrip("/")
    host = (cfg.get("host") or "").strip()
    if host.startswith("http://") or host.startswith("https://"):
        return host.rstrip("/")
    if host in ("localhost", "127.0.0.1") and cfg.get("port"):
        return f"http://{host}:{int(cfg['port'])}"
    return ""


def is_local_endpoint(cfg: dict[str, Any]) -> bool:
    endpoint = resolve_endpoint_url(cfg)
    if not endpoint:
        host = (cfg.get("host") or "").strip().lower()
        return host in ("localhost", "127.0.0.1", "host.docker.internal")
    parsed = urlparse(endpoint)
    return parsed.hostname in ("localhost", "127.0.0.1", "host.docker.internal")


def resolve_region(cfg: dict[str, Any]) -> str:
    host = (cfg.get("host") or "").strip()
    if host.startswith("http://") or host.startswith("https://"):
        return "us-east-1"
    if host and host not in ("localhost", "127.0.0.1"):
        return host
    return "us-east-1"


def boto3_client(service: str, cfg: dict[str, Any]):
    import boto3

    region = resolve_region(cfg)
    access_key = (cfg.get("username") or "").strip() or "local"
    secret_key = (cfg.get("password") or "").strip() or "local"
    endpoint_url = resolve_endpoint_url(cfg)
    kwargs: dict[str, Any] = {
        "region_name": region,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client(service, **kwargs)
