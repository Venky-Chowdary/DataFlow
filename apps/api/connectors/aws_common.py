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
    explicit = (cfg.get("endpoint_url") or cfg.get("connection_string") or "").strip()
    if explicit.startswith("http://") or explicit.startswith("https://"):
        return explicit.rstrip("/")
    host = (cfg.get("host") or "").strip()
    if host.startswith("http://") or host.startswith("https://"):
        return host.rstrip("/")
    if host.endswith(".amazonaws.com"):
        return f"https://{host}"
    # A host with no dots (e.g. ``us-east-1``) is an AWS region, not a network
    # endpoint.  Leave endpoint_url empty so boto3 uses its default resolver
    # (required for moto mocks and real AWS SDK endpoints).
    if host and "." not in host and host not in ("localhost", "127.0.0.1", "host.docker.internal"):
        return ""
    # If host already includes a port, extract it so we don't duplicate the port param.
    if ":" in host:
        host, _, port_from_host = host.rpartition(":")
        port = int(port_from_host) if port_from_host.isdigit() else cfg.get("port")
    else:
        port = cfg.get("port")
    if host and port:
        ssl = cfg.get("ssl", False)
        scheme = "https" if ssl else "http"
        return f"{scheme}://{host}:{int(port)}"
    if host in ("localhost", "127.0.0.1", "host.docker.internal") and port:
        return f"http://{host}:{int(port)}"
    return ""


def is_local_endpoint(cfg: dict[str, Any]) -> bool:
    endpoint = resolve_endpoint_url(cfg)
    if not endpoint:
        host = (cfg.get("host") or "").strip().lower()
        return host in ("localhost", "127.0.0.1", "host.docker.internal")
    parsed = urlparse(endpoint)
    return parsed.hostname in ("localhost", "127.0.0.1", "host.docker.internal")


def resolve_region(cfg: dict[str, Any]) -> str:
    host = (cfg.get("host") or "").strip().split(":")[0]
    if host.startswith("http://") or host.startswith("https://") or host.endswith(".amazonaws.com"):
        if host == "s3.amazonaws.com":
            return "us-east-1"
        # Extract region from virtual-hosted style endpoints like s3.us-east-1.amazonaws.com
        parts = host.split(".")
        if host.endswith(".amazonaws.com") and len(parts) >= 3 and parts[-2] == "amazonaws":
            candidate = parts[-3]
            if candidate and candidate not in ("s3", "s3-website"):
                return candidate
        return "us-east-1"
    if host and host not in ("localhost", "127.0.0.1", "host.docker.internal"):
        return host
    return "us-east-1"


def boto3_client(service: str, cfg: dict[str, Any]):
    import boto3
    from botocore.config import Config

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
    if cfg.get("path_style") and service == "s3":
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client(service, **kwargs)
