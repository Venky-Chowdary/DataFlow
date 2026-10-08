"""Shared AWS client helpers for S3 and DynamoDB connectors."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

_REPEATED_SCHEME = re.compile(r"^(https?://)(?=https?://)", re.IGNORECASE)
_PORT_THEN_DEFAULT_TLS = re.compile(
    r"^(https?://)([^/:]+):(\d+):443/?$",
    re.IGNORECASE,
)


def aws_credentials(cfg: dict[str, Any]) -> tuple[str, str, str]:
    region = (cfg.get("host") or "").strip() or "us-east-1"
    access_key = (cfg.get("username") or "").strip()
    secret_key = (cfg.get("password") or "").strip()
    return region, access_key, secret_key


def _explicit_service_port(cfg: dict[str, Any]) -> int | None:
    """Port the operator typed, when it names a service rather than HTTPS.

    ``0`` and ``443`` stay "no custom endpoint" so a region token such as
    ``us-east-1`` is not turned into ``http://us-east-1:443``.
    """
    raw = cfg.get("port")
    if raw in (None, "", 0, "0"):
        return None
    try:
        port = int(raw)
    except (TypeError, ValueError):
        return None
    if port in (0, 443):
        return None
    return port


def normalize_service_endpoint(
    raw: str,
    *,
    port: Any = None,
    ssl: bool = False,
) -> str:
    """One URL: no doubled scheme, no ``:20988:443``.

    A saved tunnel ``http://bore.pub:20988`` plus the DynamoDB form default
    ``443`` used to become ``http://http://bore.pub:20988:443``. The host
    already carries its port; ``443`` is not a second port.
    """
    text = (raw or "").strip()
    while True:
        nxt = _REPEATED_SCHEME.sub("", text, count=1)
        if nxt == text:
            break
        text = nxt
    doubled = _PORT_THEN_DEFAULT_TLS.match(text)
    if doubled:
        text = f"{doubled.group(1)}{doubled.group(2)}:{doubled.group(3)}"
    if "://" not in text:
        scheme = "https" if ssl else "http"
        text = f"{scheme}://{text}"
    try:
        parsed = urlparse(text)
        hostname = parsed.hostname
        url_port = parsed.port
    except ValueError:
        hostname = None
        url_port = None
    if not hostname:
        # ``http://bore.pub:20988:443`` does not parse. Peel a trailing :443.
        peeled = _PORT_THEN_DEFAULT_TLS.match(text)
        if peeled:
            return f"{peeled.group(1)}{peeled.group(2)}:{peeled.group(3)}"
        return text.rstrip("/")
    if url_port:
        scheme = parsed.scheme or ("https" if ssl else "http")
        return f"{scheme}://{hostname}:{url_port}"
    extra = _explicit_service_port({"port": port})
    scheme = parsed.scheme or ("https" if ssl else "http")
    if extra:
        return f"{scheme}://{hostname}:{extra}"
    return f"{scheme}://{hostname}"


def resolve_endpoint_url(cfg: dict[str, Any]) -> str:
    """Custom endpoint for DynamoDB Local or private AWS-compatible stacks."""
    explicit = (cfg.get("endpoint_url") or cfg.get("connection_string") or "").strip()
    if explicit.startswith("http://") or explicit.startswith("https://"):
        return normalize_service_endpoint(
            explicit,
            port=cfg.get("port"),
            ssl=bool(cfg.get("ssl")),
        )
    host = (cfg.get("host") or "").strip()
    if host.startswith("http://") or host.startswith("https://"):
        return normalize_service_endpoint(
            host,
            port=cfg.get("port"),
            ssl=bool(cfg.get("ssl")),
        )
    if host.endswith(".amazonaws.com"):
        return f"https://{host}"
    # A host with no dots is an AWS region only when no service port was
    # given (``us-east-1``). ``minio`` + ``9000`` is a Docker endpoint;
    # treating it as a region dropped the host and port.
    if host and "." not in host and host not in ("localhost", "127.0.0.1", "host.docker.internal"):
        port = _explicit_service_port(cfg)
        if port is None:
            return ""
        ssl = cfg.get("ssl", False)
        scheme = "https" if ssl else "http"
        return f"{scheme}://{host}:{port}"
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
    # MinIO and other custom endpoints require path-style addressing.
    # Virtual-hosted style looks up ``bucket.endpoint`` and listing fails
    # while a hand-typed GetObject can still succeed. Real AWS (no custom
    # endpoint) stays on the SDK default.
    if service == "s3" and (endpoint_url or cfg.get("path_style")):
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client(service, **kwargs)
