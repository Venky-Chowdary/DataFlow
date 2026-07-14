"""Shared Azure Blob Storage / ADLS Gen2 client helpers."""

from __future__ import annotations

import json
from typing import Any


def _is_local(host: str, port: int) -> bool:
    return host in ("localhost", "127.0.0.1", "host.docker.internal") or port == 10000


def _connection_string(cfg: dict[str, Any]) -> str | None:
    raw = (cfg.get("connection_string") or "").strip()
    if raw and ("AccountName" in raw or "BlobEndpoint" in raw):
        return raw
    return None


def _account_key(cfg: dict[str, Any]) -> str:
    return (cfg.get("password") or cfg.get("account_key") or "").strip()


def _account_name(cfg: dict[str, Any]) -> str:
    return (cfg.get("username") or cfg.get("account_name") or cfg.get("host") or "").strip()


def _account_url(cfg: dict[str, Any]) -> str:
    account = _account_name(cfg)
    host = (cfg.get("host") or "").strip()
    port = int(cfg.get("port") or 0)
    if _is_local(host, port):
        return f"http://{host}:{port}/{account}"
    return f"https://{account}.blob.core.windows.net"


def _service_principal_credential(cfg: dict[str, Any]):
    """Return an Azure credential from service_account JSON if available."""
    sa = (cfg.get("service_account") or "").strip()
    if not sa:
        return None
    try:
        info = json.loads(sa)
    except json.JSONDecodeError:
        return None
    if not isinstance(info, dict):
        return None
    tenant_id = info.get("tenant_id") or info.get("tenantId")
    client_id = info.get("client_id") or info.get("clientId")
    client_secret = info.get("client_secret") or info.get("clientSecret")
    if not (tenant_id and client_id and client_secret):
        return None
    try:
        from azure.identity import ClientSecretCredential

        return ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
    except Exception:
        return None


def blob_service_client(cfg: dict[str, Any]):
    """Build a BlobServiceClient from connection string, service principal, or account URL + key."""
    from azure.storage.blob import BlobServiceClient

    conn_str = _connection_string(cfg)
    connection_timeout = cfg.get("connection_timeout", 60)
    read_timeout = cfg.get("read_timeout", 60)
    retry_total = cfg.get("retry_total", 3)
    client_kwargs = {
        "connection_timeout": connection_timeout,
        "read_timeout": read_timeout,
        "retry_total": retry_total,
    }
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str, **client_kwargs)

    sp = _service_principal_credential(cfg)
    if sp:
        url = _account_url(cfg)
        return BlobServiceClient(account_url=url, credential=sp, **client_kwargs)

    account = _account_name(cfg)
    key = _account_key(cfg)
    url = _account_url(cfg)
    return BlobServiceClient(account_url=url, credential=key or None, **client_kwargs)
