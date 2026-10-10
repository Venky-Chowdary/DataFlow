"""Shared Azure Blob Storage / ADLS Gen2 client helpers."""

from __future__ import annotations

import json
import logging
from typing import Any

_logger = logging.getLogger(__name__)


def _is_local(host: str, port: int) -> bool:
    return host in ("localhost", "127.0.0.1", "host.docker.internal") or port == 10000


def _emulator_endpoint(cfg: dict[str, Any]) -> bool:
    """True when this config is Azurite or another local Blob stand-in.

    Real Azure keeps the SDK's current service version. Azurite answers
    ``400 Bad Request`` to that version on ``list_containers`` before any
    container exists. A tunneled emulator is not always ``localhost``.
    """
    host = str(cfg.get("host") or "").strip().lower()
    port = int(cfg.get("port") or 0)
    blob = " ".join(
        str(cfg.get(key) or "")
        for key in ("connection_string", "endpoint", "account_url", "url")
    ).lower()
    if _is_local(host, port) or port == 10000:
        return True
    if "azurite" in host or "azurite" in blob:
        return True
    if "devstoreaccount1" in blob or "usedevelopmentstorage=true" in blob:
        return True
    if _account_name(cfg).lower() == "devstoreaccount1":
        return True
    if cfg.get("emulator") or cfg.get("azurite"):
        return True
    return False


def api_version_rejected(exc: BaseException) -> bool:
    """True when the service refused the request's ``x-ms-version``.

    A bad account key or a missing container is a different 400. Only a
    version refusal is retried against the Azurite-compatible pin.
    """
    text = str(exc).lower()
    return (
        "x-ms-version" in text
        or "rest version" in text
        or "specified api version" in text
        or ("invalidheadervalue" in text and "version" in text)
    )


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
    public = host.lower() in ("", account.lower()) or host.lower().endswith(".core.windows.net")
    if not public and (port or _emulator_endpoint(cfg)):
        # A tunneled / private Blob endpoint is path-style; the shared-key
        # signature covers /<account>/..., so public Azure must not be dialed.
        scheme = "https" if cfg.get("ssl") else "http"
        url = f"{scheme}://{host}:{port or 10000}/{account}"
        _logger.info("ADLS field form uses path-style custom endpoint %s", url)
        return url
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


def list_service_containers(client: Any, *, maxresults: int = 1) -> Any:
    """One page of account containers, without an empty ``include=`` query.

    ``BlobServiceClient.list_containers`` always passes ``include=[]``. The
    generated serializer writes that as ``?comp=list&include=``, and Azurite
    answers ``400 Bad Request`` before any container exists. ``include=None``
    omits the parameter. Metadata, deleted, and system flags are not requested.
    """
    service = client._client.service
    return service.list_containers_segment(include=None, maxresults=maxresults)


def blob_service_client(cfg: dict[str, Any]):
    """Build a BlobServiceClient from connection string, service principal, or account URL + key."""
    from azure.storage.blob import BlobServiceClient

    conn_str = _connection_string(cfg)
    azurite = _emulator_endpoint({**cfg, "connection_string": conn_str or cfg.get("connection_string") or ""})
    connection_timeout = cfg.get("connection_timeout", 5 if azurite else 60)
    read_timeout = cfg.get("read_timeout", 5 if azurite else 60)
    retry_total = cfg.get("retry_total", 0 if azurite else 3)
    client_kwargs = {
        "connection_timeout": connection_timeout,
        "read_timeout": read_timeout,
        "retry_total": retry_total,
    }
    pinned = str(cfg.get("api_version") or "").strip()
    if pinned:
        client_kwargs["api_version"] = pinned
    elif azurite:
        # Current blob SDK defaults (2025-x) make Azurite answer
        # ``400 Bad Request`` on list_containers before any container exists.
        # 2021-12-02 is the version Azurite has accepted since 3.18. Real
        # Azure keeps the SDK default — this pin is only for the stand-in,
        # or for an explicit retry after the service rejected x-ms-version.
        client_kwargs["api_version"] = "2021-12-02"
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str, **client_kwargs)

    sp = _service_principal_credential(cfg)
    if sp:
        url = _account_url(cfg)
        return BlobServiceClient(account_url=url, credential=sp, **client_kwargs)

    key = _account_key(cfg)
    url = _account_url(cfg)
    # Name the account explicitly: a path-style endpoint's hostname is not
    # <account>.blob..., so the SDK cannot derive it for the shared-key signature.
    credential = {"account_name": _account_name(cfg), "account_key": key} if key else None
    return BlobServiceClient(account_url=url, credential=credential, **client_kwargs)
