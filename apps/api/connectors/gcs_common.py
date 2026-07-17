"""Shared Google Cloud Storage client helpers."""

from __future__ import annotations

import json
from typing import Any


def _resolve_endpoint(cfg: dict[str, Any]) -> str | None:
    """Support GCS-compatible emulators (fake-gcs-server, LocalStack, etc.)."""
    raw = (cfg.get("connection_string") or cfg.get("host") or "").strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw.rstrip("/")
    port = cfg.get("port") or 0
    if raw in ("localhost", "127.0.0.1", "host.docker.internal") and port:
        return f"http://{raw}:{int(port)}"
    return None


def _credentials(creds_ref: str, project: str | None):
    from google.auth.credentials import AnonymousCredentials

    if not creds_ref:
        return AnonymousCredentials()

    if creds_ref.startswith("{"):
        from google.oauth2 import service_account

        info = json.loads(creds_ref)
        return service_account.Credentials.from_service_account_info(info)

    if creds_ref.endswith(".json") or "/" in creds_ref:
        from google.oauth2 import service_account

        return service_account.Credentials.from_service_account_file(creds_ref)

    return AnonymousCredentials()


def gcs_client(cfg: dict[str, Any]):
    from google.api_core.client_options import ClientOptions
    from google.cloud import storage

    project = (cfg.get("host") or "").strip() or None
    if project and project.startswith("http"):
        project = None
    creds_ref = (cfg.get("service_account") or cfg.get("connection_string") or cfg.get("password") or "").strip()
    if creds_ref.startswith("http://") or creds_ref.startswith("https://"):
        endpoint = creds_ref.rstrip("/")
        creds_ref = ""
    else:
        endpoint = _resolve_endpoint(cfg)

    creds = _credentials(creds_ref, project)
    client_options = ClientOptions(api_endpoint=endpoint) if endpoint else None
    return storage.Client(project=project, credentials=creds, client_options=client_options)
