"""Shared Google Cloud Storage client helpers."""

from __future__ import annotations

import json
from typing import Any


def gcs_client(cfg: dict[str, Any]):
    from google.cloud import storage

    project = (cfg.get("host") or "").strip() or None
    creds_ref = (cfg.get("connection_string") or cfg.get("password") or "").strip()

    if creds_ref.startswith("{"):
        from google.oauth2 import service_account

        info = json.loads(creds_ref)
        creds = service_account.Credentials.from_service_account_info(info)
        return storage.Client(project=project or info.get("project_id"), credentials=creds)

    if creds_ref.endswith(".json") or "/" in creds_ref:
        from google.oauth2 import service_account

        creds = service_account.Credentials.from_service_account_file(creds_ref)
        return storage.Client(project=project or creds.project_id, credentials=creds)

    return storage.Client(project=project)
