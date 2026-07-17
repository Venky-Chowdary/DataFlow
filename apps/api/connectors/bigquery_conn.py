"""BigQuery connection helper — project + optional service account JSON path.

Supports local emulators (e.g. goccy/bigquery-emulator) by detecting
localhost/api_endpoint URLs and using anonymous credentials.
"""

from __future__ import annotations

from typing import Any


def _is_local_endpoint(host: str, connection_string: str) -> tuple[bool, str]:
    """Return (is_local, endpoint_url) for BigQuery-compatible emulators."""
    raw = (connection_string or host or "").strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return True, raw.rstrip("/")
    if raw in ("localhost", "127.0.0.1", "host.docker.internal"):
        return True, ""
    return False, ""


def get_client(
    *,
    project_id: str,
    credentials_path: str = "",
    service_account: str = "",
    location: str = "",
    host: str = "",
    port: int = 0,
    connection_string: str = "",
) -> Any:
    from google.api_core.client_options import ClientOptions
    from google.cloud import bigquery

    creds_ref = (service_account or connection_string or credentials_path or "").strip()
    is_local, endpoint_url = _is_local_endpoint(host, creds_ref)

    if is_local:
        from google.auth.credentials import AnonymousCredentials

        creds = AnonymousCredentials()
        client_options = None
        if endpoint_url:
            client_options = ClientOptions(api_endpoint=endpoint_url)
        elif port:
            client_options = ClientOptions(api_endpoint=f"http://{host}:{port}")
        elif host in ("localhost", "127.0.0.1"):
            client_options = ClientOptions(api_endpoint="http://127.0.0.1:9050")
        return bigquery.Client(
            project=project_id,
            credentials=creds,
            location=location or None,
            client_options=client_options,
        )

    if creds_ref:
        from google.oauth2 import service_account

        if creds_ref.startswith("{"):
            import json

            info = json.loads(creds_ref)
            creds = service_account.Credentials.from_service_account_info(info)
        else:
            creds = service_account.Credentials.from_service_account_file(creds_ref)
        return bigquery.Client(project=project_id, credentials=creds, location=location or None)
    return bigquery.Client(project=project_id, location=location or None)
