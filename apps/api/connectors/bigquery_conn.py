"""BigQuery connection helper — project + optional service account JSON path."""

from __future__ import annotations

from typing import Any


def get_client(
    *,
    project_id: str,
    credentials_path: str = "",
    location: str = "",
) -> Any:
    from google.cloud import bigquery

    if credentials_path.strip():
        from google.oauth2 import service_account

        creds = service_account.Credentials.from_service_account_file(credentials_path.strip())
        return bigquery.Client(project=project_id, credentials=creds, location=location or None)
    return bigquery.Client(project=project_id, location=location or None)
