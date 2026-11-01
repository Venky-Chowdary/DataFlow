from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class NotionSource(ManifestConnector):
    name = "notion"
    descriptor = ConnectorDescriptor(
        id="notion",
        display_name="Notion",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "database_id", "type": "string", "required": False, "sensitive": False},
        ),
        evidence="synthetic-fixture",
        docs="https://developers.notion.com/reference",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Reads use POST /search (pages, databases) and POST "
            "databases/{id}/query with start_cursor in the request body; "
            "database_rows requires the database_id field. The workspace must "
            "share each page/database with the integration or the API returns "
            "empty results — that is Notion's contract, not a read failure."
        ),
        catalog_ids=("notion",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault(
                "access_token",
                credentials.get("token") or credentials.get("api_key") or "",
            )
            credentials.setdefault("api_key", credentials["access_token"])
            adapted["credentials"] = credentials
        else:
            adapted.setdefault(
                "api_key",
                adapted.get("access_token") or adapted.get("token") or "",
            )
        adapted.setdefault("database_id", adapted.get("database") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key"],
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "database_id": {"type": "string"},
                },
            }
        }
