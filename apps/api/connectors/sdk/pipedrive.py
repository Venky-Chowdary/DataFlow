from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class PipedriveSource(ManifestConnector):
    name = "pipedrive"
    descriptor = ConnectorDescriptor(
        id="pipedrive",
        display_name="Pipedrive",
        roles=frozenset({"source"}),
        auth_modes=("api_key",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "domain", "type": "string", "required": True, "sensitive": False},
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://developers.pipedrive.com/docs/api/v1",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Pipedrive v1 has no updated-since filter on entity "
            "endpoints, so streams are honest full-refresh reads paginated by "
            "additional_data.pagination.next_start."
        ),
        catalog_ids=("pipedrive",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault(
                "api_key",
                credentials.get("api_token") or credentials.get("token") or "",
            )
            adapted["credentials"] = credentials
        else:
            adapted.setdefault(
                "api_key",
                adapted.get("api_token") or adapted.get("token") or "",
            )
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["domain", "api_key"],
                "properties": {
                    "domain": {"type": "string"},
                    "api_key": {"type": "string", "airbyte_secret": True},
                },
            }
        }
