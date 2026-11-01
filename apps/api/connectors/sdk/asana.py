from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class AsanaSource(ManifestConnector):
    name = "asana"
    descriptor = ConnectorDescriptor(
        id="asana",
        display_name="Asana",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "workspace", "type": "string", "required": True, "sensitive": False},
        ),
        evidence="synthetic-fixture",
        docs="https://developers.asana.com/reference/rest-api-reference",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Tasks run incrementally on modified_since; other "
            "streams re-read in full."
        ),
        catalog_ids=("asana",),
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
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key", "workspace"],
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "workspace": {"type": "string"},
                },
            }
        }
