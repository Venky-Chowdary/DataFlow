from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class TodoistSource(ManifestConnector):
    name = "todoist"
    descriptor = ConnectorDescriptor(
        id="todoist",
        display_name="Todoist",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://developer.todoist.com/rest/v2/",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Todoist REST v2 offers no updated-since filter on "
            "list endpoints, so streams are honestly full-refresh (the Sync API's "
            "sync_token is a different contract)."
        ),
        catalog_ids=("todoist",),
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
                "required": ["api_key"],
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                },
            }
        }
