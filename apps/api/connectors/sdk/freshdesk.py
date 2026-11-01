from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class FreshdeskSource(ManifestConnector):
    name = "freshdesk"
    descriptor = ConnectorDescriptor(
        id="freshdesk",
        display_name="Freshdesk",
        roles=frozenset({"source"}),
        auth_modes=("basic",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "domain", "type": "string", "required": True, "sensitive": False},
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://developers.freshdesk.com/api/",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Freshdesk basic auth takes the API key as username "
            "with password X. Incremental tickets use at-least-once "
            "updated_since with a 60-second lookback."
        ),
        catalog_ids=("freshdesk",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault(
                "username",
                credentials.get("api_key") or credentials.get("token") or "",
            )
            credentials.setdefault("password", "X")
            adapted["credentials"] = credentials
        else:
            adapted.setdefault(
                "username",
                adapted.get("api_key") or adapted.get("token") or adapted.get("username") or "",
            )
            adapted.setdefault("password", "X")
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
