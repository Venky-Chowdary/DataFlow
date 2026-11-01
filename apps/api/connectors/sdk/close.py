from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class CloseSource(ManifestConnector):
    name = "close"
    descriptor = ConnectorDescriptor(
        id="close",
        display_name="Close",
        roles=frozenset({"source"}),
        auth_modes=("basic",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://developer.close.com/",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Close basic auth sends the API key as username "
            "(password is ignored by the API). Incremental leads use "
            "at-least-once date_updated__gte with a 60-second lookback."
        ),
        catalog_ids=("close",),
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
                "required": ["api_key"],
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                },
            }
        }
