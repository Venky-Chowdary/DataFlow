from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class SlackSource(ManifestConnector):
    name = "slack"
    descriptor = ConnectorDescriptor(
        id="slack",
        display_name="Slack",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "channel", "type": "string", "required": False, "sensitive": False},
        ),
        evidence="synthetic-fixture",
        docs="https://api.slack.com/methods",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. The messages stream requires the channel field and "
            "uses at-least-once delivery with a one-second lookback on oldest."
        ),
        catalog_ids=("slack",),
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
                    "channel": {"type": "string"},
                },
            }
        }
