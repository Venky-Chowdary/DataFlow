from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class KlaviyoSource(ManifestConnector):
    name = "klaviyo"
    descriptor = ConnectorDescriptor(
        id="klaviyo",
        display_name="Klaviyo",
        roles=frozenset({"source"}),
        auth_modes=("api_key",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://developers.klaviyo.com/en/reference/api-overview",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). Klaviyo accepts its links.next URL as the next "
            "page[cursor]; incremental profiles/events use at-least-once "
            "greater-than filters with a one-second lookback."
        ),
        catalog_ids=("klaviyo",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault(
                "api_key",
                credentials.get("private_key") or credentials.get("token") or "",
            )
            adapted["credentials"] = credentials
        else:
            adapted.setdefault(
                "api_key",
                adapted.get("private_key") or adapted.get("token") or "",
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
