from __future__ import annotations

from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class ClickUpSource(ManifestConnector):
    name = "clickup"
    descriptor = ConnectorDescriptor(
        id="clickup",
        display_name="ClickUp",
        roles=frozenset({"source"}),
        auth_modes=("api_key",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "list_id", "type": "string", "required": False},
        ),
        evidence="synthetic-fixture",
        docs="https://developer.clickup.com/",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). The personal API token is sent as a raw Authorization "
            "header (pk_...); tasks stream requires a list id."
        ),
        catalog_ids=("clickup",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        adapted.setdefault("api_key", adapted.get("token") or adapted.get("access_token") or "")
        adapted.setdefault("list_id", adapted.get("list") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key"],
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "list_id": {"type": "string"},
                },
            }
        }
