from __future__ import annotations

from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class MondaySource(ManifestConnector):
    name = "monday"
    descriptor = ConnectorDescriptor(
        id="monday",
        display_name="monday.com",
        roles=frozenset({"source"}),
        auth_modes=("api_key",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "board_id", "type": "string", "required": False},
        ),
        evidence="synthetic-fixture",
        docs="https://developer.monday.com/api-reference/docs",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). GraphQL v2 API (raw Authorization token). Boards and "
            "users stream top-level; items require a board id in the config."
        ),
        catalog_ids=("monday",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        adapted.setdefault(
            "api_key",
            adapted.get("token") or adapted.get("access_token") or "",
        )
        adapted.setdefault("board_id", adapted.get("board") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key"],
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "board_id": {"type": "string"},
                },
            }
        }
