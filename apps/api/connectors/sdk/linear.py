from __future__ import annotations

from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class LinearSource(ManifestConnector):
    name = "linear"
    descriptor = ConnectorDescriptor(
        id="linear",
        display_name="Linear",
        roles=frozenset({"source"}),
        auth_modes=("api_key",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://developers.linear.app/docs/graphql/working-with-the-graphql-api",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. GraphQL API (raw Authorization key, no Bearer prefix). "
            "Streams issues, projects and teams with pageInfo endCursor pagination."
        ),
        catalog_ids=("linear",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        adapted.setdefault(
            "api_key",
            adapted.get("token") or adapted.get("access_token") or "",
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
