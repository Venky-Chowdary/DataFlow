from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class GitLabSource(ManifestConnector):
    name = "gitlab"
    descriptor = ConnectorDescriptor(
        id="gitlab",
        display_name="GitLab",
        roles=frozenset({"source"}),
        auth_modes=("api_key",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "base_url", "type": "string", "required": False, "sensitive": False},
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://docs.gitlab.com/ee/api/rest/",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). Incremental issues/merge_requests use at-least-once "
            "delivery with a one-second lookback on updated_after."
        ),
        catalog_ids=("gitlab",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault(
                "api_key",
                credentials.get("pat") or credentials.get("token") or "",
            )
            adapted["credentials"] = credentials
        else:
            adapted.setdefault(
                "api_key",
                adapted.get("pat") or adapted.get("token") or "",
            )
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key"],
                "properties": {
                    "base_url": {"type": "string"},
                    "api_key": {"type": "string", "airbyte_secret": True},
                },
            }
        }
