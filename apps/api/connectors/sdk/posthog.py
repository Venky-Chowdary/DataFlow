from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class PosthogSource(ManifestConnector):
    name = "posthog"
    descriptor = ConnectorDescriptor(
        id="posthog",
        display_name="Posthog",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "domain", "type": "string", "required": True, "sensitive": False},
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "project_id", "type": "string", "required": True, "sensitive": False},
        ),
        evidence="synthetic-fixture",
        docs="https://posthog.com/docs/api",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). domain is the full host (app.posthog.com for US "
            "cloud, eu.posthog.com for EU, or your self-hosted host); every "
            "stream needs project_id. Posthog list APIs embed the next page "
            "URL in the body ('next') — the next_url paginator follows it on "
            "the same origin only."
        ),
        catalog_ids=("posthog",),
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
        adapted.setdefault("domain", adapted.get("host") or "")
        adapted.setdefault("project_id", adapted.get("project") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["domain", "api_key", "project_id"],
                "properties": {
                    "domain": {"type": "string"},
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "project_id": {"type": "string"},
                },
            }
        }
