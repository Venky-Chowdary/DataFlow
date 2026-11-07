from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class CalendlySource(ManifestConnector):
    name = "calendly"
    descriptor = ConnectorDescriptor(
        id="calendly",
        display_name="Calendly",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "user_uri", "type": "string", "required": True, "sensitive": False},
        ),
        evidence="synthetic-fixture",
        docs="https://developer.calendly.com/api-docs",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). scheduled_events increments on min_start_time with "
            "a sixty-second lookback."
        ),
        catalog_ids=("calendly",),
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
        adapted.setdefault("user_uri", adapted.get("user") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key", "user_uri"],
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "user_uri": {"type": "string"},
                },
            }
        }
