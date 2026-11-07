from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class BrevoSource(ManifestConnector):
    name = "brevo"
    descriptor = ConnectorDescriptor(
        id="brevo",
        display_name="Brevo",
        roles=frozenset({"source"}),
        auth_modes=("api_key",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://developers.brevo.com/reference",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). Contacts increment on modifiedSince; lists and "
            "campaigns re-read in full."
        ),
        catalog_ids=("brevo",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault(
                "api_key",
                credentials.get("token") or credentials.get("access_token") or "",
            )
            adapted["credentials"] = credentials
        else:
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
