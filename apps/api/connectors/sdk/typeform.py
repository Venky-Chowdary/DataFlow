from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class TypeformSource(ManifestConnector):
    name = "typeform"
    descriptor = ConnectorDescriptor(
        id="typeform",
        display_name="Typeform",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "form_id", "type": "string", "required": False, "sensitive": False},
        ),
        evidence="synthetic-fixture",
        docs="https://developer.typeform.com/",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). Responses run incrementally on 'since' "
            "(submitted_at) and require the form_id field; forms re-read in full."
        ),
        catalog_ids=("typeform",),
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
        adapted.setdefault("form_id", adapted.get("form") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key"],
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "form_id": {"type": "string"},
                },
            }
        }
