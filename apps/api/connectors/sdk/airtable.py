from __future__ import annotations

from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class AirtableSource(ManifestConnector):
    name = "airtable"
    descriptor = ConnectorDescriptor(
        id="airtable",
        display_name="Airtable",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "base_id", "type": "string", "required": True},
            {"name": "table_id", "type": "string", "required": False},
        ),
        evidence="synthetic-fixture",
        docs="https://airtable.com/developers/web/api/introduction",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). Records stream follows the response-carried offset "
            "token; Airtable has no updated-since filter so records are honestly "
            "full-refresh."
        ),
        catalog_ids=("airtable",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        adapted.setdefault("api_key", adapted.get("access_token") or adapted.get("token") or "")
        adapted.setdefault("base_id", adapted.get("base") or "")
        adapted.setdefault("table_id", adapted.get("table") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key", "base_id"],
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "base_id": {"type": "string"},
                    "table_id": {"type": "string"},
                },
            }
        }
