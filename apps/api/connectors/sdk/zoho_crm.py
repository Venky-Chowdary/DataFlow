from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class ZohoCrmSource(ManifestConnector):
    name = "zoho_crm"
    descriptor = ConnectorDescriptor(
        id="zoho_crm",
        display_name="Zoho CRM",
        roles=frozenset({"source"}),
        auth_modes=("oauth2_refresh",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "client_id", "type": "string", "required": True, "sensitive": False},
            {"name": "client_secret", "type": "string", "required": True, "sensitive": True},
            {"name": "refresh_token", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://www.zoho.com/crm/developer/docs/api/v2/",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). OAuth2 refresh grant against accounts.zoho.com — "
            "other Zoho datacenters need their regional token URL (set the "
            "manifest token_url in an override config). Zoho's delta reads need "
            "an If-Modified-Since header, which the SDK cursor does not emit — "
            "streams are honestly full-refresh until that lands."
        ),
        catalog_ids=("zoho_crm",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            for key in ("client_id", "client_secret", "refresh_token", "access_token"):
                credentials.setdefault(
                    key, credentials.get(key) or adapted.get(key) or ""
                )
            adapted["credentials"] = credentials
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["client_id", "client_secret", "refresh_token"],
                "properties": {
                    "client_id": {"type": "string"},
                    "client_secret": {"type": "string", "airbyte_secret": True},
                    "refresh_token": {"type": "string", "airbyte_secret": True},
                    "access_token": {"type": "string", "airbyte_secret": True},
                },
            }
        }
