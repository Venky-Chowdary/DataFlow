from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class BigCommerceSource(ManifestConnector):
    name = "bigcommerce"
    descriptor = ConnectorDescriptor(
        id="bigcommerce",
        display_name="BigCommerce",
        roles=frozenset({"source"}),
        auth_modes=("api_key",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "store_hash", "type": "string", "required": True, "sensitive": False},
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://developer.bigcommerce.com/docs/rest-management",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). Products/customers increment on date_modified:min; "
            "orders on min_date_modified (v2 API)."
        ),
        catalog_ids=("bigcommerce",),
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
        adapted.setdefault("store_hash", adapted.get("store") or adapted.get("store_id") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["store_hash", "api_key"],
                "properties": {
                    "store_hash": {"type": "string"},
                    "api_key": {"type": "string", "airbyte_secret": True},
                },
            }
        }
