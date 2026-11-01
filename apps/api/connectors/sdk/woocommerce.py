from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class WooCommerceSource(ManifestConnector):
    name = "woocommerce"
    descriptor = ConnectorDescriptor(
        id="woocommerce",
        display_name="WooCommerce",
        roles=frozenset({"source"}),
        auth_modes=("basic",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "domain", "type": "string", "required": True, "sensitive": False},
            {"name": "username", "type": "string", "required": True, "sensitive": False},
            {"name": "password", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://woocommerce.github.io/woocommerce-rest-api-docs/",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Orders and products increment on modified_after; "
            "customers re-read in full (the v3 customers list has no modified "
            "filter)."
        ),
        catalog_ids=("woocommerce",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault(
                "username",
                credentials.get("consumer_key") or credentials.get("username") or "",
            )
            credentials.setdefault(
                "password",
                credentials.get("consumer_secret") or credentials.get("password") or "",
            )
            adapted["credentials"] = credentials
        else:
            adapted.setdefault(
                "username",
                adapted.get("consumer_key") or adapted.get("username") or "",
            )
            adapted.setdefault(
                "password",
                adapted.get("consumer_secret") or adapted.get("password") or "",
            )
        adapted.setdefault("domain", adapted.get("host") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["domain", "username", "password"],
                "properties": {
                    "domain": {"type": "string"},
                    "username": {"type": "string"},
                    "password": {"type": "string", "airbyte_secret": True},
                },
            }
        }
