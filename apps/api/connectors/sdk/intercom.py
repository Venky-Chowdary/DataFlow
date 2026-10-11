from __future__ import annotations

from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class IntercomSource(ManifestConnector):
    name = "intercom"
    descriptor = ConnectorDescriptor(
        id="intercom",
        display_name="Intercom",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {
                "name": "access_token",
                "type": "string",
                "required": True,
                "sensitive": True,
            },
        ),
        evidence="synthetic-fixture",
        docs="https://developer.intercom.com/docs/references/rest-api/api.intercom.io/contacts",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Incremental conversations use at-least-once delivery "
            "with a one-second overlap on epoch-second cursors."
        ),
        catalog_ids=("intercom",),
    )

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["access_token"],
                "properties": {
                    "access_token": {
                        "type": "string",
                        "airbyte_secret": True,
                    }
                },
            }
        }
