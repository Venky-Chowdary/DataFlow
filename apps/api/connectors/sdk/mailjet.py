from __future__ import annotations

from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class MailjetSource(ManifestConnector):
    name = "mailjet"
    descriptor = ConnectorDescriptor(
        id="mailjet",
        display_name="Mailjet",
        roles=frozenset({"source"}),
        auth_modes=("basic",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "secret_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://dev.mailjet.com/email/reference/",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Basic auth uses the public API key + private secret "
            "key pair; REST v3 lists are offset-paginated full refresh."
        ),
        catalog_ids=("mailjet",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        adapted.setdefault("username", adapted.get("api_key") or "")
        adapted.setdefault("password", adapted.get("secret_key") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["api_key", "secret_key"],
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "secret_key": {"type": "string", "airbyte_secret": True},
                },
            }
        }
