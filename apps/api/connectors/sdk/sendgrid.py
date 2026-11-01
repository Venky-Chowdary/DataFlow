from __future__ import annotations

from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class SendGridSource(ManifestConnector):
    name = "sendgrid"
    descriptor = ConnectorDescriptor(
        id="sendgrid",
        display_name="SendGrid",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://docs.sendgrid.com/api-reference/",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Streams lists, segments and templates via the v3 API "
            "(full refresh; contacts export is an async job, out of scope)."
        ),
        catalog_ids=("sendgrid",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        adapted.setdefault("api_key", adapted.get("access_token") or adapted.get("token") or "")
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
