from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class MailchimpSource(ManifestConnector):
    name = "mailchimp"
    descriptor = ConnectorDescriptor(
        id="mailchimp",
        display_name="Mailchimp",
        roles=frozenset({"source"}),
        auth_modes=("basic",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "dc", "type": "string", "required": True, "sensitive": False},
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://mailchimp.com/developer/marketing/api/",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Mailchimp's Marketing API accepts any basic-auth "
            "username; the API key is the password. Incremental lists/campaigns/"
            "templates use at-least-once since_* filters."
        ),
        catalog_ids=("mailchimp",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault("username", "datawrap")
            credentials.setdefault(
                "password",
                credentials.get("api_key") or credentials.get("password") or "",
            )
            adapted["credentials"] = credentials
        else:
            adapted.setdefault("username", adapted.get("username") or "datawrap")
            adapted.setdefault("password", adapted.get("api_key") or adapted.get("password") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["dc", "api_key"],
                "properties": {
                    "dc": {"type": "string"},
                    "api_key": {"type": "string", "airbyte_secret": True},
                },
            }
        }
