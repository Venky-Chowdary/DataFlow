from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class ZendeskSource(ManifestConnector):
    name = "zendesk"
    descriptor = ConnectorDescriptor(
        id="zendesk",
        display_name="Zendesk",
        roles=frozenset({"source"}),
        auth_modes=("basic",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "domain", "type": "string", "required": True, "sensitive": False},
            {"name": "username", "type": "string", "required": True, "sensitive": False},
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
            {"name": "ticket_id", "type": "string", "required": False, "sensitive": False},
        ),
        evidence="synthetic-fixture",
        docs="https://developer.zendesk.com/api-reference/",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). Zendesk token auth is {email}/token + API token as "
            "the basic-auth pair; domain is the subdomain (acme for "
            "acme.zendesk.com). ticket_comments requires the ticket_id field. "
            "Ticket-export incremental needs the cursor-based export endpoint — "
            "streams are honestly full-refresh until that lands."
        ),
        catalog_ids=("zendesk",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)

        def _zendesk_user(email: str) -> str:
            # Zendesk API-token auth sends {email}/token as the username.
            return email if email.endswith("/token") else f"{email}/token"

        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            email = credentials.get("email") or credentials.get("user") or credentials.get("username") or ""
            credentials["username"] = _zendesk_user(str(email))
            credentials.setdefault(
                "password",
                credentials.get("api_key") or credentials.get("token") or "",
            )
            adapted["credentials"] = credentials
        else:
            email = adapted.get("email") or adapted.get("user") or adapted.get("username") or ""
            adapted["username"] = _zendesk_user(str(email))
            adapted.setdefault(
                "password",
                adapted.get("api_key") or adapted.get("token") or adapted.get("password") or "",
            )
        adapted.setdefault("domain", adapted.get("host") or "")
        adapted.setdefault("ticket_id", adapted.get("ticket") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["domain", "username", "api_key"],
                "properties": {
                    "domain": {"type": "string"},
                    "username": {"type": "string"},
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "ticket_id": {"type": "string"},
                },
            }
        }
