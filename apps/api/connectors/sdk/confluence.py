from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class ConfluenceSource(ManifestConnector):
    name = "confluence"
    descriptor = ConnectorDescriptor(
        id="confluence",
        display_name="Confluence",
        roles=frozenset({"source"}),
        auth_modes=("basic",),
        sync_modes=("full_refresh",),
        form_fields=(
            {"name": "domain", "type": "string", "required": True, "sensitive": False},
            {"name": "username", "type": "string", "required": True, "sensitive": False},
            {"name": "api_key", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://developer.atlassian.com/cloud/confluence/rest/v2/",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Atlassian basic auth is account email + API token. "
            "domain is the site subdomain (acme for acme.atlassian.net). "
            "Confluence v2 embeds the next page in _links.next — the next_url "
            "paginator follows it on the same origin only."
        ),
        catalog_ids=("confluence",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault(
                "username",
                credentials.get("email") or credentials.get("user") or "",
            )
            credentials.setdefault(
                "password",
                credentials.get("api_key") or credentials.get("token") or "",
            )
            adapted["credentials"] = credentials
        else:
            adapted.setdefault(
                "username",
                adapted.get("email") or adapted.get("user") or adapted.get("username") or "",
            )
            adapted.setdefault(
                "password",
                adapted.get("api_key") or adapted.get("token") or adapted.get("password") or "",
            )
        adapted.setdefault("domain", adapted.get("host") or "")
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
                },
            }
        }
