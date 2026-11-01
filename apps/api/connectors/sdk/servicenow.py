from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class ServiceNowSource(ManifestConnector):
    name = "servicenow"
    descriptor = ConnectorDescriptor(
        id="servicenow",
        display_name="ServiceNow",
        roles=frozenset({"source"}),
        auth_modes=("basic",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "instance", "type": "string", "required": True, "sensitive": False},
            {"name": "username", "type": "string", "required": True, "sensitive": False},
            {"name": "password", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://www.servicenow.com/docs/bundle/zurich-api-reference/page/integrate/inbound-rest/concept/c_TableAPI.html",
        description=(
            "wired into the transfer engine; certified on synthetic fixtures (beta evidence). Incremental table reads use at-least-once "
            "sysparm_query sys_updated_on filters with a 60-second lookback."
        ),
        catalog_ids=("servicenow",),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault("username", credentials.get("user") or "")
            credentials.setdefault("password", credentials.get("api_key") or "")
            adapted["credentials"] = credentials
        else:
            adapted.setdefault("username", adapted.get("user") or "")
            adapted.setdefault("password", adapted.get("api_key") or adapted.get("password") or "")
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["instance", "username", "password"],
                "properties": {
                    "instance": {"type": "string"},
                    "username": {"type": "string"},
                    "password": {"type": "string", "airbyte_secret": True},
                },
            }
        }
