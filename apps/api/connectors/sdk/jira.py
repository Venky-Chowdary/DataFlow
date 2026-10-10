from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class JiraCloudSource(ManifestConnector):
    name = "jira"
    descriptor = ConnectorDescriptor(
        id="jira",
        display_name="Jira Cloud",
        roles=frozenset({"source"}),
        auth_modes=("basic",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "site", "type": "string", "required": True, "sensitive": False},
            {"name": "email", "type": "string", "required": True, "sensitive": False},
            {"name": "api_token", "type": "string", "required": True, "sensitive": True},
        ),
        evidence="synthetic-fixture",
        docs="https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Incremental reads use at-least-once delivery with a "
            "60-second lookback for Jira's minute-granularity JQL timestamps."
        ),
    )

    def __init__(self, config: dict[str, Any]) -> None:
        adapted = dict(config)
        credentials = adapted.get("credentials")
        if isinstance(credentials, Mapping):
            credentials = dict(credentials)
            credentials.setdefault("username", credentials.get("email", ""))
            credentials.setdefault("password", credentials.get("api_token", ""))
            adapted["credentials"] = credentials
        else:
            adapted.setdefault("username", adapted.get("email", ""))
            adapted.setdefault("password", adapted.get("api_token", ""))
        super().__init__(adapted)

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["site", "email", "api_token"],
                "properties": {
                    "site": {"type": "string"},
                    "email": {"type": "string"},
                    "api_token": {
                        "type": "string",
                        "airbyte_secret": True,
                    },
                },
            }
        }
