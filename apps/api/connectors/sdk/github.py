from __future__ import annotations

from typing import Any

from connectors.sdk import ConnectorDescriptor, register_connector
from connectors.sdk.declarative.connector import ManifestConnector


@register_connector
class GitHubSource(ManifestConnector):
    name = "github"
    descriptor = ConnectorDescriptor(
        id="github",
        display_name="GitHub",
        roles=frozenset({"source"}),
        auth_modes=("bearer",),
        sync_modes=("full_refresh", "incremental"),
        form_fields=(
            {"name": "access_token", "type": "string", "required": True, "sensitive": True},
            {"name": "owner", "type": "string", "required": True, "sensitive": False},
            {"name": "repo", "type": "string", "required": True, "sensitive": False},
        ),
        evidence="synthetic-fixture",
        docs="https://docs.github.com/en/rest/issues/issues#list-repository-issues",
        description=(
            "certified as SDK source on synthetic fixtures; not yet wired into the "
            "transfer engine. Issue records preserve GitHub's pull_request field."
        ),
    )

    def spec(self) -> dict[str, Any]:
        return {
            "connectionSpecification": {
                "type": "object",
                "required": ["access_token", "owner", "repo"],
                "properties": {
                    "access_token": {
                        "type": "string",
                        "airbyte_secret": True,
                    },
                    "owner": {"type": "string"},
                    "repo": {"type": "string"},
                },
            }
        }
