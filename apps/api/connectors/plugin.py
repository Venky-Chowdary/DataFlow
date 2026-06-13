"""Connector plugin protocol — plan Part 5 contract."""

from __future__ import annotations

from typing import Any, Protocol


class ConnectorPlugin(Protocol):
    id: str
    name: str
    capabilities: list[str]

    def test_connection(self, config: dict[str, Any]) -> dict[str, Any]: ...
    def extract_schema(self, config: dict[str, Any]) -> dict[str, Any]: ...
    def read_batch(self, config: dict[str, Any], cursor: dict[str, Any], size: int) -> dict[str, Any]: ...
    def write_batch(self, config: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]: ...
    def preflight_probe(self, config: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]: ...


class RestConnectorPlugin:
    """Base class for AI Factory REST connectors."""

    connector_id: str = "rest_api"
    base_url: str = ""
    auth_type: str = "none"

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"method": method, "path": path, "params": params or {}, "status": "stub"}
