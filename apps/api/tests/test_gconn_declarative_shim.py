from __future__ import annotations

from connectors.sdk.http_declarative import DeclarativeHttpConnector
from connectors.sdk.declarative.connector import DeclarativeSource
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer


def test_legacy_connector_delegates_to_declarative_source_and_preserves_state() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/items",
            FixtureResponse(body={"data": [{"id": "1", "updated_at": "t1"}]}),
        )
        connector = DeclarativeHttpConnector(
            {
                "api_key": "secret",
                "spec": {
                    "name": "legacy",
                    "base_url": fixture.base_url,
                    "streams": [
                        {
                            "name": "items",
                            "path": "items",
                            "primary_key": ["id"],
                            "records_path": "data",
                            "cursor_field": "updated_at",
                            "cursor_param": "after",
                            "properties": {"id": "string", "updated_at": "string"},
                        }
                    ],
                },
            }
        )

        assert isinstance(connector, DeclarativeSource)
        batch = next(connector.read("items", limit=5))

        assert batch.records == [{"id": "1", "updated_at": "t1"}]
        assert batch.state == {"items": {"updated_at": "t1"}}
