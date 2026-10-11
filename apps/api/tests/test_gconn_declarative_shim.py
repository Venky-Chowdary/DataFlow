from __future__ import annotations

import inspect

import pytest

from connectors.sdk import get_descriptor
from connectors.sdk.declarative.errors import PaginationError
from connectors.sdk.http_declarative import DeclarativeHttpConnector
from connectors.sdk.declarative.connector import DeclarativeSource
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer


@pytest.mark.parametrize(
    "indicator",
    [
        {"next": "/items?page=2"},
        {"next_page": "page-2"},
        {"has_more": True},
        {"paging": {"next": {"url": "/items?page=2"}}},
    ],
)
def test_legacy_shim_rejects_pagination_indicators(indicator: dict[str, object]) -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/items",
            FixtureResponse(
                body={
                    "data": [{"id": "1", "updated_at": "t1"}],
                    **indicator,
                }
            ),
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
                            "records_path": "data",
                            "primary_key": ["id"],
                            "cursor_field": "updated_at",
                            "cursor_param": "after",
                            "properties": {"id": "string", "updated_at": "string"},
                        }
                    ],
                },
            }
        )

        with pytest.raises(
            PaginationError,
            match="legacy declarative spec cannot paginate; migrate to a DeclarativeSource manifest",
        ):
            list(connector.read("items"))


def test_legacy_shim_descriptor_and_docstring_state_single_page_contract() -> None:
    descriptor = get_descriptor("declarative_http")

    assert descriptor is not None
    assert descriptor.certification_skips["resume_after_failure"] == (
        "legacy spec has no pagination keys; single-page source; recovery is a full "
        "re-read of that page; use a DeclarativeSource manifest for paginated APIs"
    )
    assert "single-page" in (inspect.getdoc(DeclarativeHttpConnector) or "").lower()


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
