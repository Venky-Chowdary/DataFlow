"""Demand-wave 6: monday.com and Linear — the first GraphQL SDK sources.

Both are POST-only GraphQL APIs where the cursor rides inside the request
body (``variables.*``) and the next token comes back under
``data.<entity>.pageInfo.endCursor`` — which keeps a non-empty value on
the LAST page, so a new ``has_more_path`` on the cursor paginator stops
on ``pageInfo.hasNextPage`` instead of issuing a wasted/empty request.

monday additionally exercises two new SDK capabilities this wave adds:
``{board_id}`` placeholders inside request_body_template values
(board-scoped items_page query) and numeric ``_dig_path`` segments
(``data.boards.0.items_page.cursor``).
"""

from __future__ import annotations

import json

from connectors.sdk import get_descriptor
from connectors.sdk.linear import LinearSource
from connectors.sdk.monday import MondaySource
from tests.connector_certification.fixture_server import (
    FixtureResponse,
    FixtureServer,
)


def _headers(log_entry) -> dict[str, str]:
    return {key.lower(): value for key, value in log_entry.headers.items()}


def _body(log_entry) -> dict:
    return json.loads(log_entry.body or "{}")


# ---------------------------------------------------------------- monday ---


def test_monday_manifest_contract() -> None:
    source = MondaySource({"api_key": "mdy_synthetic", "board_id": "42"})
    boards, users, items = source.manifest.streams

    assert source._base_url == "https://api.monday.com/v2/"
    assert source.auth.headers == {"Authorization": "mdy_synthetic"}
    assert boards.method == "POST" and boards.path == "."
    assert items.paginator.type == "cursor"
    assert items.paginator.cursor_location == "body"
    assert items.paginator.cursor_param == "variables.cursor"
    assert "{board_id}" in items.request_body_template["query"]
    assert get_descriptor("monday").catalog_ids == ("monday",)


def test_monday_boards_posts_graphql() -> None:
    payload = {
        "data": {
            "boards": [
                {"id": 11, "name": "Sprint", "state": "active"},
                {"id": 12, "name": "Backlog", "state": "active"},
            ]
        }
    }
    with FixtureServer() as fixture:
        fixture.add_route("/v2/", method="POST", responses=[FixtureResponse(body=payload)])
        source = MondaySource({"base_url": fixture.base_url + "/v2", "api_key": "mdy_synthetic"})
        records = [r for batch in source.read("boards") for r in batch.records]

    assert [r["id"] for r in records] == [11, 12]
    assert len(fixture.request_log) == 1
    entry = fixture.request_log[0]
    assert _headers(entry)["authorization"] == "mdy_synthetic"
    assert "boards(limit: 500)" in _body(entry)["query"]


def test_monday_items_pages_via_body_cursor() -> None:
    """items_page cursor rides in variables.cursor; page 2 stops when the
    response carries no next cursor."""
    page_one = {
        "data": {
            "boards": [
                {
                    "items_page": {
                        "cursor": "cursor-two",
                        "items": [{"id": 501, "name": "Task one"}],
                    }
                }
            ]
        }
    }
    page_two = {
        "data": {
            "boards": [
                {
                    "items_page": {
                        "cursor": None,
                        "items": [{"id": 502, "name": "Task two"}],
                    }
                }
            ]
        }
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/v2/",
            method="POST",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = MondaySource(
            {
                "base_url": fixture.base_url + "/v2",
                "api_key": "mdy_synthetic",
                "board_id": "42",
            }
        )
        records = [r for batch in source.read("items") for r in batch.records]

    assert [r["id"] for r in records] == [501, 502]
    assert len(fixture.request_log) == 2
    first = _body(fixture.request_log[0])
    second = _body(fixture.request_log[1])
    # {board_id} rendered inside the query string (raw, not quoted).
    assert "boards(ids: 42)" in first["query"]
    assert second["variables"]["cursor"] == "cursor-two"


# ---------------------------------------------------------------- linear ---


def test_linear_manifest_contract() -> None:
    source = LinearSource({"api_key": "lin_api_synthetic"})
    issues, projects, teams = source.manifest.streams

    assert source._base_url == "https://api.linear.app/graphql/"
    assert source.auth.headers == {"Authorization": "lin_api_synthetic"}
    for stream in (issues, projects, teams):
        assert stream.method == "POST"
        assert stream.paginator.type == "cursor"
        assert stream.paginator.cursor_location == "body"
        assert stream.paginator.has_more_path.endswith("pageInfo.hasNextPage")
    assert get_descriptor("linear").catalog_ids == ("linear",)


def test_linear_issues_stop_on_has_next_page_false() -> None:
    """pageInfo.endCursor stays populated on the last page — has_more_path
    must stop the walk without a wasted extra request."""
    page_one = {
        "data": {
            "issues": {
                "pageInfo": {"hasNextPage": True, "endCursor": "endc-1"},
                "nodes": [{"id": "i1", "identifier": "ENG-1", "title": "First"}],
            }
        }
    }
    page_two = {
        "data": {
            "issues": {
                # Linear keeps endCursor non-null even when hasNextPage=false.
                "pageInfo": {"hasNextPage": False, "endCursor": "endc-2"},
                "nodes": [{"id": "i2", "identifier": "ENG-2", "title": "Last"}],
            }
        }
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/graphql/",
            method="POST",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = LinearSource({"base_url": fixture.base_url + "/graphql", "api_key": "lin_api_synthetic"})
        records = [r for batch in source.read("issues") for r in batch.records]

    assert [r["identifier"] for r in records] == ["ENG-1", "ENG-2"]
    assert len(fixture.request_log) == 2
    second = _body(fixture.request_log[1])
    assert second["variables"]["after"] == "endc-1"


def test_linear_teams_single_page() -> None:
    payload = {
        "data": {
            "teams": {
                "pageInfo": {"hasNextPage": False, "endCursor": "endc-t"},
                "nodes": [{"id": "t1", "key": "ENG", "name": "Engineering"}],
            }
        }
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/graphql/", method="POST", responses=[FixtureResponse(body=payload)]
        )
        source = LinearSource({"base_url": fixture.base_url + "/graphql", "api_key": "lin_api_synthetic"})
        records = [r for batch in source.read("teams") for r in batch.records]

    assert [r["key"] for r in records] == ["ENG"]
    assert len(fixture.request_log) == 1
