from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pytest

from connectors.sdk.declarative.errors import PaginationError, ResponseShapeError
from connectors.sdk.declarative.pagination import PaginatorSpec, paginate
from connectors.sdk.declarative.requester import HttpRequester
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer


def _requester() -> HttpRequester:
    return HttpRequester(timeout_s=2, max_attempts=1, sleep=lambda _seconds: None)


def _records(start: int, count: int) -> list[dict[str, int]]:
    return [{"id": value} for value in range(start, start + count)]


def _query(target: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(target).query)


def test_none_paginator_requests_one_page() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/items",
            FixtureResponse(body={"records": _records(1, 3)}),
        )

        pages = paginate(
            _requester(),
            f"{fixture.base_url}/items",
            records_path="records",
            paginator=PaginatorSpec(type="none"),
        )

        assert len(pages) == 1
        assert pages[0].records == _records(1, 3)
        assert len(fixture.request_log) == 1


def test_cursor_paginator_follows_three_tokens_and_stops_without_token() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/items",
            responses=[
                FixtureResponse(body={"records": _records(1, 1), "next": "a"}),
                FixtureResponse(body={"records": _records(2, 1), "next": "b"}),
                FixtureResponse(body={"records": _records(3, 1)}),
            ],
        )

        pages = paginate(
            _requester(),
            f"{fixture.base_url}/items",
            records_path="records",
            paginator=PaginatorSpec(type="cursor", cursor_param="after", cursor_path="next"),
        )

        assert [page.records for page in pages] == [[{"id": 1}], [{"id": 2}], [{"id": 3}]]
        assert _query(fixture.request_log[0].target).get("after") is None
        assert _query(fixture.request_log[1].target)["after"] == ["a"]
        assert _query(fixture.request_log[2].target)["after"] == ["b"]


def test_cursor_paginator_can_read_tokens_from_response_headers() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/header-cursor",
            responses=[
                FixtureResponse(
                    body={"records": _records(1, 1)},
                    headers={"X-Next-Cursor": "a"},
                ),
                FixtureResponse(
                    body={"records": _records(2, 1)},
                    headers={"X-Next-Cursor": "b"},
                ),
                FixtureResponse(body={"records": _records(3, 1)}),
            ],
        )

        pages = paginate(
            _requester(),
            f"{fixture.base_url}/header-cursor",
            records_path="records",
            paginator=PaginatorSpec(
                type="cursor",
                cursor_param="cursor",
                cursor_header="X-Next-Cursor",
            ),
        )

        assert [page.records for page in pages] == [
            [{"id": 1}],
            [{"id": 2}],
            [{"id": 3}],
        ]
        assert [_query(entry.target).get("cursor") for entry in fixture.request_log] == [
            None,
            ["a"],
            ["b"],
        ]


def test_cursor_repeated_token_and_empty_page_with_token_fail_closed() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/repeat",
            responses=[
                FixtureResponse(body={"records": _records(1, 1), "next": "same"}),
                FixtureResponse(body={"records": _records(2, 1), "next": "same"}),
            ],
        )
        with pytest.raises(PaginationError, match="repeated"):
            paginate(
                _requester(),
                f"{fixture.base_url}/repeat",
                records_path="records",
                paginator=PaginatorSpec(
                    type="cursor", cursor_param="after", cursor_path="next"
                ),
            )

    with FixtureServer() as fixture:
        fixture.add_route(
            "/empty",
            FixtureResponse(body={"records": [], "next": "continue"}),
        )
        with pytest.raises(PaginationError, match="empty"):
            paginate(
                _requester(),
                f"{fixture.base_url}/empty",
                records_path="records",
                paginator=PaginatorSpec(
                    type="cursor", cursor_param="after", cursor_path="next"
                ),
            )

    with FixtureServer() as fixture:
        fixture.add_route(
            "/initial-token",
            FixtureResponse(body={"records": _records(1, 1), "next": "start"}),
        )
        with pytest.raises(PaginationError, match="repeated"):
            paginate(
                _requester(),
                f"{fixture.base_url}/initial-token",
                records_path="records",
                paginator=PaginatorSpec(
                    type="cursor",
                    cursor_param="after",
                    cursor_path="next",
                    initial_token="start",
                ),
            )


def test_offset_and_page_paginators_stop_on_short_page() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/offset",
            responses=[
                FixtureResponse(body={"records": _records(1, 2)}),
                FixtureResponse(body={"records": _records(3, 2)}),
                FixtureResponse(body={"records": _records(5, 1)}),
            ],
        )
        offset_pages = paginate(
            _requester(),
            f"{fixture.base_url}/offset",
            records_path="records",
            paginator=PaginatorSpec(
                type="offset",
                offset_param="start",
                page_size_param="count",
                page_size=2,
            ),
        )
        assert len(offset_pages) == 3
        assert [_query(item.target)["start"] for item in fixture.request_log] == [
            ["0"],
            ["2"],
            ["4"],
        ]
        assert all(_query(item.target)["count"] == ["2"] for item in fixture.request_log)

    with FixtureServer() as fixture:
        fixture.add_route(
            "/page",
            responses=[
                FixtureResponse(body={"records": _records(1, 2)}),
                FixtureResponse(body={"records": _records(3, 2)}),
                FixtureResponse(body={"records": _records(5, 1)}),
            ],
        )
        page_results = paginate(
            _requester(),
            f"{fixture.base_url}/page",
            records_path="records",
            paginator=PaginatorSpec(
                type="page",
                page_param="page",
                start_index=3,
                page_size_param="limit",
                page_size=2,
            ),
        )
        assert len(page_results) == 3
        assert [_query(item.target)["page"] for item in fixture.request_log] == [
            ["3"],
            ["4"],
            ["5"],
        ]


def test_max_pages_with_a_next_cursor_fails_instead_of_returning_partial() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/bounded",
            FixtureResponse(body={"records": _records(1, 1), "next": "more"}),
        )
        with pytest.raises(PaginationError, match="max_pages"):
            paginate(
                _requester(),
                f"{fixture.base_url}/bounded",
                records_path="records",
                paginator=PaginatorSpec(
                    type="cursor",
                    cursor_param="after",
                    cursor_path="next",
                    max_pages=1,
                ),
            )


def test_link_header_follows_relative_then_absolute_next_urls() -> None:
    with FixtureServer() as fixture:
        absolute = f"{fixture.base_url}/page3"
        fixture.add_route(
            "/links",
            FixtureResponse(
                body={"records": _records(1, 1)},
                headers={"Link": '<../page2?batch=2>; rel="next"'},
            ),
        )
        fixture.add_route(
            "/page2",
            FixtureResponse(
                body={"records": _records(2, 1)},
                headers={"Link": f'<{absolute}>; rel="next"'},
            ),
        )
        fixture.add_route("/page3", FixtureResponse(body={"records": _records(3, 1)}))

        pages = paginate(
            _requester(),
            f"{fixture.base_url}/links",
            records_path="records",
            paginator=PaginatorSpec(type="link_header"),
        )

        assert [page.records for page in pages] == [
            [{"id": 1}],
            [{"id": 2}],
            [{"id": 3}],
        ]
        assert urlsplit(fixture.request_log[1].target).path == "/page2"
        assert urlsplit(fixture.request_log[2].target).path == "/page3"


def test_link_header_repeated_url_and_invalid_records_path_fail_closed() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/repeat",
            FixtureResponse(
                body={"records": _records(1, 1)},
                headers={'Link': '</repeat>; rel="next"'},
            ),
        )
        with pytest.raises(PaginationError, match="repeated"):
            paginate(
                _requester(),
                f"{fixture.base_url}/repeat",
                records_path="records",
                paginator=PaginatorSpec(type="link_header"),
            )

    with FixtureServer() as fixture:
        fixture.add_route("/shape", FixtureResponse(body={"payload": []}))
        with pytest.raises(ResponseShapeError):
            paginate(
                _requester(),
                f"{fixture.base_url}/shape",
                records_path="records",
                paginator=PaginatorSpec(type="none"),
            )
