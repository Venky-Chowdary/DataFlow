from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from connectors.sdk.hubspot_cdk import (
    HubSpotCDKConnector,
    HubSpotCursorMissing,
    HubSpotCursorStalled,
    HubSpotPaginationError,
)


class _ServerState:
    def __init__(self) -> None:
        self.list_pages: dict[str | None, dict[str, Any]] = {}
        self.search_responder: Any = None
        self.requests: list[dict[str, Any]] = []


@pytest.fixture
def hubspot_http_server():
    state = _ServerState()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *_args: Any) -> None:
            return

        def _send(self, payload: dict[str, Any]) -> None:
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            after = query.get("after", [None])[0]
            state.requests.append(
                {"method": "GET", "path": parsed.path, "query": query}
            )
            self._send(state.list_pages.get(after, {"results": []}))

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) or b"{}")
            state.requests.append(
                {"method": "POST", "path": parsed.path, "body": body}
            )
            if state.search_responder is None:
                self._send({"results": []})
            else:
                self._send(state.search_responder(body))

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield state, base
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _item(
    record_id: str,
    cursor: str,
    *,
    include_cursor: bool = True,
) -> dict[str, Any]:
    properties: dict[str, Any] = {"email": f"{record_id}@example.test"}
    if include_cursor:
        properties["lastmodifieddate"] = cursor
    return {"id": record_id, "properties": properties}


def _epoch_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = parsed.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def _connector(base: str) -> HubSpotCDKConnector:
    return HubSpotCDKConnector({"api_key": "local-test-token", "host": base})


def test_full_refresh_paginates_and_finishes_with_search_checkpoint(hubspot_http_server):
    state, base = hubspot_http_server
    state.list_pages = {
        None: {
            "results": [
                _item("1", "2026-01-01T00:00:00Z"),
                _item("2", "2026-01-02T00:00:00Z"),
            ],
            "paging": {"next": {"after": "2"}},
        },
        "2": {
            "results": [_item("3", "2026-01-03T00:00:00Z")],
            "paging": {"next": {"after": "4"}},
        },
        "4": {"results": [_item("4", "2026-01-04T00:00:00Z")]},
    }

    batches = list(_connector(base).read("contacts", limit=100))

    assert [[row["id"] for row in batch.records] for batch in batches] == [
        ["1", "2"],
        ["3"],
        ["4"],
    ]
    assert [request["query"].get("after", [None])[0] for request in state.requests] == [
        None,
        "2",
        "4",
    ]
    assert batches[0].state["contacts"] == {
        "cursor": "2026-01-02T00:00:00Z",
        "after": "2",
        "mode": "list",
    }
    assert batches[-1].state["contacts"] == {
        "cursor": "2026-01-04T00:00:00Z",
        "after": None,
        "mode": "search",
    }


def test_incremental_search_posts_inclusive_epoch_filter_and_sort(hubspot_http_server):
    state, base = hubspot_http_server
    cursor = "2026-01-02T03:04:05.123Z"
    later = "2026-01-03T00:00:00Z"
    state.search_responder = lambda _body: {
        "results": [_item("boundary", cursor), _item("updated", later)]
    }

    batches = list(
        _connector(base).read(
            "contacts",
            state={"contacts": {"cursor": cursor, "after": None, "mode": "search"}},
            limit=100,
        )
    )

    request = state.requests[0]
    assert request["method"] == "POST"
    assert request["path"] == "/crm/v3/objects/contacts/search"
    body = request["body"]
    assert body["filterGroups"] == [
        {
            "filters": [
                {
                    "propertyName": "lastmodifieddate",
                    "operator": "GTE",
                    "value": str(_epoch_ms(cursor)),
                }
            ]
        }
    ]
    assert body["sorts"] == [
        {"propertyName": "lastmodifieddate", "direction": "ASCENDING"}
    ]
    assert "lastmodifieddate" in body["properties"]
    assert body["after"] == 0 and type(body["after"]) is int
    assert body["limit"] <= 100
    assert [row["id"] for row in batches[0].records] == ["boundary", "updated"]
    assert batches[0].state["contacts"]["cursor"] == later
    assert batches[0].state["contacts"]["mode"] == "search"


def test_full_refresh_checkpoint_resumes_without_losing_population(hubspot_http_server):
    state, base = hubspot_http_server
    state.list_pages = {
        None: {
            "results": [
                _item("1", "2026-01-01T00:00:00Z"),
                _item("2", "2026-01-02T00:00:00Z"),
            ],
            "paging": {"next": {"after": "2"}},
        },
        "2": {
            "results": [
                _item("3", "2026-01-03T00:00:00Z"),
                _item("4", "2026-01-04T00:00:00Z"),
            ],
            "paging": {"next": {"after": "4"}},
        },
        "4": {"results": [_item("5", "2026-01-05T00:00:00Z")]},
    }
    connector = _connector(base)
    first_read = connector.read("contacts", limit=100)

    first_batch = next(first_read)
    first_read.close()
    resumed = list(connector.read("contacts", state=first_batch.state, limit=100))

    ids = [row["id"] for row in first_batch.records]
    ids.extend(row["id"] for batch in resumed for row in batch.records)
    assert ids == ["1", "2", "3", "4", "5"]
    assert state.requests[-1]["query"]["after"] == ["4"]


def test_search_checkpoint_resumes_with_its_original_filter(hubspot_http_server):
    state, base = hubspot_http_server
    initial_cursor = "2025-12-31T00:00:00Z"
    first_cursor = "2026-01-01T00:00:00Z"
    final_cursor = "2026-01-02T00:00:00Z"

    def respond(body: dict[str, Any]) -> dict[str, Any]:
        if body["after"] == 0:
            return {
                "results": [_item("1", first_cursor)],
                "paging": {"next": {"after": 100}},
            }
        return {"results": [_item("2", final_cursor)]}

    state.search_responder = respond
    connector = _connector(base)
    first_read = connector.read(
        "contacts",
        state={"contacts": {"cursor": initial_cursor, "mode": "search"}},
        limit=100,
    )

    first_batch = next(first_read)
    first_read.close()
    resumed = list(connector.read("contacts", state=first_batch.state, limit=100))

    assert [row["id"] for row in first_batch.records] == ["1"]
    assert [row["id"] for batch in resumed for row in batch.records] == ["2"]
    assert len(state.requests) == 2
    assert state.requests[1]["body"]["after"] == 100
    assert state.requests[1]["body"]["filterGroups"][0]["filters"][0]["value"] == str(
        _epoch_ms(initial_cursor)
    )
    assert resumed[-1].state["contacts"] == {
        "cursor": final_cursor,
        "after": None,
        "mode": "search",
    }


def test_search_repeated_after_token_fails_closed(hubspot_http_server):
    state, base = hubspot_http_server
    state.search_responder = lambda _body: {
        "results": [_item("1", "2026-01-01T00:00:00Z")],
        "paging": {"next": {"after": 100}},
    }

    with pytest.raises(HubSpotPaginationError, match="repeated an after cursor"):
        list(
            _connector(base).read(
                "contacts",
                state={
                    "contacts": {
                        "cursor": "2025-12-31T00:00:00Z",
                        "after": None,
                        "mode": "search",
                    }
                },
                limit=300,
            )
        )


def test_search_empty_page_with_next_token_fails_closed(hubspot_http_server):
    state, base = hubspot_http_server
    state.search_responder = lambda _body: {
        "results": [],
        "paging": {"next": {"after": 100}},
    }

    with pytest.raises(HubSpotPaginationError, match="empty page"):
        list(
            _connector(base).read(
                "contacts",
                state={"contacts": {"cursor": "2025-12-31T00:00:00Z"}},
                limit=300,
            )
        )


def test_search_record_without_cursor_field_fails_closed(hubspot_http_server):
    state, base = hubspot_http_server
    state.search_responder = lambda _body: {
        "results": [
            _item("1", "2026-01-01T00:00:00Z", include_cursor=False)
        ]
    }

    with pytest.raises(HubSpotCursorMissing, match="lastmodifieddate"):
        list(
            _connector(base).read(
                "contacts",
                state={"contacts": {"cursor": "2025-12-31T00:00:00Z"}},
                limit=100,
            )
        )


def test_search_restarts_at_10k_with_progress(hubspot_http_server):
    state, base = hubspot_http_server
    initial_cursor = "2025-12-31T00:00:00Z"
    first_cursor = "2026-01-01T00:00:00Z"
    second_cursor = "2026-01-02T00:00:00Z"

    def respond(body: dict[str, Any]) -> dict[str, Any]:
        if body["filterGroups"][0]["filters"][0]["value"] == str(_epoch_ms(initial_cursor)):
            return {
                "results": [_item("1", first_cursor)],
                "paging": {"next": {"after": 10_000}},
            }
        return {
            "results": [_item("1", first_cursor), _item("2", second_cursor)]
        }

    state.search_responder = respond
    batches = list(
        _connector(base).read(
            "contacts",
            state={"contacts": {"cursor": initial_cursor}},
            limit=100,
        )
    )

    assert len(state.requests) == 2
    assert all(request["body"]["after"] == 0 for request in state.requests)
    assert state.requests[1]["body"]["filterGroups"][0]["filters"][0]["value"] == str(
        _epoch_ms(first_cursor)
    )
    assert batches[-1].state["contacts"]["cursor"] == second_cursor


def test_search_10k_restart_without_cursor_progress_raises_stalled(hubspot_http_server):
    state, base = hubspot_http_server
    cursor = "2026-01-01T00:00:00Z"
    state.search_responder = lambda _body: {
        "results": [_item("1", cursor)],
        "paging": {"next": {"after": 10_000}},
    }

    with pytest.raises(
        HubSpotCursorStalled,
        match=">10,000 records share the cursor value",
    ):
        list(
            _connector(base).read(
                "contacts",
                state={"contacts": {"cursor": "2025-12-31T00:00:00Z"}},
                limit=300,
            )
        )
    assert len(state.requests) == 2


def test_legacy_after_and_cursor_state_shapes(hubspot_http_server):
    state, base = hubspot_http_server
    state.list_pages["2"] = {"results": [_item("list", "2026-01-01T00:00:00Z")]}
    list_batches = list(
        _connector(base).read("contacts", state={"contacts": {"after": "2"}}, limit=100)
    )
    state.search_responder = lambda _body: {
        "results": [_item("search", "2026-01-02T00:00:00Z")]
    }
    cursor_batches = list(
        _connector(base).read(
            "contacts",
            state={"contacts": {"lastmodifieddate": "2026-01-01T00:00:00Z"}},
            limit=100,
        )
    )

    assert state.requests[0]["method"] == "GET"
    assert state.requests[0]["query"]["after"] == ["2"]
    assert state.requests[1]["method"] == "POST"
    assert list_batches[0].records[0]["id"] == "list"
    assert cursor_batches[0].records[0]["id"] == "search"
