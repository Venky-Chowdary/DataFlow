from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pytest

from connectors.sdk.declarative.connector import DeclarativeSource
from connectors.sdk.declarative.errors import TransientExhausted
from connectors.sdk.declarative.incremental import run_sync
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer


def _manifest(base_url: str, *, inferred: bool = False) -> dict:
    stream = {
        "name": "users",
        "path": "users",
        "records_path": "items",
        "primary_key": ["id"],
        "cursor": {
            "field": "updated_at",
            "request_param": "updated_since",
            "format": "iso8601",
            "lookback_s": 1,
        },
        "paginator": {
            "type": "cursor",
            "cursor_param": "after",
            "cursor_path": "next",
            "page_size": 1,
        },
    }
    if not inferred:
        stream["json_schema"] = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "updated_at": {"type": "string", "format": "date-time"},
            },
        }
    return {
        "name": "fixture",
        "base_url": base_url,
        "auth": {"type": "api_key", "location": "header", "name": "X-API-Key"},
        "streams": [stream],
        "defaults": {
            "page_size": 1,
            "max_records": 2,
            "retry": {"max_attempts": 1, "base_delay": 0, "max_delay": 0},
        },
    }


def test_declarative_source_discovers_schema_and_reads_checkpointable_batches() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/users",
            responses=[
                FixtureResponse(
                    body={
                        "items": [{"id": 1, "updated_at": "2025-01-01T00:00:00Z"}],
                        "next": "page-2",
                    }
                ),
                FixtureResponse(
                    body={
                        "items": [{"id": 2, "updated_at": "2025-01-02T00:00:00Z"}],
                        "next": "page-3",
                    }
                ),
            ],
        )
        source = DeclarativeSource(
            {"manifest": _manifest(fixture.base_url), "credentials": {"api_key": "secret"}}
        )

        schema = source.discover()[0]
        assert schema.name == "users"
        assert schema.supported_sync_modes == ["full_refresh", "incremental"]
        batches = list(source.read("users", limit=10))

        assert [batch.records for batch in batches] == [
            [{"id": 1, "updated_at": "2025-01-01T00:00:00Z"}],
            [{"id": 2, "updated_at": "2025-01-02T00:00:00Z"}],
        ]
        assert batches[0].state["page_token"] == "page-2"
        assert batches[1].state["page_token"] == "page-3"
        assert batches[1].state["cursor"] == "2025-01-02T00:00:00Z"
        assert all(entry.headers.get("X-API-Key") == "secret" for entry in fixture.request_log)


def test_discover_infers_schema_from_exactly_one_response_page() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/users",
            FixtureResponse(
                body={
                    "items": [
                        {"id": 1, "updated_at": None, "unknown": None},
                        {"id": 2, "updated_at": None, "unknown": None},
                    ],
                    "next": "ignored",
                }
            ),
        )
        source = DeclarativeSource(
            {"manifest": _manifest(fixture.base_url, inferred=True), "credentials": {"api_key": "k"}}
        )

        schema = source.discover()[0]

        assert schema.json_schema["properties"]["unknown"]["type"] == ["null"]
        assert len(fixture.request_log) == 1


def test_check_and_caller_limit_use_one_live_request_and_respect_the_cap() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/users",
            responses=[
                FixtureResponse(
                    body={
                        "items": [{"id": 1, "updated_at": "2025-01-01T00:00:00Z"}],
                        "next": "page-2",
                    }
                ),
                FixtureResponse(
                    body={
                        "items": [{"id": 2, "updated_at": "2025-01-02T00:00:00Z"}],
                        "next": "page-3",
                    }
                ),
            ],
        )
        source = DeclarativeSource(
            {"manifest": _manifest(fixture.base_url), "credentials": {"api_key": "secret"}}
        )

        assert source.spec()["connectionSpecification"]["required"] == ["manifest"]
        assert source.check() == (True, "OK")
        batches = list(source.read("users", limit=1))

        assert sum(len(batch.records) for batch in batches) == 1
        assert len(fixture.request_log) == 2


def test_oauth_client_credentials_refreshes_once_after_a_401() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/token",
            responses=[
                FixtureResponse(body={"access_token": "first-token", "expires_in": 3600}),
                FixtureResponse(body={"access_token": "second-token", "expires_in": 3600}),
            ],
            method="POST",
        )
        fixture.add_route(
            "/users",
            responses=[
                FixtureResponse(status=401, body={"error": "expired"}),
                FixtureResponse(body={"items": [{"id": 1, "updated_at": "2025-01-01T00:00:00Z"}]}),
            ],
        )
        manifest = _manifest(fixture.base_url)
        manifest["auth"] = {
            "type": "oauth2_client_credentials",
            "token_url": f"{fixture.base_url}/token",
            "client_id": "client",
            "client_secret": "not-logged",
        }
        manifest["defaults"]["retry"]["max_attempts"] = 2
        source = DeclarativeSource({"manifest": manifest})

        batches = list(source.read("users", limit=1))

        assert len(batches) == 1
        api_calls = [entry for entry in fixture.request_log if entry.path == "/users"]
        assert [entry.headers.get("Authorization") for entry in api_calls] == [
            "Bearer first-token",
            "Bearer second-token",
        ]


def test_run_sync_checkpoints_page_one_before_page_two_fault_and_resumes() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/users",
            responses=[
                FixtureResponse(
                    body={
                        "items": [
                            {"id": 1, "updated_at": "2025-01-01T00:00:00Z"},
                            {"id": 2, "updated_at": "2025-01-02T00:00:00Z"},
                        ],
                        "next": "page-2",
                    }
                ),
                FixtureResponse(status=500, body={"error": "injected page-two fault"}),
            ],
        )
        manifest = _manifest(fixture.base_url)
        manifest["defaults"]["max_records"] = None
        manifest["defaults"]["retry"]["max_attempts"] = 1
        source = DeclarativeSource(
            {"manifest": manifest, "credentials": {"api_key": "secret"}}
        )
        written: list[dict] = []
        saved_states: list[dict] = []

        def write_page(records: list[dict]) -> None:
            written.extend(records)

        def load_state(_stream: str) -> dict | None:
            return saved_states[-1] if saved_states else None

        def save_state(_stream: str, state: dict) -> None:
            saved_states.append(state)

        with pytest.raises(TransientExhausted):
            run_sync(source, "users", write_page, load_state, save_state)

        first_page = [
            {"id": 1, "updated_at": "2025-01-01T00:00:00Z"},
            {"id": 2, "updated_at": "2025-01-02T00:00:00Z"},
        ]
        assert written == first_page
        assert len(saved_states) == 1
        assert saved_states[0]["page_token"] == "page-2"

        fixture.add_route(
            "/users",
            FixtureResponse(
                body={
                    "items": [
                        {"id": 3, "updated_at": "2025-01-03T00:00:00Z"},
                        {"id": 4, "updated_at": "2025-01-04T00:00:00Z"},
                    ]
                }
            ),
        )
        fixture.clear_request_log()
        resumed_source = DeclarativeSource(
            {"manifest": manifest, "credentials": {"api_key": "secret"}}
        )
        run_sync(resumed_source, "users", write_page, load_state, save_state)

        query = parse_qs(urlsplit(fixture.request_log[0].target).query)
        assert query["after"] == ["page-2"]
        assert {row["id"] for row in written} == {1, 2, 3, 4}
