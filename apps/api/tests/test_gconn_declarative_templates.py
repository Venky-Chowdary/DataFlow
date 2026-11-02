from __future__ import annotations

import json
from urllib.parse import parse_qs, urlsplit

import pytest

from connectors.sdk.declarative.connector import DeclarativeSource
from connectors.sdk.declarative.errors import ManifestError
from connectors.sdk.declarative.manifest import parse_manifest
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer


def test_cursor_request_template_formats_iso_cursor_and_preserves_static_params() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/rest/api/3/search/jql",
            FixtureResponse(body={"issues": [{"id": 1, "updated": "2026-02-03T04:05:06Z"}]}),
        )
        manifest = {
            "name": "jira-template",
            "docs": "https://docs.example.test/jira",
            "base_url": fixture.base_url,
            "auth": {"type": "none"},
            "streams": [
                {
                    "name": "issues",
                    "path": "rest/api/3/search/jql",
                    "records_path": "issues",
                    "primary_key": ["id"],
                    "request_params": {"jql": "ORDER BY updated ASC"},
                    "cursor": {
                        "field": "updated",
                        "request_param": "jql",
                        "request_template": 'updated >= "{cursor:%Y/%m/%d %H:%M}" ORDER BY updated ASC',
                    },
                    "paginator": {
                        "type": "cursor",
                        "cursor_param": "nextPageToken",
                        "cursor_path": "nextPageToken",
                        "page_size_param": "maxResults",
                        "page_size": 100,
                    },
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "updated": {"type": "string"},
                        },
                    },
                }
            ],
        }

        batches = list(
            DeclarativeSource({"manifest": manifest}).read(
                "issues",
                state={"cursor": "2026-02-01T12:34:56Z"},
            )
        )

        assert [record["id"] for batch in batches for record in batch.records] == [1]
        query = parse_qs(urlsplit(fixture.request_log[0].target).query)
        assert query["jql"] == ['updated >= "2026/02/01 12:34" ORDER BY updated ASC']
        assert query["maxResults"] == ["100"]


def test_request_body_template_formats_cursor_and_places_next_page_token_in_body() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/conversations/search",
            responses=[
                FixtureResponse(
                    body={
                        "conversations": [{"id": "a", "updated_at": 1700000001}],
                        "pages": {"next": {"starting_after": "page-two"}},
                    }
                ),
                FixtureResponse(body={"conversations": [{"id": "b", "updated_at": 1700000002}]}),
            ],
            method="POST",
        )
        manifest = {
            "name": "intercom-template",
            "docs": "https://docs.example.test/intercom",
            "base_url": fixture.base_url,
            "auth": {"type": "none"},
            "streams": [
                {
                    "name": "conversations",
                    "path": "conversations/search",
                    "method": "POST",
                    "records_path": "conversations",
                    "primary_key": ["id"],
                    "request_body_template": {
                        "query": {
                            "operator": "AND",
                            "value": [
                                {
                                    "field": "updated_at",
                                    "operator": ">",
                                    "value": 0,
                                }
                            ],
                        },
                        "pagination": {"per_page": 150},
                    },
                    "cursor": {
                        "field": "updated_at",
                        "request_param": "query.value.0.value",
                        "request_location": "body",
                        "format": "epoch_s",
                    },
                    "paginator": {
                        "type": "cursor",
                        "cursor_param": "pagination.starting_after",
                        "cursor_location": "body",
                        "cursor_path": "pages.next.starting_after",
                        "page_size_param": "",
                        "page_size": 150,
                    },
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "updated_at": {"type": "integer"},
                        },
                    },
                }
            ],
        }

        batches = list(
            DeclarativeSource({"manifest": manifest}).read(
                "conversations",
                state={"cursor": 1700000000},
            )
        )

        assert [
            record["id"] for batch in batches for record in batch.records
        ] == ["a", "b"]
        first_request, second_request = fixture.request_log
        first_body = json.loads(first_request.body)
        second_body = json.loads(second_request.body)
        assert first_body["query"]["value"][0]["value"] == 1700000000
        assert first_body["pagination"] == {"per_page": 150}
        assert second_body["pagination"] == {
            "per_page": 150,
            "starting_after": "page-two",
        }


def test_manifest_path_templates_resolve_required_config_values() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/repos/fixture-owner/fixture-repo/issues",
            FixtureResponse(body=[{"id": 1}]),
        )
        manifest = {
            "name": "github-template",
            "base_url": fixture.base_url,
            "auth": {"type": "none"},
            "streams": [
                {
                    "name": "issues",
                    "path": "repos/{owner}/{repo}/issues",
                    "records_path": "$",
                    "primary_key": ["id"],
                    "json_schema": {
                        "type": "object",
                        "properties": {"id": {"type": "integer"}},
                    },
                }
            ],
        }

        source = DeclarativeSource(
            {
                "manifest": manifest,
                "owner": "fixture-owner",
                "repo": "fixture-repo",
            }
        )

        assert [
            record["id"]
            for batch in source.read("issues")
            for record in batch.records
        ] == [1]
        assert fixture.request_log[0].path == "/repos/fixture-owner/fixture-repo/issues"

        missing_config_source = DeclarativeSource({"manifest": manifest})
        with pytest.raises(ManifestError) as raised:
            list(missing_config_source.read("issues"))
        assert raised.value.path == "streams[0].path"


def test_manifest_template_extensions_reject_invalid_values_with_paths() -> None:
    manifest = {
        "name": "invalid-template",
        "docs": "not a URL",
        "base_url": "https://api.example.test",
        "auth": {"type": "none"},
        "streams": [{"name": "items", "path": "items"}],
    }

    with pytest.raises(ManifestError, match="absolute HTTP"):
        parse_manifest(manifest)

    manifest["docs"] = "https://docs.example.test"
    manifest["streams"][0]["request_body_template"] = []
    with pytest.raises(ManifestError) as body_error:
        parse_manifest(manifest)
    assert body_error.value.path == "streams[0].request_body_template"

    manifest["streams"][0].pop("request_body_template")
    manifest["streams"][0]["cursor"] = {
        "field": "updated_at",
        "request_param": "updated_at",
        "request_template": "updated_at > {cursor",
    }
    with pytest.raises(ManifestError) as cursor_error:
        parse_manifest(manifest)
    assert cursor_error.value.path == "streams[0].cursor.request_template"


def test_static_manifest_request_headers_reach_each_request() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/items",
            FixtureResponse(body=[{"id": 1}]),
        )
        manifest = {
            "name": "header-template",
            "base_url": fixture.base_url,
            "auth": {"type": "none"},
            "streams": [
                {
                    "name": "items",
                    "path": "items",
                    "records_path": "$",
                    "primary_key": ["id"],
                    "request_headers": {
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28",
                    },
                    "json_schema": {
                        "type": "object",
                        "properties": {"id": {"type": "integer"}},
                    },
                }
            ],
        }

        source = DeclarativeSource({"manifest": manifest})
        assert list(source.read("items"))
        request_headers = {
            key.lower(): value for key, value in fixture.request_log[0].headers.items()
        }
        assert request_headers["accept"] == "application/vnd.github+json"
        assert request_headers["x-github-api-version"] == "2022-11-28"

        assert source.test_connection()
        connection_headers = {
            key.lower(): value for key, value in fixture.request_log[1].headers.items()
        }
        assert connection_headers["x-github-api-version"] == "2022-11-28"
